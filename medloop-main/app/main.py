"""FastAPI application and REST endpoints for MedLoop (`app/main.py`).

Exposes endpoints for listing medical devices, executing atomic state transitions
with append-only custody event logging, querying full device history, creating
patient requests with 14-day TTL expiration, matching nearby available equipment
via `$geoNear`, and running operational analytics with a moving-average shortage
forecast. Also serves the MedLoop web dashboard at `/` when `medloop_dashboard.html`
is present in the project root.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional
from bson import Decimal128, ObjectId
from bson.errors import InvalidId
from fastapi import FastAPI, HTTPException, Path as FastAPIPath
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from pymongo import DESCENDING, ReturnDocument
from pymongo.errors import WriteError

from app.db import get_db
from app.state_machine import (
    get_event_type_for_transition,
    is_valid_transition,
)
from db.pipelines import (
    get_demand_vs_supply_pipeline,
    get_nearest_matches_pipeline,
    get_overdue_devices_pipeline,
)
from db.setup import CATEGORIES_REQUIRING_INSPECTION

app = FastAPI(
    title="MedLoop API",
    description=(
        "Hyperlocal medical equipment registry that matches available equipment "
        "to nearby patients and records inspection, sanitization, and custody history. "
        "NOTE: This system enforces and records a verification workflow; it does NOT "
        "verify physical safety."
    ),
    version="1.0.0",
)

# Enable CORS so the single-file frontend dashboard (`medloop_dashboard.html`)
# can call the API whether opened via `http://127.0.0.1:8000/` or directly as a local file.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Type Aliases & Pydantic Models
# ---------------------------------------------------------------------------

DeviceCategory = Literal[
    "wheelchair",
    "hospital_bed",
    "oxygen_concentrator",
    "nebulizer",
    "walker",
    "bp_monitor",
]

DeviceStatus = Literal[
    "listed",
    "inspected",
    "available",
    "reserved",
    "in_use",
    "returned",
    "sanitized",
    "rejected",
    "retired",
]

UrgencyLevel = Literal["low", "medium", "high"]


class GeoJSONPoint(BaseModel):
    """GeoJSON Point coordinate model ([longitude, latitude] in WGS84)."""

    type: Literal["Point"] = "Point"
    coordinates: List[float] = Field(
        ...,
        min_length=2,
        max_length=2,
        description="[longitude, latitude] in decimal degrees",
        examples=[[75.8573, 30.9010]],
    )


class DeviceCreateRequest(BaseModel):
    """Request payload for listing a donated or shared medical device."""

    category: DeviceCategory
    name: str = Field(..., min_length=1, examples=["Philips EverFlo 5L Oxygen Concentrator"])
    specs: Dict[str, Any] = Field(
        ...,
        description="Category-specific embedded specifications (e.g. seat_width_cm or flow_lpm, hours_used).",
        examples=[{"flow_lpm": 5, "hours_used": 420}],
    )
    owner_id: str = Field(..., description="Hex ObjectId string of the donor user")
    location: GeoJSONPoint
    city: str = Field(..., min_length=1, examples=["Ludhiana"])
    notes: Optional[str] = Field(default=None, examples=["Includes humidifier bottle and nasal cannula."])
    purchase_value: Decimal = Field(
        ...,
        gt=Decimal("0"),
        description="Original purchase value in INR (stored as BSON Decimal128).",
        examples=[Decimal("42500.00")],
    )


class TransitionRequest(BaseModel):
    """Request payload for transitioning a device to a new lifecycle state."""

    to_status: DeviceStatus
    actor_id: str = Field(..., description="Hex ObjectId string of the user performing the action")
    details: Dict[str, Any] = Field(
        default_factory=dict,
        description="Audit details (e.g. {'result': 'pass', 'checklist': ['filter_clean', 'flow_tested']})",
    )


class PatientRequestCreate(BaseModel):
    """Request payload for a patient requesting medical equipment."""

    requester_id: str = Field(..., description="Hex ObjectId string of the requester user")
    category: DeviceCategory
    city: str = Field(..., min_length=1, examples=["Ludhiana"])
    location: GeoJSONPoint
    urgency: UrgencyLevel = "medium"


# ---------------------------------------------------------------------------
# Serialization Helpers
# ---------------------------------------------------------------------------


def parse_object_id(id_str: str, field_name: str = "id") -> ObjectId:
    """Convert a hex string into a BSON ObjectId or raise HTTP 400.

    Args:
        id_str: Candidate 24-character hex string.
        field_name: Human-readable field label for error reporting.

    Returns:
        ObjectId: Validated BSON ObjectId instance.
    """
    try:
        return ObjectId(id_str)
    except (InvalidId, TypeError) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid ObjectId format for '{field_name}': {id_str}",
        ) from exc


def serialize_bson(value: Any) -> Any:
    """Recursively convert BSON types (ObjectId, Decimal128, datetime) into JSON-safe values.

    Args:
        value: Arbitrary document, list, or primitive returned by PyMongo.

    Returns:
        Any: JSON-serializable representation.
    """
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: serialize_bson(v) for k, v in value.items()}
    if isinstance(value, list):
        return [serialize_bson(item) for item in value]
    return value


# ---------------------------------------------------------------------------
# Frontend Dashboard & Sample Helper Routes
# ---------------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def serve_dashboard() -> HTMLResponse:
    """Serve `medloop_dashboard.html` at `http://127.0.0.1:8000/` if present."""
    root_dir = Path(__file__).resolve().parent.parent
    for candidate in (root_dir / "medloop_dashboard.html", root_dir / "app" / "medloop_dashboard.html"):
        if candidate.exists():
            return HTMLResponse(content=candidate.read_text(encoding="utf-8"))
    return HTMLResponse(
        content=(
            "<h3>MedLoop API is running.</h3>"
            "<p>Place <code>medloop_dashboard.html</code> in your <code>medloop/</code> folder "
            "or open <a href='/docs'>/docs</a>.</p>"
        )
    )


@app.get("/samples", include_in_schema=False)
def get_demo_samples() -> Dict[str, Any]:
    """Return sample seeded ObjectIds and documents so the web UI dropdowns auto-populate."""
    db = get_db()
    donor = db.users.find_one({"role": "donor"})
    requester = db.users.find_one({"role": "requester"})
    inspector = db.users.find_one({"role": "inspector"})
    devices = list(db.devices.find({}).limit(12))
    requests = list(db.requests.find({"status": "open"}).limit(12))
    return serialize_bson(
        {
            "donor_id": donor["_id"] if donor else None,
            "requester_id": requester["_id"] if requester else None,
            "inspector_id": inspector["_id"] if inspector else None,
            "devices": devices,
            "requests": requests,
        }
    )


# ---------------------------------------------------------------------------
# Core MedLoop Endpoints
# ---------------------------------------------------------------------------


@app.post("/devices", status_code=201)
def create_device(payload: DeviceCreateRequest) -> Dict[str, Any]:
    """List a new medical device with initial status 'listed' and record a 'listed' event.

    Automatically sets `requires_inspection = True` for high-risk equipment categories
    (`oxygen_concentrator`, `nebulizer`, `hospital_bed`) and stores `purchase_value`
    as BSON `Decimal128`.

    Args:
        payload: Validated `DeviceCreateRequest` body.

    Returns:
        Dict[str, Any]: Newly created device document and initial audit event ID.
    """
    db = get_db()
    owner_oid = parse_object_id(payload.owner_id, "owner_id")
    now = datetime.now(timezone.utc)

    requires_inspection = payload.category in CATEGORIES_REQUIRING_INSPECTION

    device_doc: Dict[str, Any] = {
        "category": payload.category,
        "name": payload.name,
        "specs": payload.specs,
        "status": "listed",
        "requires_inspection": requires_inspection,
        "last_inspection_at": None,
        "last_sanitized_at": None,
        "owner_id": owner_oid,
        "location": payload.location.model_dump(),
        "city": payload.city,
        "notes": payload.notes,
        "purchase_value": Decimal128(str(payload.purchase_value)),
        "listed_at": now,
    }

    try:
        insert_result = db.devices.insert_one(device_doc)
    except WriteError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Document failed $jsonSchema validation: {exc.details}",
        ) from exc

    device_id = insert_result.inserted_id

    # Append initial custody event to the unbounded `events` collection
    event_doc: Dict[str, Any] = {
        "device_id": device_id,
        "type": "listed",
        "actor_id": owner_oid,
        "ts": now,
        "from_status": None,
        "to_status": "listed",
        "details": {"note": "Device listed in MedLoop registry"},
    }
    event_result = db.events.insert_one(event_doc)

    device_doc["_id"] = device_id
    return {
        "device": serialize_bson(device_doc),
        "event_id": str(event_result.inserted_id),
    }


@app.post("/devices/{id}/transition")
def transition_device_status(
    payload: TransitionRequest,
    id: str = FastAPIPath(..., description="Hex ObjectId of the target device"),
) -> Dict[str, Any]:
    """Atomically transition a device to a new status and append an audit event.

    Validates the requested transition using `is_valid_transition(current_status, to_status)`
    (raising HTTP 409 if disallowed). Uses `find_one_and_update` with the optimistic
    concurrency filter `{"_id": device_oid, "status": current_status}` so two concurrent
    requesters cannot reserve or transition the same device simultaneously.

    Args:
        payload: Target status, actor ObjectId, and event details.
        id: Device ObjectId string from the URL path.

    Returns:
        Dict[str, Any]: Updated device document and newly appended event document.
    """
    db = get_db()
    device_oid = parse_object_id(id, "id")
    actor_oid = parse_object_id(payload.actor_id, "actor_id")

    # Fetch current device state to validate the transition rules
    current_device = db.devices.find_one({"_id": device_oid})
    if current_device is None:
        raise HTTPException(status_code=404, detail=f"Device {id} not found")

    current_status: str = current_device["status"]
    if not is_valid_transition(current_status, payload.to_status):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Invalid state transition from '{current_status}' "
                f"to '{payload.to_status}'."
            ),
        )

    now = datetime.now(timezone.utc)
    update_fields: Dict[str, Any] = {"status": payload.to_status}

    # Record compliance timestamps when inspection or sanitization milestones occur
    if payload.to_status == "inspected":
        update_fields["last_inspection_at"] = now
    elif payload.to_status == "sanitized":
        update_fields["last_sanitized_at"] = now

    # WHY find_one_and_update WITH { _id, status: current_status }:
    # Matching on both `_id` and the expected `current_status` executes as a single
    # atomic compare-and-swap operation in MongoDB. If another request concurrently
    # reserves or modifies the device between our read and this write, the filter
    # fails to match and returns None—preventing double-reservation race conditions.
    try:
        updated_device = db.devices.find_one_and_update(
            filter={"_id": device_oid, "status": current_status},
            update={"$set": update_fields},
            return_document=ReturnDocument.AFTER,
        )
    except WriteError as exc:
        raise HTTPException(
            status_code=409,
            detail=f"Transition rejected by $jsonSchema validator: {exc.details}",
        ) from exc

    if updated_device is None:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Concurrent modification detected: device {id} is no longer in "
                f"status '{current_status}'."
            ),
        )

    event_type = get_event_type_for_transition(current_status, payload.to_status)
    event_doc: Dict[str, Any] = {
        "device_id": device_oid,
        "type": event_type,
        "actor_id": actor_oid,
        "ts": now,
        "from_status": current_status,
        "to_status": payload.to_status,
        "details": payload.details,
    }
    event_result = db.events.insert_one(event_doc)
    event_doc["_id"] = event_result.inserted_id

    return {
        "device": serialize_bson(updated_device),
        "event": serialize_bson(event_doc),
    }


@app.get("/devices/{id}/history")
def get_device_history(
    id: str = FastAPIPath(..., description="Hex ObjectId of the target device"),
) -> Dict[str, Any]:
    """Return a device document along with all of its lifecycle events sorted by `ts`.

    Uses the compound index `{device_id: 1, ts: -1}` on `events` to fetch the complete
    custody, inspection, and sanitization trail without an in-memory sort.

    Args:
        id: Device ObjectId string from the URL path.

    Returns:
        Dict[str, Any]: Device metadata and chronologically ordered list of events.
    """
    db = get_db()
    device_oid = parse_object_id(id, "id")

    device = db.devices.find_one({"_id": device_oid})
    if device is None:
        raise HTTPException(status_code=404, detail=f"Device {id} not found")

    # Query leverages compound index {device_id: 1, ts: -1}
    events = list(
        db.events.find({"device_id": device_oid}).sort("ts", DESCENDING)
    )

    return {
        "device": serialize_bson(device),
        "event_count": len(events),
        "events": serialize_bson(events),
    }


@app.post("/requests", status_code=201)
def create_request(payload: PatientRequestCreate) -> Dict[str, Any]:
    """Create a patient equipment request with automatic 14-day TTL expiration.

    Sets `expires_at = now + 14 days`, which works with the TTL index on
    `requests.expires_at` (`expireAfterSeconds: 0`) to automatically purge stale requests.

    Args:
        payload: Validated `PatientRequestCreate` body.

    Returns:
        Dict[str, Any]: Newly created request document.
    """
    db = get_db()
    requester_oid = parse_object_id(payload.requester_id, "requester_id")
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(days=14)

    request_doc: Dict[str, Any] = {
        "requester_id": requester_oid,
        "category": payload.category,
        "city": payload.city,
        "location": payload.location.model_dump(),
        "urgency": payload.urgency,
        "status": "open",
        "matched_device_id": None,
        "created_at": now,
        "expires_at": expires_at,
    }

    try:
        result = db.requests.insert_one(request_doc)
    except WriteError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Request failed $jsonSchema validation: {exc.details}",
        ) from exc

    request_doc["_id"] = result.inserted_id
    return {"request": serialize_bson(request_doc)}


@app.get("/match/{request_id}")
def match_devices_for_request(
    request_id: str = FastAPIPath(..., description="Hex ObjectId of the patient request"),
) -> Dict[str, Any]:
    """Find available devices within 25 km matching a patient request's category.

    Executes Pipeline 3 (`$geoNear` up to 25,000 meters). Because MongoDB `$jsonSchema`
    cannot compare `last_inspection_at` against the current time, this endpoint enforces
    that devices with `requires_inspection == True` must have `last_inspection_at` within
    the last 180 days.

    Args:
        request_id: Patient request ObjectId string.

    Returns:
        Dict[str, Any]: Request summary and list of nearest eligible available devices.
    """
    db = get_db()
    req_oid = parse_object_id(request_id, "request_id")

    patient_request = db.requests.find_one({"_id": req_oid})
    if patient_request is None:
        raise HTTPException(status_code=404, detail=f"Request {request_id} not found")

    coords: List[float] = patient_request["location"]["coordinates"]
    longitude, latitude = float(coords[0]), float(coords[1])
    category: str = patient_request["category"]

    matches = get_nearest_matches_pipeline(
        db=db,
        longitude=longitude,
        latitude=latitude,
        category=category,
        max_distance_meters=25_000.0,
    )

    return {
        "request_id": request_id,
        "category": category,
        "city": patient_request["city"],
        "max_radius_km": 25,
        "match_count": len(matches),
        "matches": serialize_bson(matches),
    }


@app.get("/analytics/overdue")
def get_overdue_analytics() -> Dict[str, Any]:
    """Run Aggregation Pipeline 1 to list devices overdue for sanitization or inspection.

    Flags non-retired devices whose `last_sanitized_at` is older than 30 days or
    whose `last_inspection_at` (when `requires_inspection` is True) is older than 180 days,
    joined with their most recent custody event.

    Returns:
        Dict[str, Any]: Count and list of overdue devices with latest event details.
    """
    db = get_db()
    overdue_devices = get_overdue_devices_pipeline(db)
    return {
        "overdue_count": len(overdue_devices),
        "overdue_devices": serialize_bson(overdue_devices),
    }


@app.get("/analytics/shortages")
def get_shortages_and_forecast() -> Dict[str, Any]:
    """Run Aggregation Pipeline 2 (demand vs supply) and a 28-day moving-average forecast.

    AI LAYER NOTE:
    This endpoint computes a 4-week (28-day) moving-average baseline in plain Python
    (not a trained ML model). Its operational purpose is to support the concrete
    triage decision: "which equipment categories in which cities should inspectors
    and sanitization volunteers refurbish first?"

    Returns:
        Dict[str, Any]: Current open demand vs supply gaps and next-week shortage forecasts.
    """
    db = get_db()
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(days=28)

    # 1. Run Pipeline 2 for current open demand vs available supply
    demand_vs_supply = get_demand_vs_supply_pipeline(db)

    # 2. AI LAYER: Plain-Python 28-day moving-average baseline (not a trained model).
    # Purpose: Guide the operational decision "which categories to inspect and refurbish first"
    # by comparing forecasted weekly request volume against currently available stock.
    recent_requests_cursor = db.requests.find(
        {"created_at": {"$gte": window_start}},
        {"category": 1, "city": 1, "_id": 0},
    )
    counts_28d: Dict[tuple[str, str], int] = {}
    for req in recent_requests_cursor:
        key = (req["category"], req["city"])
        counts_28d[key] = counts_28d.get(key, 0) + 1

    # Query available device counts per {category, city} so even pairs without open requests
    # but with recent 28-day activity are accurately evaluated
    available_cursor = db.devices.aggregate(
        [
            {"$match": {"status": "available"}},
            {
                "$group": {
                    "_id": {"category": "$category", "city": "$city"},
                    "available_count": {"$sum": 1},
                }
            },
        ]
    )
    available_map: Dict[tuple[str, str], int] = {
        (doc["_id"]["category"], doc["_id"]["city"]): doc["available_count"]
        for doc in available_cursor
    }

    all_keys = sorted(set(counts_28d.keys()) | set(available_map.keys()))
    forecasts: List[Dict[str, Any]] = []
    for category, city in all_keys:
        req_28d = counts_28d.get((category, city), 0)
        weekly_avg = round(req_28d / 4.0, 2)
        forecast_next_week = weekly_avg
        available_supply = available_map.get((category, city), 0)
        shortage_flagged = forecast_next_week > available_supply

        forecasts.append(
            {
                "category": category,
                "city": city,
                "requests_last_28_days": req_28d,
                "weekly_average_demand": weekly_avg,
                "forecast_next_week_demand": forecast_next_week,
                "available_supply": available_supply,
                "shortage_flagged": shortage_flagged,
                "recommended_action": (
                    "Prioritize inspection and sanitization of pending stock"
                    if shortage_flagged
                    else "Supply sufficient for forecasted weekly demand"
                ),
            }
        )

    # Sort so flagged shortages with largest deficit appear first
    forecasts.sort(
        key=lambda item: (
            not item["shortage_flagged"],
            -(item["forecast_next_week_demand"] - item["available_supply"]),
        )
    )

    return {
        "model_note": (
            "Moving-average baseline over the past 28 days (4 weeks), not a trained ML model. "
            "Used to decide which categories to inspect and refurbish first."
        ),
        "current_demand_vs_supply": serialize_bson(demand_vs_supply),
        "weekly_shortage_forecasts": forecasts,
    }
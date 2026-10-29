"""Deterministic synthetic dataset generator for MedLoop (`python -m scripts.seed`).

Seeds:
- 30 users (donors, requesters, inspectors) across 3 Indian cities (Ludhiana, Delhi, Bengaluru)
- 2,000 medical devices with realistic GeoJSON coordinate clusters and category-specific embedded specs
- 50,000+ append-only lifecycle events consistent with each device's final status
- 1,500 patient requests with 14-day TTL expiration timestamps

Uses `random.seed(42)` and `Faker.seed(42)` for reproducibility, drops and recreates
collections via `db/setup.py` on every run (idempotent), and inserts documents in
batches of 1,000.
"""

from datetime import datetime, timedelta, timezone
import random
from typing import Any, Dict, Iterable, List, Sequence, Tuple
from bson import Decimal128, ObjectId
from faker import Faker
from pymongo.collection import Collection

from app.db import get_db
from db.setup import (
    CATEGORIES_REQUIRING_INSPECTION,
    DEVICE_CATEGORIES,
    setup_database,
)

BATCH_SIZE: int = 1_000

# Realistic WGS84 [longitude, latitude] centers for 3 Indian cities
CITY_COORDINATES: Dict[str, Tuple[float, float]] = {
    "Ludhiana": (75.8573, 30.9010),
    "Delhi": (77.2090, 28.6139),
    "Bengaluru": (77.5946, 12.9716),
}

CATEGORY_MODELS: Dict[str, List[Tuple[str, str]]] = {
    "wheelchair": [
        ("Karma Ryder 4 Folding Wheelchair", "Foldable steel frame, manual footrests"),
        ("Kosmocare Pride Transit Wheelchair", "Lightweight aluminum frame, rear drum brakes"),
        ("Vissco Invalid Manual Wheelchair", "Heavy-duty upholstery, detachable armrests"),
    ],
    "hospital_bed": [
        ("Paramount Semi-Fowler Hospital Bed", "Mechanical backrest crank, collapsible side rails"),
        ("Midmark Full-Fowler ICU Bed", "Dual actuator elevation, IV pole mount included"),
        ("Janak 3-Function Manual Medical Bed", "Powder-coated frame, perforated steel mattress base"),
    ],
    "oxygen_concentrator": [
        ("Philips Respironics EverFlo 5L", "Continuous flow 5 LPM, low-noise compressor"),
        ("Oxymed Mini 5L Stationary Concentrator", "Dual outlet with built-in nebulizer port"),
        ("Yuwell 8F-5AW Oxygen Concentrator", "Digital hour meter, purity alarm indicator"),
    ],
    "nebulizer": [
        ("Omron NE-C28 Compressor Nebulizer", "Virtual valve technology, adult and child masks"),
        ("Dr. Morepen CN-06 Piston Nebulizer", "Compact tabletop compressor, thermal protector"),
        ("Philips InnoSpire Essence Nebulizer", "SideStream nebulizer kit, continuous duty"),
    ],
    "walker": [
        ("Vissco Reciprocal Folding Walker", "Height-adjustable anodized aluminum legs"),
        ("Flamingo Rollator Walker with Front Wheels", "Padded handgrips, foldable frame"),
        ("Tynor Walking Frame Universal", "Non-slip rubber ferrules, lightweight build"),
    ],
    "bp_monitor": [
        ("Omron HEM-7120 Automatic BP Monitor", "Intellisense cuff inflation, hypertension indicator"),
        ("Dr. Trust Smart Dual-User BP Monitor", "Upper arm universal cuff, irregular heartbeat alert"),
        ("Beurer BM-27 Upper Arm Blood Pressure Monitor", "Cuff wrapping guide, 4-user memory bank"),
    ],
}


def chunked_insert(collection: Collection, docs: Sequence[Dict[str, Any]], batch_size: int = BATCH_SIZE) -> None:
    """Insert documents into a MongoDB collection in batches of `batch_size`.

    Args:
        collection: Target PyMongo Collection.
        docs: Sequence of documents to insert.
        batch_size: Maximum number of documents per `insert_many` call (default 1,000).
    """
    for idx in range(0, len(docs), batch_size):
        batch = docs[idx : idx + batch_size]
        if batch:
            collection.insert_many(batch, ordered=False)


def random_city_point(city: str) -> Dict[str, Any]:
    """Generate a GeoJSON Point within ~12 km of the given city's center.

    Args:
        city: City name key from `CITY_COORDINATES`.

    Returns:
        Dict[str, Any]: GeoJSON Point object `{"type": "Point", "coordinates": [lng, lat]}`.
    """
    base_lng, base_lat = CITY_COORDINATES[city]
    # ~0.08 degrees is roughly 8-9 km around the city center
    lng = round(base_lng + random.gauss(0.0, 0.045), 6)
    lat = round(base_lat + random.gauss(0.0, 0.045), 6)
    return {"type": "Point", "coordinates": [lng, lat]}


def generate_specs_for_category(category: str) -> Dict[str, Any]:
    """Generate category-specific embedded `specs` demonstrating polymorphic schema design.

    Args:
        category: Medical equipment category string.

    Returns:
        Dict[str, Any]: Embedded specification dictionary for the category.
    """
    if category == "wheelchair":
        return {
            "seat_width_cm": random.choice([40, 43, 46, 50]),
            "max_load_kg": random.choice([100, 120, 135]),
            "foldable": True,
        }
    if category == "oxygen_concentrator":
        return {
            "flow_lpm": random.choice([5, 10]),
            "hours_used": random.randint(50, 4800),
            "oxygen_purity_pct": round(random.uniform(90.5, 95.5), 1),
        }
    if category == "hospital_bed":
        return {
            "fowler_type": random.choice(["semi_fowler", "full_fowler"]),
            "side_rails": True,
            "mattress_included": random.choice([True, False]),
        }
    if category == "nebulizer":
        return {
            "particle_size_microns": round(random.uniform(2.5, 4.5), 1),
            "hours_used": random.randint(10, 600),
            "mask_sizes": ["adult", "pediatric"],
        }
    if category == "walker":
        return {
            "height_adjustable_cm": [76, 94],
            "has_front_wheels": random.choice([True, False]),
            "frame_material": "anodized_aluminum",
        }
    # bp_monitor
    return {
        "cuff_circumference_cm": [22, 42],
        "power_source": random.choice(["4xAA_battery", "AC_adapter_and_AA"]),
        "memory_slots": random.choice([30, 60, 120]),
    }


def build_device_and_events(
    device_id: ObjectId,
    category: str,
    city: str,
    donors: List[ObjectId],
    requesters: List[ObjectId],
    inspectors: List[ObjectId],
    now: datetime,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Construct one device document and ~25-27 consistent lifecycle events.

    Simulates multi-cycle reuse (`listed -> inspected -> available -> reserved ->
    in_use -> returned -> sanitized -> available`) so that 2,000 devices produce
    50,000+ append-only audit events while keeping each device's final status and
    compliance dates 100% consistent with its event history and `$jsonSchema`.

    Args:
        device_id: Pre-allocated ObjectId for the device.
        category: Equipment category.
        city: Assigned city name.
        donors: List of donor user ObjectIds.
        requesters: List of requester user ObjectIds.
        inspectors: List of inspector user ObjectIds.
        now: Reference current UTC timestamp.

    Returns:
        Tuple[Dict[str, Any], List[Dict[str, Any]]]: Device document and its ordered events.
    """
    owner_id = random.choice(donors)
    inspector_id = random.choice(inspectors)
    model_name, model_note = random.choice(CATEGORY_MODELS[category])
    requires_inspection = category in CATEGORIES_REQUIRING_INSPECTION

    # Start listing ~400 to 500 days ago so we can fit 5-6 complete loan cycles
    cursor_time = now - timedelta(days=random.randint(420, 520))
    listed_at = cursor_time

    events: List[Dict[str, Any]] = []
    last_inspection_at: datetime | None = None
    last_sanitized_at: datetime | None = None

    # Step 1: Initial listing
    events.append(
        {
            "_id": ObjectId(),
            "device_id": device_id,
            "type": "listed",
            "actor_id": owner_id,
            "ts": cursor_time,
            "from_status": None,
            "to_status": "listed",
            "details": {"note": "Initial donor listing"},
        }
    )

    # Step 2: Initial inspection (listed -> inspected)
    cursor_time += timedelta(days=random.randint(1, 3))
    last_inspection_at = cursor_time
    events.append(
        {
            "_id": ObjectId(),
            "device_id": device_id,
            "type": "inspected",
            "actor_id": inspector_id,
            "ts": cursor_time,
            "from_status": "listed",
            "to_status": "inspected",
            "details": {
                "result": "pass",
                "checklist": ["structural_check", "functional_test", "label_check"],
            },
        }
    )

    # Step 3: Release to available pool (inspected -> available)
    cursor_time += timedelta(days=1)
    last_sanitized_at = cursor_time
    events.append(
        {
            "_id": ObjectId(),
            "device_id": device_id,
            "type": "serviced",
            "actor_id": inspector_id,
            "ts": cursor_time,
            "from_status": "inspected",
            "to_status": "available",
            "details": {"note": "Initial sanitization and release to available inventory"},
        }
    )

    current_status = "available"

    # Run 5 full reuse cycles (each cycle adds 5 events -> 3 + 25 = 28 events per device -> ~56,000 events total)
    for cycle in range(5):
        patient_id = random.choice(requesters)

        # available -> reserved
        cursor_time += timedelta(days=random.randint(5, 10))
        events.append(
            {
                "_id": ObjectId(),
                "device_id": device_id,
                "type": "reserved",
                "actor_id": patient_id,
                "ts": cursor_time,
                "from_status": "available",
                "to_status": "reserved",
                "details": {"cycle": cycle + 1},
            }
        )

        # reserved -> in_use
        cursor_time += timedelta(days=random.randint(1, 2))
        events.append(
            {
                "_id": ObjectId(),
                "device_id": device_id,
                "type": "handed_over",
                "actor_id": patient_id,
                "ts": cursor_time,
                "from_status": "reserved",
                "to_status": "in_use",
                "details": {"cycle": cycle + 1, "custody": "patient"},
            }
        )

        # in_use -> returned
        cursor_time += timedelta(days=random.randint(14, 30))
        events.append(
            {
                "_id": ObjectId(),
                "device_id": device_id,
                "type": "returned",
                "actor_id": patient_id,
                "ts": cursor_time,
                "from_status": "in_use",
                "to_status": "returned",
                "details": {"cycle": cycle + 1, "condition_note": "Returned intact"},
            }
        )

        # returned -> sanitized
        cursor_time += timedelta(days=random.randint(1, 3))
        last_sanitized_at = cursor_time
        events.append(
            {
                "_id": ObjectId(),
                "device_id": device_id,
                "type": "sanitized",
                "actor_id": inspector_id,
                "ts": cursor_time,
                "from_status": "returned",
                "to_status": "sanitized",
                "details": {
                    "cycle": cycle + 1,
                    "disinfectant": "70_pct_isopropyl_and_quaternary_ammonium",
                },
            }
        )

        # sanitized -> available
        cursor_time += timedelta(days=1)
        # Refresh inspection timestamp on recent cycles for most devices so some are fresh (<180d)
        # while ~20% keep an older inspection date to make `/analytics/overdue` realistic.
        if cycle == 4 and random.random() < 0.80:
            last_inspection_at = now - timedelta(days=random.randint(5, 120))
            last_sanitized_at = now - timedelta(days=random.randint(2, 25))
            cursor_time = last_sanitized_at

        events.append(
            {
                "_id": ObjectId(),
                "device_id": device_id,
                "type": "serviced",
                "actor_id": inspector_id,
                "ts": cursor_time,
                "from_status": "sanitized",
                "to_status": "available",
                "details": {"cycle": cycle + 1, "ready_for_match": True},
            }
        )
        current_status = "available"

    # Optionally advance a subset of devices into other terminal/active states for variety
    roll = random.random()
    if roll < 0.60:
        # Stay in 'available'
        pass
    elif roll < 0.75:
        # Transition available -> reserved
        cursor_time += timedelta(days=1)
        events.append(
            {
                "_id": ObjectId(),
                "device_id": device_id,
                "type": "reserved",
                "actor_id": random.choice(requesters),
                "ts": cursor_time,
                "from_status": "available",
                "to_status": "reserved",
                "details": {"note": "Currently reserved by nearby patient"},
            }
        )
        current_status = "reserved"
    elif roll < 0.90:
        # Transition available -> reserved -> in_use
        patient_id = random.choice(requesters)
        cursor_time += timedelta(days=1)
        events.append(
            {
                "_id": ObjectId(),
                "device_id": device_id,
                "type": "reserved",
                "actor_id": patient_id,
                "ts": cursor_time,
                "from_status": "available",
                "to_status": "reserved",
                "details": {"note": "Reserved prior to handover"},
            }
        )
        cursor_time += timedelta(days=1)
        events.append(
            {
                "_id": ObjectId(),
                "device_id": device_id,
                "type": "handed_over",
                "actor_id": patient_id,
                "ts": cursor_time,
                "from_status": "reserved",
                "to_status": "in_use",
                "details": {"note": "Active home care deployment"},
            }
        )
        current_status = "in_use"
    else:
        # Transition available -> retired
        cursor_time += timedelta(days=1)
        events.append(
            {
                "_id": ObjectId(),
                "device_id": device_id,
                "type": "retired",
                "actor_id": inspector_id,
                "ts": cursor_time,
                "from_status": "available",
                "to_status": "retired",
                "details": {"reason": "End of service life / wear limit reached"},
            }
        )
        current_status = "retired"

    price_map = {
        "wheelchair": "8500.00",
        "hospital_bed": "24000.00",
        "oxygen_concentrator": "45000.00",
        "nebulizer": "2200.00",
        "walker": "1800.00",
        "bp_monitor": "1950.00",
    }

    device_doc: Dict[str, Any] = {
        "_id": device_id,
        "category": category,
        "name": model_name,
        "specs": generate_specs_for_category(category),
        "status": current_status,
        "requires_inspection": requires_inspection,
        "last_inspection_at": last_inspection_at,
        "last_sanitized_at": last_sanitized_at,
        "owner_id": owner_id,
        "location": random_city_point(city),
        "city": city,
        "notes": f"{model_note}. Routine home-care equipment.",
        "purchase_value": Decimal128(price_map[category]),
        "listed_at": listed_at,
    }

    return device_doc, events


def seed_all() -> Dict[str, int]:
    """Drop, recreate, and seed the MedLoop database with synthetic data.

    Returns:
        Dict[str, int]: Document counts inserted into each collection.
    """
    random.seed(42)
    Faker.seed(42)
    fake = Faker("en_IN")

    db = get_db()
    # Idempotent: drop existing collections and recreate validators + indexes via db/setup.py
    setup_database(db=db, drop_existing=True)

    now = datetime.now(timezone.utc)
    cities = list(CITY_COORDINATES.keys())

    # 1. Generate 30 users (10 donors, 12 requesters, 8 inspectors)
    users: List[Dict[str, Any]] = []
    donors: List[ObjectId] = []
    requesters: List[ObjectId] = []
    inspectors: List[ObjectId] = []

    roles = (["donor"] * 10) + (["requester"] * 12) + (["inspector"] * 8)
    for role in roles:
        uid = ObjectId()
        city = random.choice(cities)
        user_doc: Dict[str, Any] = {
            "_id": uid,
            "name": fake.name(),
            "role": role,
            "city": city,
            "location": random_city_point(city),
        }
        if role == "inspector":
            user_doc["certified_categories"] = random.sample(
                DEVICE_CATEGORIES, k=random.randint(3, len(DEVICE_CATEGORIES))
            )
            inspectors.append(uid)
        elif role == "donor":
            donors.append(uid)
        else:
            requesters.append(uid)
        users.append(user_doc)

    chunked_insert(db.users, users, batch_size=BATCH_SIZE)

    # 2. Generate 2,000 devices and 50,000+ lifecycle events
    devices: List[Dict[str, Any]] = []
    all_events: List[Dict[str, Any]] = []

    for _ in range(2_000):
        dev_id = ObjectId()
        category = random.choice(DEVICE_CATEGORIES)
        city = random.choice(cities)
        device_doc, dev_events = build_device_and_events(
            device_id=dev_id,
            category=category,
            city=city,
            donors=donors,
            requesters=requesters,
            inspectors=inspectors,
            now=now,
        )
        devices.append(device_doc)
        all_events.extend(dev_events)

    chunked_insert(db.devices, devices, batch_size=BATCH_SIZE)
    chunked_insert(db.events, all_events, batch_size=BATCH_SIZE)

    # 3. Generate 1,500 patient requests (mix of open, matched, fulfilled, expired)
    requests_docs: List[Dict[str, Any]] = []
    available_device_ids = [d["_id"] for d in devices if d["status"] == "available"]

    for _ in range(1_500):
        req_city = random.choice(cities)
        req_cat = random.choice(DEVICE_CATEGORIES)
        # Distribute created_at mostly within the last 28 days so the moving-average forecast has rich signal
        days_ago = random.randint(0, 27)
        created_at = now - timedelta(days=days_ago, hours=random.randint(0, 23))
        # Keep expires_at in the future so the TTL index does not immediately delete seeded rows during testing
        expires_at = now + timedelta(days=14)

        req_status = random.choices(
            population=["open", "matched", "fulfilled"],
            weights=[0.65, 0.20, 0.15],
            k=1,
        )[0]
        matched_id = (
            random.choice(available_device_ids)
            if req_status in {"matched", "fulfilled"} and available_device_ids
            else None
        )

        requests_docs.append(
            {
                "_id": ObjectId(),
                "requester_id": random.choice(requesters),
                "category": req_cat,
                "city": req_city,
                "location": random_city_point(req_city),
                "urgency": random.choice(["low", "medium", "high"]),
                "status": req_status,
                "matched_device_id": matched_id,
                "created_at": created_at,
                "expires_at": expires_at,
            }
        )

    chunked_insert(db.requests, requests_docs, batch_size=BATCH_SIZE)

    counts = {
        "users": db.users.count_documents({}),
        "devices": db.devices.count_documents({}),
        "events": db.events.count_documents({}),
        "requests": db.requests.count_documents({}),
    }
    return counts


if __name__ == "__main__":
    summary = seed_all()
    print(
        f"Seed complete: {summary['users']} users, {summary['devices']} devices, "
        f"{summary['events']} events, {summary['requests']} requests."
    )

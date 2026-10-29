"""MongoDB aggregation pipelines for MedLoop (`db/pipelines.py`).

Exposes three functions implementing the required analytical and geospatial
aggregation pipelines, with per-stage comments explaining why each stage is used.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List
from pymongo.database import Database


def get_overdue_devices_pipeline(
    db: Database, now: datetime | None = None
) -> List[Dict[str, Any]]:
    """Pipeline 1: Identify devices overdue for sanitization (>30 days) or inspection (>180 days).

    Matches devices where:
    - `last_sanitized_at` is older than 30 days (or null/missing on active inventory), OR
    - `requires_inspection` is True and `last_inspection_at` is older than 180 days.
    Then looks up the most recent lifecycle event for each overdue device and projects
    a clean audit summary.

    Args:
        db: PyMongo Database handle.
        now: Reference UTC timestamp (defaults to current UTC time).

    Returns:
        List[Dict[str, Any]]: List of overdue device records with their latest event.
    """
    reference_now = now or datetime.now(timezone.utc)
    sanitize_cutoff = reference_now - timedelta(days=30)
    inspection_cutoff = reference_now - timedelta(days=180)

    pipeline: List[Dict[str, Any]] = [
        # Stage 1 ($match): Filter early using predicate pushdown so downstream
        # $lookup only executes on devices that are actually overdue for sanitization
        # (>30 days) or overdue for mandatory inspection (>180 days).
        {
            "$match": {
                "status": {"$nin": ["retired", "rejected"]},
                "$or": [
                    {"last_sanitized_at": {"$lt": sanitize_cutoff}},
                    {"last_sanitized_at": None},
                    {
                        "requires_inspection": True,
                        "$or": [
                            {"last_inspection_at": {"$lt": inspection_cutoff}},
                            {"last_inspection_at": None},
                        ],
                    },
                ],
            }
        },
        # Stage 2 ($lookup): Join the `events` collection using a correlated sub-pipeline
        # that sorts by `ts` descending and takes `$limit: 1`—leveraging the compound index
        # `{device_id: 1, ts: -1}` to fetch only the latest custody/maintenance event.
        {
            "$lookup": {
                "from": "events",
                "let": {"dev_id": "$_id"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$device_id", "$$dev_id"]}}},
                    {"$sort": {"ts": -1}},
                    {"$limit": 1},
                ],
                "as": "latest_event",
            }
        },
        # Stage 3 ($project): Reshape the output document to expose only the relevant
        # compliance timestamps and flatten the 1-element `latest_event` array via `$arrayElemAt`.
        {
            "$project": {
                "_id": 1,
                "name": 1,
                "category": 1,
                "city": 1,
                "status": 1,
                "requires_inspection": 1,
                "last_sanitized_at": 1,
                "last_inspection_at": 1,
                "latest_event": {"$arrayElemAt": ["$latest_event", 0]},
            }
        },
    ]

    return list(db.devices.aggregate(pipeline))


def get_demand_vs_supply_pipeline(db: Database) -> List[Dict[str, Any]]:
    """Pipeline 2: Compare open patient demand against available device supply per {category, city}.

    Groups open requests by `{category, city}`, performs a `$lookup` against `devices`
    to count currently available units in the same `{category, city}`, and projects
    the shortage gap (`open_requests - available_devices`).

    Args:
        db: PyMongo Database handle.

    Returns:
        List[Dict[str, Any]]: Demand, supply, and gap metrics grouped by category and city.
    """
    pipeline: List[Dict[str, Any]] = [
        # Stage 1 ($match): Restrict input exclusively to currently open patient requests
        # before grouping so fulfilled or expired requests do not inflate active demand.
        {"$match": {"status": "open"}},
        # Stage 2 ($group): Aggregate open requests by composite key `{category, city}`
        # to compute total active patient demand in each hyperlocal market segment.
        {
            "$group": {
                "_id": {"category": "$category", "city": "$city"},
                "open_requests": {"$sum": 1},
            }
        },
        # Stage 3 ($lookup): Join the `devices` collection for the matching `{category, city}`
        # and count how many devices currently have `status: "available"`.
        {
            "$lookup": {
                "from": "devices",
                "let": {
                    "req_cat": "$_id.category",
                    "req_city": "$_id.city",
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$category", "$$req_cat"]},
                                    {"$eq": ["$city", "$$req_city"]},
                                    {"$eq": ["$status", "available"]},
                                ]
                            }
                        }
                    },
                    {"$count": "count"},
                ],
                "as": "supply_info",
            }
        },
        # Stage 4 ($project): Extract `available_devices` (defaulting to 0 via `$ifNull`
        # when no available devices exist) and compute `gap = open_requests - available_devices`.
        {
            "$project": {
                "_id": 0,
                "category": "$_id.category",
                "city": "$_id.city",
                "open_requests": 1,
                "available_devices": {
                    "$ifNull": [{"$arrayElemAt": ["$supply_info.count", 0]}, 0]
                },
                "gap": {
                    "$subtract": [
                        "$open_requests",
                        {
                            "$ifNull": [
                                {"$arrayElemAt": ["$supply_info.count", 0]},
                                0,
                            ]
                        },
                    ]
                },
            }
        },
        # Stage 5 ($sort): Order results by largest shortage gap first so coordinators
        # immediately see the highest-deficit category/city pairs.
        {"$sort": {"gap": -1, "city": 1, "category": 1}},
    ]

    return list(db.requests.aggregate(pipeline))


def get_nearest_matches_pipeline(
    db: Database,
    longitude: float,
    latitude: float,
    category: str,
    max_distance_meters: float = 25_000.0,
    now: datetime | None = None,
) -> List[Dict[str, Any]]:
    """Pipeline 3: Find available devices of a category within 25 km using `$geoNear`.

    For categories where `requires_inspection` is True, also enforces that
    `last_inspection_at` is within the past 180 days.

    Args:
        db: PyMongo Database handle.
        longitude: Patient request longitude (WGS84).
        latitude: Patient request latitude (WGS84).
        category: Requested medical equipment category.
        max_distance_meters: Maximum search radius in meters (default 25,000m = 25 km).
        now: Reference UTC timestamp for the 180-day inspection window check.

    Returns:
        List[Dict[str, Any]]: Matching available devices sorted nearest-first with `distance_meters`.
    """
    reference_now = now or datetime.now(timezone.utc)
    inspection_cutoff = reference_now - timedelta(days=180)

    # Note: Because $jsonSchema cannot compare `last_inspection_at` to the current time,
    # we enforce the "inspection within 180 days" rule right here in the $geoNear query filter.
    query_filter: Dict[str, Any] = {
        "category": category,
        "status": "available",
        "$or": [
            {"requires_inspection": False},
            {
                "requires_inspection": True,
                "last_inspection_at": {"$gte": inspection_cutoff},
            },
        ],
    }

    pipeline: List[Dict[str, Any]] = [
        # Stage 1 ($geoNear): Must be the very first stage in the pipeline; uses the
        # 2dsphere index on `devices.location` to scan outward from the patient's GeoJSON Point
        # up to `maxDistance` (25 km), applying the category/status/180-day inspection filter
        # and writing the computed geodesic distance into `distance_meters`.
        {
            "$geoNear": {
                "near": {
                    "type": "Point",
                    "coordinates": [longitude, latitude],
                },
                "distanceField": "distance_meters",
                "maxDistance": max_distance_meters,
                "spherical": True,
                "query": query_filter,
            }
        },
        # Stage 2 ($project): Keep essential device metadata, location, compliance dates,
        # and calculated distance in meters for the patient matching response.
        {
            "$project": {
                "_id": 1,
                "name": 1,
                "category": 1,
                "specs": 1,
                "status": 1,
                "requires_inspection": 1,
                "last_inspection_at": 1,
                "last_sanitized_at": 1,
                "city": 1,
                "location": 1,
                "distance_meters": {"$round": ["$distance_meters", 1]},
            }
        },
    ]

    return list(db.devices.aggregate(pipeline))

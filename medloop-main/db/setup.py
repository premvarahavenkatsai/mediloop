"""Database setup script for MedLoop (`python -m db.setup`).

Creates or updates the four collections (`users`, `devices`, `events`, `requests`)
with strict `$jsonSchema` validators and builds all required indexes.
"""

from typing import Any, Dict, List
from pymongo import ASCENDING, DESCENDING, GEOSPHERE, TEXT
from pymongo.database import Database
from pymongo.errors import CollectionInvalid

from app.db import get_db

DEVICE_CATEGORIES: List[str] = [
    "wheelchair",
    "hospital_bed",
    "oxygen_concentrator",
    "nebulizer",
    "walker",
    "bp_monitor",
]

CATEGORIES_REQUIRING_INSPECTION: List[str] = [
    "oxygen_concentrator",
    "nebulizer",
    "hospital_bed",
]

DEVICE_STATUSES: List[str] = [
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

EVENT_TYPES: List[str] = [
    "listed",
    "inspected",
    "sanitized",
    "serviced",
    "reserved",
    "handed_over",
    "returned",
    "retired",
]

GEOJSON_POINT_SCHEMA: Dict[str, Any] = {
    "bsonType": "object",
    "required": ["type", "coordinates"],
    "properties": {
        "type": {"enum": ["Point"]},
        "coordinates": {
            "bsonType": "array",
            "minItems": 2,
            "maxItems": 2,
            "items": {"bsonType": ["double", "int", "long", "decimal"]},
            "description": "[longitude, latitude] in WGS84",
        },
    },
}


def get_validators() -> Dict[str, Dict[str, Any]]:
    """Return `$jsonSchema` validator definitions for all four collections.

    Returns:
        Dict[str, Dict[str, Any]]: Mapping of collection name to `$jsonSchema` validator dict.
    """
    users_validator: Dict[str, Any] = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["name", "role", "city", "location"],
            "properties": {
                "name": {"bsonType": "string", "minLength": 1},
                "role": {"enum": ["donor", "requester", "inspector"]},
                "city": {"bsonType": "string", "minLength": 1},
                "location": GEOJSON_POINT_SCHEMA,
                "certified_categories": {
                    "bsonType": "array",
                    "items": {"enum": DEVICE_CATEGORIES},
                },
            },
        }
    }

    # NOTE: $jsonSchema cannot compare a field against the current time ($$NOW),
    # so the "inspection within 180 days" check is enforced at query time in the
    # GET /match/{request_id} endpoint rather than in this collection validator.
    # Here we enforce the structural invariant: if requires_inspection is true and
    # status is "available", then last_inspection_at must exist and be a BSON Date.
    devices_validator: Dict[str, Any] = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [
                "category",
                "name",
                "specs",
                "status",
                "requires_inspection",
                "owner_id",
                "location",
                "city",
                "purchase_value",
                "listed_at",
            ],
            "properties": {
                "category": {"enum": DEVICE_CATEGORIES},
                "name": {"bsonType": "string", "minLength": 1},
                "specs": {
                    "bsonType": "object",
                    "properties": {
                        "hours_used": {
                            "bsonType": ["int", "long", "double"],
                            "minimum": 0,
                        }
                    },
                },
                "status": {"enum": DEVICE_STATUSES},
                "requires_inspection": {"bsonType": "bool"},
                "last_inspection_at": {"bsonType": ["date", "null"]},
                "last_sanitized_at": {"bsonType": ["date", "null"]},
                "owner_id": {"bsonType": "objectId"},
                "location": GEOJSON_POINT_SCHEMA,
                "city": {"bsonType": "string", "minLength": 1},
                "notes": {"bsonType": ["string", "null"]},
                "purchase_value": {
                    "bsonType": "decimal",
                    "description": "Monetary value stored as Decimal128 to avoid floating-point rounding errors.",
                },
                "listed_at": {"bsonType": "date"},
            },
            "anyOf": [
                {"properties": {"requires_inspection": {"enum": [False]}}},
                {
                    "properties": {
                        "status": {
                            "enum": [
                                s for s in DEVICE_STATUSES if s != "available"
                            ]
                        }
                    }
                },
                {
                    "required": ["last_inspection_at"],
                    "properties": {
                        "last_inspection_at": {"bsonType": "date"}
                    },
                },
            ],
        }
    }

    events_validator: Dict[str, Any] = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [
                "device_id",
                "type",
                "actor_id",
                "ts",
                "to_status",
                "details",
            ],
            "properties": {
                "device_id": {"bsonType": "objectId"},
                "type": {"enum": EVENT_TYPES},
                "actor_id": {"bsonType": "objectId"},
                "ts": {"bsonType": "date"},
                "from_status": {"bsonType": ["string", "null"]},
                "to_status": {"enum": DEVICE_STATUSES},
                "details": {"bsonType": "object"},
            },
        }
    }

    requests_validator: Dict[str, Any] = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [
                "requester_id",
                "category",
                "city",
                "location",
                "urgency",
                "status",
                "created_at",
                "expires_at",
            ],
            "properties": {
                "requester_id": {"bsonType": "objectId"},
                "category": {"enum": DEVICE_CATEGORIES},
                "city": {"bsonType": "string", "minLength": 1},
                "location": GEOJSON_POINT_SCHEMA,
                "urgency": {"enum": ["low", "medium", "high"]},
                "status": {"enum": ["open", "matched", "fulfilled", "expired"]},
                "matched_device_id": {"bsonType": ["objectId", "null"]},
                "created_at": {"bsonType": "date"},
                "expires_at": {"bsonType": "date"},
            },
        }
    }

    return {
        "users": users_validator,
        "devices": devices_validator,
        "events": events_validator,
        "requests": requests_validator,
    }


def create_collections_and_validators(db: Database) -> None:
    """Create collections with strict `$jsonSchema` validation or update existing ones.

    Enforces `validationLevel="strict"` and `validationAction="error"` on all four
    collections (`users`, `devices`, `events`, `requests`).

    Args:
        db: Target PyMongo Database instance.
    """
    validators = get_validators()
    existing_collections = set(db.list_collection_names())

    for coll_name, validator in validators.items():
        if coll_name not in existing_collections:
            try:
                db.create_collection(
                    coll_name,
                    validator=validator,
                    validationLevel="strict",
                    validationAction="error",
                )
            except CollectionInvalid:
                pass
        else:
            # Update validator in place so running db.setup is idempotent
            db.command(
                {
                    "collMod": coll_name,
                    "validator": validator,
                    "validationLevel": "strict",
                    "validationAction": "error",
                }
            )


def create_indexes(db: Database) -> None:
    """Create all geospatial, compound, text, and TTL indexes required by MedLoop.

    Args:
        db: Target PyMongo Database instance.
    """
    # 1. 2dsphere indexes on devices.location and requests.location for $geoNear and $geoWithin queries
    db.devices.create_index([("location", GEOSPHERE)], name="idx_devices_location_2dsphere")
    db.requests.create_index([("location", GEOSPHERE)], name="idx_requests_location_2dsphere")

    # 2. Compound index on devices {category: 1, status: 1} to accelerate filtering available devices by category
    db.devices.create_index(
        [("category", ASCENDING), ("status", ASCENDING)],
        name="idx_devices_category_status",
    )

    # 3. Compound index on events {device_id: 1, ts: -1}.
    # WHY FIELD ORDER MATTERS (ESR Rule - Equality, Sort, Range):
    # Queries for device history filter by an exact `device_id` (Equality) and sort by `ts` descending (Sort).
    # Putting `device_id` first groups all events for the same device contiguously in the B-tree, and `ts: -1`
    # keeps them pre-sorted newest-first within that partition—eliminating an in-memory blocking SORT stage.
    # If reversed as `{ts: -1, device_id: 1}`, MongoDB would have to scan global timestamps across all devices.
    db.events.create_index(
        [("device_id", ASCENDING), ("ts", DESCENDING)],
        name="idx_events_device_id_ts_desc",
    )

    # 4. Text index on devices.name and devices.notes for keyword search across equipment descriptions
    db.devices.create_index(
        [("name", TEXT), ("notes", TEXT)],
        name="idx_devices_name_notes_text",
    )

    # 5. TTL index on requests.expires_at (expireAfterSeconds: 0) so MongoDB automatically
    # purges unfulfilled requests once their `expires_at` timestamp is reached.
    db.requests.create_index(
        [("expires_at", ASCENDING)],
        expireAfterSeconds=0,
        name="idx_requests_expires_at_ttl",
    )


def setup_database(db: Database | None = None, drop_existing: bool = False) -> None:
    """Initialize collections, `$jsonSchema` validators, and indexes.

    Args:
        db: Optional PyMongo Database handle. Uses `get_db()` if omitted.
        drop_existing: If True, drops the 4 collections before recreating them.
    """
    target_db = db if db is not None else get_db()
    if drop_existing:
        for coll_name in ("users", "devices", "events", "requests"):
            target_db[coll_name].drop()

    create_collections_and_validators(target_db)
    create_indexes(target_db)


if __name__ == "__main__":
    setup_database()
    print("Database collections, $jsonSchema validators, and indexes configured successfully.")

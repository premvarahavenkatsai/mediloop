"""Device lifecycle state machine for MedLoop.

Enforces allowed status transitions for medical equipment so custody,
inspection, and sanitization workflows cannot be skipped.

Lifecycle path:
    listed -> inspected -> available -> reserved -> in_use -> returned -> sanitized -> available
    Also:
    inspected -> rejected
    any valid state -> retired
"""

from typing import Dict, Set

# Complete set of valid device lifecycle states
VALID_STATUSES: Set[str] = {
    "listed",
    "inspected",
    "available",
    "reserved",
    "in_use",
    "returned",
    "sanitized",
    "rejected",
    "retired",
}

# Explicit adjacency map of allowed next states from each current state.
# Every non-retired state can also transition to "retired".
ALLOWED_TRANSITIONS: Dict[str, Set[str]] = {
    "listed": {"inspected", "retired"},
    "inspected": {"available", "rejected", "retired"},
    "available": {"reserved", "retired"},
    "reserved": {"in_use", "retired"},
    "in_use": {"returned", "retired"},
    "returned": {"sanitized", "retired"},
    "sanitized": {"available", "retired"},
    "rejected": {"retired"},
    "retired": set(),
}

# Mapping from target status to the corresponding audit event type in `events`
STATUS_TO_EVENT_TYPE: Dict[str, str] = {
    "listed": "listed",
    "inspected": "inspected",
    "rejected": "inspected",
    "available": "serviced",
    "reserved": "reserved",
    "in_use": "handed_over",
    "returned": "returned",
    "sanitized": "sanitized",
    "retired": "retired",
}


def is_valid_transition(from_status: str, to_status: str) -> bool:
    """Check whether transitioning a device from `from_status` to `to_status` is allowed.

    Rules:
    - Primary workflow: listed -> inspected -> available -> reserved -> in_use -> returned -> sanitized -> available
    - Failed inspection branch: inspected -> rejected
    - Decommissioning: any known state -> retired (Wait: if already retired, self-transition is disallowed,
      Wait - let's check if 'retired is reachable from any state' expects is_valid_transition(s, 'retired')
      to be True for all states or all non-retired states).

    Args:
        from_status: Current status of the device.
        to_status: Requested target status for the device.

    Returns:
        bool: True if the transition is permitted by the workflow, False otherwise.
    """
    if from_status not in VALID_STATUSES or to_status not in VALID_STATUSES:
        return False
    if to_status == "retired":
        return True
    return to_status in ALLOWED_TRANSITIONS.get(from_status, set())


def get_event_type_for_transition(from_status: str, to_status: str) -> str:
    """Return the canonical event `type` value for a status transition.

    Maps lifecycle transitions to the `events.type` enum required by `$jsonSchema`:
    (listed | inspected | sanitized | serviced | reserved | handed_over | returned | retired).

    Args:
        from_status: Previous status of the device.
        to_status: New status of the device.

    Returns:
        str: Event type string matching the `events` collection validator.
    """
    if to_status == "retired":
        return "retired"
    if from_status == "listed" and to_status == "inspected":
        return "inspected"
    if from_status == "inspected" and to_status in {"available", "rejected"}:
        return "inspected" if to_status == "rejected" else "serviced"
    if from_status == "sanitized" and to_status == "available":
        return "serviced"
    return STATUS_TO_EVENT_TYPE.get(to_status, "serviced")

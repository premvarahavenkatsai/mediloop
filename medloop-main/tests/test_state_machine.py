"""Unit tests for the MedLoop device lifecycle state machine (`tests/test_state_machine.py`).

Verifies that:
1. All valid workflow transitions pass (`listed -> inspected -> available -> reserved ->
   in_use -> returned -> sanitized -> available`, plus `inspected -> rejected`).
2. Invalid transitions (skipping inspection, skipping sanitization, backward transitions,
   unknown states) fail.
3. The terminal `retired` state is reachable from any state.
"""

import os
import sys

# Ensure project root is on sys.path when invoked via `pytest` or `python3 tests/test_state_machine.py`
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.state_machine import (  # noqa: E402
    VALID_STATUSES,
    get_event_type_for_transition,
    is_valid_transition,
)


def test_happy_path_transitions_pass() -> None:
    """Verify the complete standard lifecycle and reusable sanitization loop."""
    happy_chain = [
        ("listed", "inspected"),
        ("inspected", "available"),
        ("available", "reserved"),
        ("reserved", "in_use"),
        ("in_use", "returned"),
        ("returned", "sanitized"),
        ("sanitized", "available"),
    ]
    for from_status, to_status in happy_chain:
        assert is_valid_transition(from_status, to_status) is True, (
            f"Expected {from_status} -> {to_status} to be valid"
        )


def test_inspection_rejection_branch_passes() -> None:
    """Verify that an inspected device can transition to 'rejected'."""
    assert is_valid_transition("inspected", "rejected") is True


def test_retired_is_reachable_from_any_state() -> None:
    """Verify that 'retired' can be reached from every valid lifecycle state."""
    for status in VALID_STATUSES:
        assert is_valid_transition(status, "retired") is True, (
            f"Expected 'retired' to be reachable from '{status}'"
        )


def test_invalid_transitions_fail() -> None:
    """Verify that skipping workflow stages or reversing custody order is rejected."""
    invalid_pairs = [
        ("listed", "available"),       # Cannot skip inspection
        ("listed", "reserved"),        # Cannot reserve an uninspected listing
        ("inspected", "in_use"),       # Must become available and reserved first
        ("available", "in_use"),       # Cannot skip reservation step
        ("in_use", "available"),       # Cannot skip return and sanitization
        ("returned", "available"),     # Cannot skip sanitization before re-listing
        ("rejected", "available"),     # Rejected devices cannot become available
        ("retired", "available"),      # Retired devices cannot return to circulation
        ("available", "listed"),       # Cannot move backward to listed
        ("unknown_state", "available"),
        ("available", "unknown_state"),
    ]
    for from_status, to_status in invalid_pairs:
        assert is_valid_transition(from_status, to_status) is False, (
            f"Expected {from_status} -> {to_status} to be invalid"
        )


def test_event_type_mapping() -> None:
    """Verify that state transitions map to valid `events.type` enum values."""
    assert get_event_type_for_transition("listed", "inspected") == "inspected"
    assert get_event_type_for_transition("available", "reserved") == "reserved"
    assert get_event_type_for_transition("reserved", "in_use") == "handed_over"
    assert get_event_type_for_transition("in_use", "returned") == "returned"
    assert get_event_type_for_transition("returned", "sanitized") == "sanitized"
    assert get_event_type_for_transition("sanitized", "available") == "serviced"
    assert get_event_type_for_transition("in_use", "retired") == "retired"


if __name__ == "__main__":
    test_happy_path_transitions_pass()
    test_inspection_rejection_branch_passes()
    test_retired_is_reachable_from_any_state()
    test_invalid_transitions_fail()
    test_event_type_mapping()
    print("All state machine tests passed!")

from __future__ import annotations

import pytest

from v3_core.user_bot.appointment_availability import AppointmentAvailabilityService


class MemoryAvailability:
    def __init__(self, *, status="active", active_count=0):
        self.status = status
        self.active_count = active_count
        self.count_calls = []
        self.updates = []

    def get_inventory_status(self, listing_id):
        return self.status

    def count_appointments_by_statuses(self, *, listing_id, statuses):
        self.count_calls.append((listing_id, tuple(statuses)))
        return self.active_count

    def update_inventory_status(self, *, listing_id, status):
        self.status = status
        self.updates.append((listing_id, status))
        return True


def test_active_listing_becomes_reserved_when_open_appointment_exists():
    repo = MemoryAvailability(status="active", active_count=2)
    result = AppointmentAvailabilityService(repository=repo).recompute("LST_1")

    assert result.previous_status == "active"
    assert result.next_status == "reserved"
    assert result.active_appointment_count == 2
    assert result.changed
    assert repo.updates == [("LST_1", "reserved")]
    assert set(repo.count_calls[0][1]) == {"pending", "assigned", "contacted", "confirmed"}


def test_reserved_listing_returns_active_after_last_open_appointment_is_gone():
    repo = MemoryAvailability(status="reserved", active_count=0)
    result = AppointmentAvailabilityService(repository=repo).recompute("LST_1")

    assert result.previous_status == "reserved"
    assert result.next_status == "active"
    assert result.changed
    assert repo.updates == [("LST_1", "active")]


def test_same_active_or_reserved_status_is_idempotent():
    for status, active_count in (("active", 0), ("reserved", 1)):
        repo = MemoryAvailability(status=status, active_count=active_count)
        result = AppointmentAvailabilityService(repository=repo).recompute("LST_1")

        assert not result.changed
        assert result.next_status == status
        assert repo.updates == []


def test_manual_or_terminal_inventory_states_are_never_overwritten():
    for status in ("pending", "rented", "offline", "inactive"):
        repo = MemoryAvailability(status=status, active_count=5)
        result = AppointmentAvailabilityService(repository=repo).recompute("LST_1")

        assert result.protected_status
        assert not result.changed
        assert result.next_status == status
        assert repo.count_calls == []
        assert repo.updates == []


def test_missing_listing_or_empty_id_is_rejected_before_write():
    with pytest.raises(ValueError, match="listing_id_required"):
        AppointmentAvailabilityService(repository=MemoryAvailability()).recompute("")

    repo = MemoryAvailability(status="")
    with pytest.raises(ValueError, match="listing_not_found"):
        AppointmentAvailabilityService(repository=repo).recompute("LST_404")
    assert repo.updates == []

from __future__ import annotations

import pytest

from v3_core.user_bot.appointment_management import AppointmentManagementService


class MemoryAppointments:
    def __init__(self):
        self.rows = {}
        self.status_updates = []

    def get_for_user(self, appointment_id, user_id):
        row = self.rows.get(int(appointment_id))
        if row and int(row["user_id"]) == int(user_id):
            return dict(row)
        return None

    def update_status(self, *, appointment_id, user_id, status):
        row = self.rows.get(int(appointment_id))
        if row is None or int(row["user_id"]) != int(user_id):
            return False
        row["status"] = str(status)
        self.status_updates.append((int(appointment_id), int(user_id), str(status)))
        return True


def _row(*, status="pending", user_id=123, listing_id="LST_1"):
    return {
        "id": 7,
        "user_id": user_id,
        "listing_id": listing_id,
        "viewing_mode": "video",
        "appointment_date": "09-05",
        "appointment_time": "pm",
        "status": status,
    }


def test_prepare_cancel_is_read_only_and_returns_confirmation_snapshot():
    repo = MemoryAppointments()
    repo.rows[7] = _row(status="confirmed")
    service = AppointmentManagementService(repository=repo)

    preview = service.prepare_cancel(appointment_id=7, user_id=123)

    assert preview.appointment_id == 7
    assert preview.listing_id == "LST_1"
    assert preview.date == "09-05"
    assert preview.time == "pm"
    assert preview.status == "confirmed"
    assert repo.rows[7]["status"] == "confirmed"
    assert repo.status_updates == []


def test_confirm_cancel_rechecks_and_updates_only_own_nonterminal_appointment():
    repo = MemoryAppointments()
    repo.rows[7] = _row(status="assigned")
    service = AppointmentManagementService(repository=repo)

    result = service.confirm_cancel(appointment_id=7, user_id=123)

    assert result.appointment_id == 7
    assert result.listing_id == "LST_1"
    assert result.previous_status == "assigned"
    assert result.status == "cancelled"
    assert result.should_recompute_listing_availability
    assert result.should_sync_channel
    assert not result.should_notify_admin
    assert result.lead_action is None
    assert repo.rows[7]["status"] == "cancelled"
    assert repo.status_updates == [(7, 123, "cancelled")]


def test_cancel_rejects_other_user_done_or_already_cancelled_without_write():
    for row, user_id in (
        (_row(user_id=999), 123),
        (_row(status="done"), 123),
        (_row(status="cancelled"), 123),
    ):
        repo = MemoryAppointments()
        repo.rows[7] = row
        service = AppointmentManagementService(repository=repo)

        with pytest.raises(ValueError, match="appointment_not_editable"):
            service.confirm_cancel(appointment_id=7, user_id=user_id)

        assert repo.status_updates == []


def test_prepare_edit_keeps_listing_mode_and_date_but_requires_time_reselection():
    repo = MemoryAppointments()
    repo.rows[7] = _row(status="contacted")
    service = AppointmentManagementService(repository=repo)

    context = service.prepare_edit(appointment_id=7, user_id=123)

    assert context.appointment_id == 7
    assert context.draft.listing_id == "LST_1"
    assert context.draft.mode == "video"
    assert context.draft.date == "09-05"
    assert context.draft.time == ""
    assert context.draft.source == "appointment_edit"
    assert context.draft.step == "time"


def test_prepare_edit_normalizes_unknown_mode_like_production_default():
    repo = MemoryAppointments()
    row = _row()
    row["viewing_mode"] = "legacy_unknown"
    repo.rows[7] = row
    service = AppointmentManagementService(repository=repo)

    context = service.prepare_edit(appointment_id=7, user_id=123)

    assert context.draft.mode == "offline"


def test_prepare_edit_requires_concrete_listing():
    repo = MemoryAppointments()
    repo.rows[7] = _row(listing_id="")
    service = AppointmentManagementService(repository=repo)

    with pytest.raises(ValueError, match="appointment_requires_concrete_listing"):
        service.prepare_edit(appointment_id=7, user_id=123)

from __future__ import annotations

import pytest

from v3_core.user_bot.appointment_service import (
    AppointmentSubmissionService,
    AppointmentUser,
)
from v3_core.user_bot.appointments import AppointmentDraft


class FakeBookability:
    def __init__(self, allowed=True):
        self.allowed = allowed
        self.calls = []

    def is_bookable(self, listing_id: str) -> bool:
        self.calls.append(listing_id)
        return self.allowed


class MemoryAppointments:
    def __init__(self):
        self.rows = {}
        self.next_id = 1
        self.created = []
        self.updated = []

    def find_exact_unfinished(self, *, user_id, listing_id, mode, date, time):
        for row in self.rows.values():
            if row.get("status") in {"done", "cancelled"}:
                continue
            if (
                int(row["user_id"]),
                row["listing_id"],
                row["viewing_mode"],
                row["appointment_date"],
                row["appointment_time"],
            ) == (int(user_id), listing_id, mode, date, time):
                return dict(row)
        return None

    def get_for_user(self, appointment_id, user_id):
        row = self.rows.get(int(appointment_id))
        if row and int(row["user_id"]) == int(user_id):
            return dict(row)
        return None

    def create(self, values):
        appointment_id = self.next_id
        self.next_id += 1
        row = {"id": appointment_id, **dict(values)}
        self.rows[appointment_id] = row
        self.created.append(dict(row))
        return appointment_id

    def update_schedule(self, *, appointment_id, user_id, mode, date, time, status):
        row = self.rows.get(int(appointment_id))
        if row is None or int(row["user_id"]) != int(user_id):
            return False
        row.update(
            viewing_mode=mode,
            appointment_date=date,
            appointment_time=time,
            status=status,
        )
        self.updated.append(dict(row))
        return True


def _ready(*, source="channel_deeplink"):
    return AppointmentDraft(
        listing_id="LST_1",
        mode="offline",
        date="09-05",
        time="pm",
        source=source,
    )


def test_submit_rechecks_bookability_before_any_write():
    repo = MemoryAppointments()
    checker = FakeBookability(False)
    service = AppointmentSubmissionService(repository=repo, bookability=checker)

    with pytest.raises(ValueError, match="listing_not_bookable"):
        service.submit(user=AppointmentUser(123), draft=_ready())

    assert checker.calls == ["LST_1"]
    assert repo.created == []
    assert repo.updated == []


def test_new_submission_creates_pending_appointment_and_requests_lead_side_effect():
    repo = MemoryAppointments()
    service = AppointmentSubmissionService(repository=repo, bookability=FakeBookability())

    result = service.submit(
        user=AppointmentUser(123, "alice", "Alice"),
        draft=_ready(),
        created_at="2026-09-08 20:00:00",
    )

    assert result.kind == "created"
    assert result.appointment_id == 1
    assert result.lead_action == "appointment_submit"
    assert result.lead_source == "channel_deeplink"
    assert result.should_sync_channel
    assert result.should_notify_admin
    row = repo.rows[1]
    assert row["status"] == "pending"
    assert row["contact_value"] == "@alice"
    assert row["created_at"] == "2026-09-08 20:00:00"


def test_exact_unfinished_duplicate_is_reused_without_new_lead_or_insert():
    repo = MemoryAppointments()
    repo.rows[7] = {
        "id": 7,
        "user_id": 123,
        "listing_id": "LST_1",
        "viewing_mode": "offline",
        "appointment_date": "09-05",
        "appointment_time": "pm",
        "status": "pending",
    }
    service = AppointmentSubmissionService(repository=repo, bookability=FakeBookability())

    result = service.submit(user=AppointmentUser(123), draft=_ready())

    assert result.kind == "reused"
    assert result.appointment_id == 7
    assert result.lead_action is None
    assert repo.created == []


def test_terminal_duplicate_does_not_block_new_appointment():
    repo = MemoryAppointments()
    repo.rows[7] = {
        "id": 7,
        "user_id": 123,
        "listing_id": "LST_1",
        "viewing_mode": "offline",
        "appointment_date": "09-05",
        "appointment_time": "pm",
        "status": "cancelled",
    }
    repo.next_id = 8
    service = AppointmentSubmissionService(repository=repo, bookability=FakeBookability())

    result = service.submit(user=AppointmentUser(123), draft=_ready())

    assert result.kind == "created"
    assert result.appointment_id == 8


def test_edit_updates_only_own_nonterminal_same_listing_and_resets_pending():
    repo = MemoryAppointments()
    repo.rows[4] = {
        "id": 4,
        "user_id": 123,
        "listing_id": "LST_1",
        "viewing_mode": "video",
        "appointment_date": "09-04",
        "appointment_time": "am",
        "status": "confirmed",
    }
    service = AppointmentSubmissionService(repository=repo, bookability=FakeBookability())

    result = service.submit(
        user=AppointmentUser(123),
        draft=_ready(source="appointment_edit"),
        edit_appointment_id=4,
    )

    assert result.kind == "updated"
    assert result.appointment_id == 4
    assert result.lead_action == "appointment_time_update"
    assert result.lead_source == "appointment_edit"
    assert repo.rows[4]["status"] == "pending"
    assert repo.rows[4]["appointment_date"] == "09-05"
    assert repo.rows[4]["appointment_time"] == "pm"


def test_edit_rejects_other_user_terminal_or_different_listing():
    for row, expected in (
        (
            {"id": 1, "user_id": 999, "listing_id": "LST_1", "status": "pending"},
            "appointment_not_editable",
        ),
        (
            {"id": 1, "user_id": 123, "listing_id": "LST_1", "status": "done"},
            "appointment_not_editable",
        ),
        (
            {"id": 1, "user_id": 123, "listing_id": "LST_2", "status": "pending"},
            "appointment_listing_mismatch",
        ),
    ):
        repo = MemoryAppointments()
        repo.rows[1] = {
            "viewing_mode": "offline",
            "appointment_date": "09-01",
            "appointment_time": "am",
            **row,
        }
        service = AppointmentSubmissionService(repository=repo, bookability=FakeBookability())
        with pytest.raises(ValueError, match=expected):
            service.submit(
                user=AppointmentUser(123),
                draft=_ready(),
                edit_appointment_id=1,
            )


def test_incomplete_or_general_request_is_blocked_before_repository_write():
    repo = MemoryAppointments()
    service = AppointmentSubmissionService(repository=repo, bookability=FakeBookability())

    with pytest.raises(ValueError, match="appointment_requires_concrete_listing"):
        service.submit(
            user=AppointmentUser(123),
            draft=AppointmentDraft(listing_id="待推荐", date="09-05", time="pm"),
        )
    with pytest.raises(ValueError, match="appointment_date_and_time_required"):
        service.submit(
            user=AppointmentUser(123),
            draft=AppointmentDraft(listing_id="LST_1", date="09-05"),
        )

    assert repo.created == []

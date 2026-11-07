"""Concrete confirmation dates and conservative read-only legacy interpretation."""

from datetime import date, datetime
import sqlite3
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from v3_core.user_bot.appointment_dates import booking_date, booking_time, legacy_date
from v3_core.user_bot.appointment_history import (
    AppointmentHistoryRecord,
    AppointmentHistoryService,
)
from v3_core.user_bot.appointment_management import AppointmentManagementService
from v3_core.user_bot.appointment_service import (
    AppointmentSubmissionService,
    AppointmentUser,
)
from v3_core.user_bot.appointments import AppointmentDraft
from v3_core.user_bot.takeover_storage import ProductionAppointmentRepository
from v3_core.user_bot.transition_callbacks import (
    TransitionCallback,
    encode_transition_choice,
    parse_transition_callback,
)
from v3_core.user_bot.transition_views import TransitionChoice

TZ = ZoneInfo("Asia/Phnom_Penh")
NOV7 = datetime(2026, 11, 7, 12, tzinfo=TZ)
DEC31 = datetime(2026, 12, 31, 12, tzinfo=TZ)


@pytest.mark.parametrize(
    "raw,now,expected",
    [
        ("11月8日", NOV7, "2026-11-08"),
        ("11-08", NOV7, "2026-11-08"),
        ("2026-11-08", NOV7, "2026-11-08"),
        ("明天", NOV7, "2026-11-08"),
        ("下周三", NOV7, "2026-11-11"),
        ("01-01", DEC31, "2027-01-01"),
        ("1月1日", DEC31, "2027-01-01"),
        ("明天", DEC31, "2027-01-01"),
        ("2027-01-01", DEC31, "2027-01-01"),
        ("2025-11-08", NOV7, "2025-11-08"),
        ("明天", datetime.fromisoformat("2026-12-31T18:00:00+00:00"), "2027-01-02"),
    ],
)
def test_confirmation_freezes_calendar_date_in_phnom_penh(raw, now, expected):
    assert booking_date(raw, now=now) == expected


@pytest.mark.parametrize(
    "raw", ["", "随便", "2026-02-29", "13月1日", "02-30", "下下周三"]
)
def test_invalid_or_unsupported_new_date_is_not_persistable(raw):
    with pytest.raises(ValueError):
        booking_date(raw, now=NOV7)


@pytest.mark.parametrize(
    "raw,created,expected",
    [
        ("11月8日", "2026-11-07 12:00:00", date(2026, 11, 8)),
        ("11-08", "2026-11-07 12:00:00", date(2026, 11, 8)),
        ("01-01", "2026-12-31 12:00:00", date(2027, 1, 1)),
        ("1月1日", "2026-12-31 12:00:00", date(2027, 1, 1)),
        ("2027-01-01", "", date(2027, 1, 1)),
        ("06-02", "2025-06-01 12:00:00", date(2025, 6, 2)),
        ("01-01", "2026-07-01 12:00:00", None),
        ("11月8日", "", None),
        ("2026-02-29", "2026-01-01", None),
        ("不明", "2026-11-07 12:00:00", None),
    ],
)
def test_legacy_year_is_anchored_to_creation_not_viewing_year(raw, created, expected):
    assert legacy_date(raw, created) == expected


def _history(raw, created, *, status="confirmed", now=NOV7):
    record = AppointmentHistoryRecord(
        1, "QL-RF-A2B3", "offline", raw, "pm", status, created
    )
    reader = SimpleNamespace(list_for_user=lambda user_id, **kwargs: (record,))
    inventory = SimpleNamespace(resolve=lambda public_id: None)
    return AppointmentHistoryService(reader=reader, inventory=inventory).build(
        7, now=now
    )


@pytest.mark.parametrize(
    "raw,created,now",
    [
        ("11月8日", "2026-11-07 12:00:00", NOV7),
        ("01-01", "2026-12-31 12:00:00", DEC31),
        ("2027-01-01", "", DEC31),
    ],
)
def test_future_booking_is_visible_not_completed(raw, created, now):
    view = _history(raw, created, now=now)
    assert len(view.items) == 1
    assert "已确认" in view.text and "已完成" not in view.text


@pytest.mark.parametrize(
    "raw,created",
    [
        ("不明", ""),
        ("11月8日", ""),
        ("01-01", "2026-07-01"),
        ("2026-02-29", "2026-01-01"),
    ],
)
def test_ambiguous_or_invalid_legacy_open_booking_remains_visible(raw, created):
    view = _history(raw, created)
    assert len(view.items) == 1
    assert "日期待确认" in view.text and "已完成" not in view.text


def test_provable_past_and_terminal_rows_keep_original_semantics():
    assert "已完成" in _history("2026-11-06", "").text
    assert "已完成" in _history("06-02", "2025-06-01").text
    assert "已取消" in _history("不明", "", status="cancelled").text


@pytest.mark.parametrize(
    "raw,expected",
    [("pm", "pm"), ("20:00", "20:00"), ("晚上8点", "20:00"), ("上午9:30", "09:30")],
)
def test_confirmed_time_preserves_fixed_windows_or_concrete_clock(raw, expected):
    assert booking_time(raw) == expected


@pytest.mark.parametrize("raw", ["随便", "25:00", "晚上99点", "晚上12点", "上午20点"])
def test_unknown_time_cannot_be_confirmed(raw):
    with pytest.raises(ValueError):
        booking_time(raw)


def _repository(tmp_path):
    db = tmp_path / "legacy.db"
    with sqlite3.connect(db) as conn:
        conn.execute("""CREATE TABLE appointments (
            id INTEGER PRIMARY KEY, user_id INTEGER, username TEXT, display_name TEXT,
            listing_id TEXT, viewing_mode TEXT, appointment_date TEXT, appointment_time TEXT,
            contact_value TEXT, note TEXT, status TEXT, created_at TEXT)""")
    return db, ProductionAppointmentRepository(db)


def test_actual_legacy_table_confirmation_edit_cancel_and_schema_unchanged(
    tmp_path, monkeypatch
):
    from v3_core.user_bot import appointment_service

    monkeypatch.setattr(appointment_service, "local_now", lambda: DEC31)
    db, repo = _repository(tmp_path)
    with sqlite3.connect(db) as conn:
        schema = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table'"
        ).fetchall()
    service = AppointmentSubmissionService(
        repository=repo, bookability=SimpleNamespace(is_bookable=lambda listing: True)
    )
    result = service.submit(
        user=AppointmentUser(7),
        draft=AppointmentDraft("L", date="明天", time="晚上8点"),
    )
    row = repo.get_for_user(result.appointment_id, 7)
    assert (row["appointment_date"], row["appointment_time"]) == ("2027-01-01", "20:00")
    manager = AppointmentManagementService(repository=repo)
    edit = manager.prepare_edit(appointment_id=result.appointment_id, user_id=7)
    service.submit(
        user=AppointmentUser(7),
        draft=edit.draft.with_date("2027-01-02").with_time("am"),
        edit_appointment_id=result.appointment_id,
    )
    assert (
        repo.get_for_user(result.appointment_id, 7)["appointment_date"] == "2027-01-02"
    )
    manager.prepare_cancel(appointment_id=result.appointment_id, user_id=7)
    manager.confirm_cancel(appointment_id=result.appointment_id, user_id=7)
    assert repo.get_for_user(result.appointment_id, 7)["status"] == "cancelled"
    with sqlite3.connect(db) as conn:
        assert (
            conn.execute("SELECT sql FROM sqlite_master WHERE type='table'").fetchall()
            == schema
        )


def test_known_legacy_month_day_duplicate_is_reused_without_rewriting_old_row(
    tmp_path, monkeypatch
):
    from v3_core.user_bot import appointment_service

    monkeypatch.setattr(appointment_service, "local_now", lambda: DEC31)
    db, repo = _repository(tmp_path)
    old = repo.create(
        dict(
            user_id=7,
            listing_id="L",
            viewing_mode="offline",
            appointment_date="1月1日",
            appointment_time="pm",
            status="confirmed",
            created_at="2026-12-31 12:00:00",
        )
    )
    service = AppointmentSubmissionService(
        repository=repo, bookability=SimpleNamespace(is_bookable=lambda listing: True)
    )
    result = service.submit(
        user=AppointmentUser(7),
        draft=AppointmentDraft("L", date="2027-01-01", time="pm"),
    )
    assert result.kind == "reused" and result.appointment_id == old
    assert repo.get_for_user(old, 7)["appointment_date"] == "1月1日"
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM appointments").fetchone()[0] == 1


def test_iso_quick_callback_roundtrips_without_losing_year():
    value = TransitionCallback("appointment_quick_slot", "2027-01-01_pm")
    assert (
        parse_transition_callback(
            encode_transition_choice(TransitionChoice("slot", value.kind, value.value))
        )
        == value
    )


@pytest.mark.parametrize("v3", [False, True])
def test_duplicate_scan_finds_older_matching_date_and_legacy_time_alias(tmp_path, v3):
    from v3_core.storage.appointment_repository import SQLiteAppointmentRepository

    if v3:
        db = tmp_path / "v3.db"
        with sqlite3.connect(db) as conn:
            conn.executescript(
                "CREATE TABLE listings_v3(listing_id TEXT PRIMARY KEY); INSERT INTO listings_v3 VALUES('L');"
                + SQLiteAppointmentRepository.DDL
            )
        repo = SQLiteAppointmentRepository(db)
    else:
        db, repo = _repository(tmp_path)
    first = repo.create(
        dict(
            user_id=7,
            listing_id="L",
            viewing_mode="offline",
            appointment_date="1月1日",
            appointment_time="晚上8点",
            status="confirmed",
            created_at="2026-12-31 12:00:00",
        )
    )
    repo.create(
        dict(
            user_id=7,
            listing_id="L",
            viewing_mode="offline",
            appointment_date="2027-01-02",
            appointment_time="20:00",
            status="confirmed",
            created_at="2026-12-31 12:00:00",
        )
    )
    match = repo.find_exact_unfinished(
        user_id=7, listing_id="L", mode="offline", date="2027-01-01", time="20:00"
    )
    assert match["id"] == first


def test_relative_selection_does_not_move_when_confirmation_crosses_midnight(
    tmp_path, monkeypatch
):
    from v3_core.user_bot import appointment_dates, appointment_service
    from v3_core.user_bot.transition_text_actions import TransitionTextActionService
    from v3_core.user_bot.transition_session import APPOINTMENT_SESSION_KEY
    from v3_core.user_bot.transition_actions import APPOINTMENT_AWAITING_DATE_KEY

    monkeypatch.setattr(appointment_dates, "local_now", lambda: DEC31)
    result = TransitionTextActionService().apply(
        "明天",
        {
            APPOINTMENT_SESSION_KEY: {
                "public_listing_id": "QL-RF-A2B3",
                "mode": "offline",
            },
            APPOINTMENT_AWAITING_DATE_KEY: True,
        },
    )
    assert result.appointment.date == "2027-01-01"
    monkeypatch.setattr(
        appointment_service, "local_now", lambda: datetime(2027, 1, 1, 0, 5, tzinfo=TZ)
    )
    db, repo = _repository(tmp_path)
    service = AppointmentSubmissionService(
        repository=repo, bookability=SimpleNamespace(is_bookable=lambda listing: True)
    )
    saved = service.submit(
        user=AppointmentUser(7),
        draft=AppointmentDraft("L", date=result.appointment.date, time="pm"),
    )
    assert (
        repo.get_for_user(saved.appointment_id, 7)["appointment_date"] == "2027-01-01"
    )

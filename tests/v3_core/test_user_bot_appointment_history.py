from __future__ import annotations

from datetime import datetime
import sqlite3
from zoneinfo import ZoneInfo

import pytest

from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.user_bot.appointment_history import (
    AppointmentHistoryService,
    SQLiteAppointmentHistoryReader,
)


PUBLIC_ID = "QL-RF-A2B3"


class EmptyInventory:
    def resolve(self, public_listing_id):
        return None


def _seed_listing(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            """INSERT INTO canonical_records
               (canonical_record_id,source_post_id,schema_version,deal_type,
                facts_json,facts_hash,quality_json)
               VALUES ('CAN_HIST','SRC_HIST','canonical_facts.v1','rent','{}','hash-hist','{}')"""
        )
        conn.execute(
            """INSERT INTO listings_v3
               (listing_id,public_listing_id,canonical_record_id,
                canonical_facts_hash,canonical_facts_schema,inventory_status)
               VALUES ('LST_PRIVATE_1',?,'CAN_HIST','hash-hist','canonical_facts.v1','active')""",
            (PUBLIC_ID,),
        )
        conn.commit()


def _insert_appointment(
    db,
    *,
    appointment_id,
    user_id=123,
    date="09-10",
    time="pm",
    status="pending",
    mode="offline",
    created_at="2026-09-08 12:00:00",
):
    with sqlite3.connect(db) as conn:
        conn.execute(
            """INSERT INTO appointments_v3
               (id,user_id,username,display_name,listing_id,viewing_mode,
                appointment_date,appointment_time,status,created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                appointment_id,
                user_id,
                "alice",
                "Alice",
                "LST_PRIVATE_1",
                mode,
                date,
                time,
                status,
                created_at,
            ),
        )
        conn.commit()


def _service(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    _seed_listing(db)
    return db, AppointmentHistoryService(
        reader=SQLiteAppointmentHistoryReader(db),
        inventory=EmptyInventory(),
    )


def test_reader_is_read_only_and_missing_database_is_not_created(tmp_path):
    db = tmp_path / "missing.db"
    reader = SQLiteAppointmentHistoryReader(db)

    with pytest.raises(FileNotFoundError):
        reader.list_for_user(123)

    assert not db.exists()


def test_reader_returns_public_identity_and_never_internal_listing_id(tmp_path):
    db, service = _service(tmp_path)
    _insert_appointment(db, appointment_id=7)

    records = service.reader.list_for_user(123)

    assert len(records) == 1
    assert records[0].appointment_id == 7
    assert records[0].public_listing_id == PUBLIC_ID
    assert not hasattr(records[0], "listing_id")
    assert "LST_PRIVATE_1" not in repr(records[0])


def test_empty_history_matches_fixed_sha_empty_copy(tmp_path):
    _, service = _service(tmp_path)

    view = service.build(123, now=datetime(2026, 9, 9, tzinfo=ZoneInfo("Asia/Phnom_Penh")))

    assert view.items == ()
    assert view.history_count == 0
    assert "目前还没有看房预约" in view.text
    assert "预约看房" in view.text


def test_history_shows_only_two_upcoming_and_counts_terminal_or_past_rows(tmp_path):
    db, service = _service(tmp_path)
    _insert_appointment(db, appointment_id=1, date="09-10", time="am", created_at="2026-09-08 10:00:00")
    _insert_appointment(db, appointment_id=2, date="09-11", time="pm", mode="video", created_at="2026-09-08 11:00:00")
    _insert_appointment(db, appointment_id=3, date="09-12", time="evening", status="confirmed", created_at="2026-09-08 12:00:00")
    _insert_appointment(db, appointment_id=4, date="09-01", time="pm", created_at="2026-09-08 13:00:00")
    _insert_appointment(db, appointment_id=5, date="09-20", time="pm", status="cancelled", created_at="2026-09-08 14:00:00")

    view = service.build(
        123,
        now=datetime(2026, 9, 9, 12, 0, tzinfo=ZoneInfo("Asia/Phnom_Penh")),
    )

    assert [item.appointment_id for item in view.items] == [3, 2]
    assert view.history_count == 2
    assert "🟢 <b>预约已确认</b>" in view.text
    assert "🎥 实时视频看房" in view.text
    assert "📁 历史预约 · 2 条" in view.text
    assert PUBLIC_ID in view.text
    assert "LST_PRIVATE_1" not in view.text


def test_other_users_appointments_are_not_visible(tmp_path):
    db, service = _service(tmp_path)
    _insert_appointment(db, appointment_id=1, user_id=999)

    view = service.build(123, now=datetime(2026, 9, 9, tzinfo=ZoneInfo("Asia/Phnom_Penh")))

    assert view.items == ()
    assert "目前还没有看房预约" in view.text

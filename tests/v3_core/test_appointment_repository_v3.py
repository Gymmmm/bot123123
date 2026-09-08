from __future__ import annotations

import sqlite3

from v3_core.storage.appointment_repository import SQLiteAppointmentRepository
from v3_core.storage.bootstrap import existing_tables, initialize_v3_storage


def _seed_listing(db, *, listing_id="LST_1", status="active"):
    with sqlite3.connect(db) as conn:
        conn.execute(
            """INSERT INTO canonical_records
               (canonical_record_id,source_post_id,schema_version,deal_type,
                facts_json,facts_hash,quality_json)
               VALUES ('CAN_1','SRC_1','canonical_facts.v1','rent','{}','hash-1','{}')"""
        )
        conn.execute(
            """INSERT INTO listings_v3
               (listing_id,public_listing_id,canonical_record_id,
                canonical_facts_hash,canonical_facts_schema,inventory_status)
               VALUES (?, 'QL-RF-A2B3','CAN_1','hash-1','canonical_facts.v1',?)""",
            (listing_id, status),
        )
        conn.commit()


def _values(*, status="pending", time="pm", user_id=101):
    return {
        "user_id": user_id,
        "username": "tester",
        "display_name": "测试用户",
        "listing_id": "LST_1",
        "viewing_mode": "offline",
        "appointment_date": "9月10日",
        "appointment_time": time,
        "contact_value": "@tester",
        "note": "",
        "status": status,
        "created_at": "2026-09-08 17:00:00",
    }


def test_bootstrap_creates_appointments_v3_without_touching_legacy_appointments(tmp_path):
    db = tmp_path / "v3.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE appointments(marker TEXT NOT NULL)")
        conn.execute("INSERT INTO appointments(marker) VALUES ('legacy-stays')")
        conn.commit()

    initialize_v3_storage(db)

    tables = existing_tables(db)
    assert "appointments_v3" in tables
    assert "appointments" in tables
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT marker FROM appointments").fetchone()[0] == "legacy-stays"
        columns = [row[1] for row in conn.execute("PRAGMA table_info(appointments)")]
    assert columns == ["marker"]


def test_repository_constructor_does_not_initialize_missing_database(tmp_path):
    db = tmp_path / "missing.db"

    SQLiteAppointmentRepository(db)

    assert not db.exists()


def test_create_and_get_for_user_round_trip_only_appointments_v3(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    _seed_listing(db)
    repo = SQLiteAppointmentRepository(db)

    appointment_id = repo.create(_values())
    row = repo.get_for_user(appointment_id, 101)

    assert row is not None
    assert row["listing_id"] == "LST_1"
    assert row["viewing_mode"] == "offline"
    assert row["appointment_date"] == "9月10日"
    assert row["appointment_time"] == "pm"
    assert row["status"] == "pending"
    assert row["contact_value"] == "@tester"
    assert repo.get_for_user(appointment_id, 999) is None


def test_exact_duplicate_reuses_only_unfinished_statuses(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    _seed_listing(db)
    repo = SQLiteAppointmentRepository(db)

    unfinished_ids = []
    for index, status in enumerate(("pending", "assigned", "contacted", "confirmed")):
        unfinished_ids.append(repo.create(_values(status=status, time=f"slot-{index}")))
        found = repo.find_exact_unfinished(
            user_id=101,
            listing_id="LST_1",
            mode="offline",
            date="9月10日",
            time=f"slot-{index}",
        )
        assert found is not None and found["id"] == unfinished_ids[-1]

    for index, status in enumerate(("done", "cancelled"), start=10):
        repo.create(_values(status=status, time=f"slot-{index}"))
        assert repo.find_exact_unfinished(
            user_id=101,
            listing_id="LST_1",
            mode="offline",
            date="9月10日",
            time=f"slot-{index}",
        ) is None


def test_schedule_and_status_updates_require_user_ownership(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    _seed_listing(db)
    repo = SQLiteAppointmentRepository(db)
    appointment_id = repo.create(_values())

    assert not repo.update_schedule(
        appointment_id=appointment_id,
        user_id=999,
        mode="video",
        date="9月11日",
        time="evening",
        status="pending",
    )
    unchanged = repo.get_for_user(appointment_id, 101)
    assert unchanged is not None
    assert unchanged["viewing_mode"] == "offline"
    assert unchanged["appointment_date"] == "9月10日"

    assert repo.update_schedule(
        appointment_id=appointment_id,
        user_id=101,
        mode="video",
        date="9月11日",
        time="evening",
        status="pending",
    )
    changed = repo.get_for_user(appointment_id, 101)
    assert changed is not None
    assert changed["viewing_mode"] == "video"
    assert changed["appointment_date"] == "9月11日"
    assert changed["appointment_time"] == "evening"

    assert not repo.update_status(appointment_id=appointment_id, user_id=999, status="cancelled")
    assert repo.update_status(appointment_id=appointment_id, user_id=101, status="cancelled")
    assert repo.get_for_user(appointment_id, 101)["status"] == "cancelled"


def test_availability_repository_counts_only_requested_v3_statuses_and_updates_listings_v3(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    _seed_listing(db, status="active")
    repo = SQLiteAppointmentRepository(db)

    repo.create(_values(status="pending", time="am"))
    repo.create(_values(status="confirmed", time="pm"))
    repo.create(_values(status="done", time="evening"))

    assert repo.count_appointments_by_statuses(
        listing_id="LST_1",
        statuses=("pending", "assigned", "contacted", "confirmed"),
    ) == 2
    assert repo.get_inventory_status("LST_1") == "active"
    assert repo.update_inventory_status(listing_id="LST_1", status="reserved")
    assert repo.get_inventory_status("LST_1") == "reserved"

from __future__ import annotations

import sqlite3

import pytest

from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.user_bot.appointment_service import AppointmentUser
from v3_core.user_bot.appointment_submit_executor import (
    build_sqlite_appointment_submit_executor,
)
from v3_core.user_bot.public_appointment import PublicAppointmentDraft


def _seed_published_rent(db, *, publication_status="published"):
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
               VALUES ('LST_1','QL-RF-A2B3','CAN_1','hash-1','canonical_facts.v1','active')"""
        )
        conn.execute(
            """INSERT INTO listing_offers
               (offer_id,listing_id,offer_type,currency,monthly_rent_usd,
                offer_status,publication_policy,publishable,publish_block_reason)
               VALUES ('OFF_1','LST_1','rent','USD',800,'active','telegram_rent',1,'')"""
        )
        conn.execute(
            """INSERT INTO publication_packages_v3
               (package_id,listing_id,offer_id,canonical_record_id,package_version,status,
                cover_style,cover_path,gallery_json,post_text,actions_json,snapshot_json,
                frozen_file_hashes_json,source_identity_json,public_token,
                canonical_facts_hash,content_hash)
               VALUES ('PKG_1','LST_1','OFF_1','CAN_1',1,'published',
                       'classic_blue','/frozen/cover.jpg','[]','caption','{}','{}','{}','{}',
                       'qltoken','hash-1','content-hash')"""
        )
        conn.execute(
            """INSERT INTO publication_instances
               (instance_id,package_id,listing_id,offer_id,platform,
                channel_chat_id,channel_message_id,publish_status,post_text)
               VALUES ('PUB_1','PKG_1','LST_1','OFF_1','telegram',
                       '-100123','777',?,'caption')""",
            (publication_status,),
        )
        conn.commit()


def _ready_public(*, time="pm", source="channel_deeplink"):
    return PublicAppointmentDraft(
        "QL-RF-A2B3",
        mode="offline",
        date="9月10日",
        time=time,
        source=source,
    )


def test_sqlite_executor_resolves_public_id_and_persists_only_v3_appointment(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    _seed_published_rent(db)
    executor = build_sqlite_appointment_submit_executor(db)

    execution = executor.execute(
        user=AppointmentUser(123, "alice", "Alice"),
        draft=_ready_public(),
        created_at="2026-09-09 00:50:00",
    )

    assert execution.public_listing_id == "QL-RF-A2B3"
    assert execution.listing_id == "LST_1"
    assert execution.submission.kind == "created"
    assert execution.effects.effects == (
        "recompute_listing_availability",
        "record_lead",
        "sync_channel",
        "notify_admin",
    )
    with sqlite3.connect(db) as conn:
        row = conn.execute("SELECT * FROM appointments_v3").fetchone()
        assert row is not None
        assert conn.execute(
            "SELECT inventory_status FROM listings_v3 WHERE listing_id='LST_1'"
        ).fetchone()[0] == "active"
        legacy = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='appointments'"
        ).fetchone()
        assert legacy is None


def test_exact_repeat_reuses_existing_v3_appointment_without_second_insert(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    _seed_published_rent(db)
    executor = build_sqlite_appointment_submit_executor(db)
    user = AppointmentUser(123)
    draft = _ready_public()

    first = executor.execute(user=user, draft=draft)
    second = executor.execute(user=user, draft=draft)

    assert first.submission.kind == "created"
    assert second.submission.kind == "reused"
    assert second.submission.appointment_id == first.submission.appointment_id
    assert second.effects.effects == ("sync_channel", "notify_admin")
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM appointments_v3").fetchone()[0] == 1


def test_edit_updates_same_v3_row_and_returns_explicit_effect_plan(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    _seed_published_rent(db)
    executor = build_sqlite_appointment_submit_executor(db)
    user = AppointmentUser(123)

    first = executor.execute(user=user, draft=_ready_public(time="am"))
    edited = executor.execute(
        user=user,
        draft=_ready_public(time="evening", source="appointment_edit"),
        edit_appointment_id=first.submission.appointment_id,
    )

    assert edited.submission.kind == "updated"
    assert edited.submission.appointment_id == first.submission.appointment_id
    assert edited.submission.lead_action == "appointment_time_update"
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT appointment_time,status FROM appointments_v3"
        ).fetchall()
    assert rows == [("evening", "pending")]


def test_unpublished_public_listing_is_rejected_before_appointment_write(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    _seed_published_rent(db, publication_status="prepared")
    executor = build_sqlite_appointment_submit_executor(db)

    with pytest.raises(ValueError, match="listing_not_publicly_published"):
        executor.execute(user=AppointmentUser(123), draft=_ready_public())

    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM appointments_v3").fetchone()[0] == 0


def test_executor_factory_does_not_initialize_missing_database(tmp_path):
    db = tmp_path / "missing.db"
    executor = build_sqlite_appointment_submit_executor(db)

    assert not db.exists()
    with pytest.raises(FileNotFoundError):
        executor.execute(user=AppointmentUser(123), draft=_ready_public())
    assert not db.exists()

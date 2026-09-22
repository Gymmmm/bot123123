from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from v3_core.storage.bootstrap import initialize_v3_storage

from qiaolian_dual import appointment_flow, appointments_view, listing as dual_listing
from qiaolian_dual.adapters.public_inventory import PublicInventoryAdapter
from qiaolian_dual.db import Database
import qiaolian_dual.channel_status_sync as channel_status_sync


PUBLIC_ID = "QL-BK-A2B3"
INTERNAL_ID = "LST_1"


def _seed_public_inventory(db):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": INTERNAL_ID,
        "public_listing_id": PUBLIC_ID,
        "listing": {
            "project_name": "富力城",
            "property_type": "公寓",
            "public_location_key": "BKK1",
            "public_location_display": "BKK1",
            "layout": "2房1厅",
            "inventory_status": "active",
        },
        "offer": {
            "offer_type": "rent",
            "monthly_rent_usd": 800,
            "deposit_terms": "押1付1",
            "contract_term": "1年",
            "offer_status": "active",
            "publication_policy": "telegram_rent",
        },
        "canonical_facts": {
            "display_title": "富力城｜2房1厅｜公寓",
            "project_name": "富力城",
            "community_name": "富力城",
            "property_type": "公寓",
            "public_location_display": "BKK1",
            "layout": "2房1厅",
        },
    }
    with sqlite3.connect(db) as conn:
        conn.execute(
            """INSERT INTO canonical_records
               (canonical_record_id,source_post_id,schema_version,parser_revision,
                deal_type,facts_json,facts_hash,quality_score,quality_json,review_status)
               VALUES ('CAN_1','SRC_1','canonical_facts.v1','v1.2','rent','{}','hash-1',100,'{}','approved')"""
        )
        conn.execute(
            """INSERT INTO listings_v3
               (listing_id,public_listing_id,canonical_record_id,project_name,
                property_type,public_location_key,public_location_display,
                layout,canonical_facts_hash,canonical_facts_schema,inventory_status)
               VALUES (?,?,'CAN_1','富力城','公寓','BKK1','BKK1',
                       '2房1厅','hash-1','canonical_facts.v1','active')""",
            (INTERNAL_ID, PUBLIC_ID),
        )
        conn.execute(
            """INSERT INTO listing_offers
               (offer_id,listing_id,offer_type,currency,monthly_rent_usd,
                offer_status,publication_policy,publishable,publish_block_reason)
               VALUES ('OFF_1',?,'rent','USD',800,'active','telegram_rent',1,'')""",
            (INTERNAL_ID,),
        )
        conn.execute(
            """INSERT INTO publication_packages_v3
               (package_id,listing_id,offer_id,canonical_record_id,package_version,status,
                cover_style,cover_path,gallery_json,post_text,actions_json,snapshot_json,
                frozen_file_hashes_json,source_identity_json,public_token,
                canonical_facts_hash,content_hash)
               VALUES ('PKG_1',?,'OFF_1','CAN_1',1,'published',
                       'classic_blue','/frozen/cover.jpg','[]','caption','{}',?,
                       '{}','{}','token-1','hash-1','content-1')""",
            (INTERNAL_ID, json.dumps(snapshot, ensure_ascii=False)),
        )
        conn.execute(
            """INSERT INTO publication_instances
               (instance_id,package_id,listing_id,offer_id,platform,
                channel_chat_id,channel_message_id,publish_status,post_text)
               VALUES ('PUB_1','PKG_1',?,'OFF_1','telegram',
                       '-100123','777','published','caption')""",
            (INTERNAL_ID,),
        )
        conn.commit()


def _db(tmp_path):
    path = tmp_path / "combined.db"
    initialize_v3_storage(path)
    Database(path)
    _seed_public_inventory(path)
    return path


@pytest.mark.asyncio
async def test_3858_submit_persists_internal_id_but_keeps_public_lead_identity(tmp_path, monkeypatch):
    path = _db(tmp_path)
    dual_db = Database(path)

    monkeypatch.setattr(appointment_flow, "db", dual_db)
    monkeypatch.setattr(appointment_flow, "DB_PATH", str(path))
    monkeypatch.setattr(dual_listing, "DB_PATH", str(path))
    monkeypatch.setattr(channel_status_sync, "sync_channel_listing_status", AsyncMock(return_value=True))

    leads = []

    def create_lead(user, **kwargs):
        leads.append(kwargs)
        return 77

    notify = AsyncMock()
    query = SimpleNamespace(edit_message_text=AsyncMock())
    user = SimpleNamespace(id=1001, username="tester", first_name="Tester", last_name="")
    update = SimpleNamespace(callback_query=query, effective_message=None, effective_user=user)
    context = SimpleNamespace(user_data={"appt": {}}, bot=SimpleNamespace())

    appt = {
        "listing_id": PUBLIC_ID,
        "mode": "offline",
        "date": "09-23",
        "time": "pm",
        "source": "listing_card",
        "touch_payload": {},
    }

    result = await appointment_flow._submit_appointment(
        update,
        context,
        appt,
        create_lead_fn=create_lead,
        notify_admins_fn=notify,
    )

    assert result == appointment_flow.MAIN
    rows = dual_db.list_appointments(1001, limit=10)
    assert len(rows) == 1
    assert rows[0]["listing_id"] == INTERNAL_ID
    assert rows[0]["status"] == "pending"
    assert leads[0]["listing_id"] == PUBLIC_ID
    assert leads[0]["payload"]["appointment_id"] == rows[0]["id"]


@pytest.mark.asyncio
async def test_duplicate_guard_uses_internal_id_and_does_not_create_second_row(tmp_path, monkeypatch):
    path = _db(tmp_path)
    dual_db = Database(path)

    monkeypatch.setattr(appointment_flow, "db", dual_db)
    monkeypatch.setattr(appointment_flow, "DB_PATH", str(path))
    monkeypatch.setattr(dual_listing, "DB_PATH", str(path))
    monkeypatch.setattr(channel_status_sync, "sync_channel_listing_status", AsyncMock(return_value=True))

    user = SimpleNamespace(id=1002, username="tester2", first_name="Tester", last_name="")
    notify = AsyncMock()
    create_lead = lambda user, **kwargs: 88

    async def submit_once():
        query = SimpleNamespace(edit_message_text=AsyncMock())
        update = SimpleNamespace(callback_query=query, effective_message=None, effective_user=user)
        context = SimpleNamespace(user_data={"appt": {}}, bot=SimpleNamespace())
        appt = {
            "listing_id": PUBLIC_ID,
            "mode": "offline",
            "date": "09-23",
            "time": "pm",
            "source": "listing_card",
            "touch_payload": {},
        }
        await appointment_flow._submit_appointment(
            update,
            context,
            appt,
            create_lead_fn=create_lead,
            notify_admins_fn=notify,
        )
        return query

    await submit_once()
    second_query = await submit_once()

    rows = dual_db.list_appointments(1002, limit=10)
    assert len(rows) == 1
    assert rows[0]["listing_id"] == INTERNAL_ID
    rendered = second_query.edit_message_text.await_args.args[0]
    assert "已经有一条进行中的预约" in rendered


def test_reverse_identity_for_appointment_ui_uses_v3_public_id(tmp_path, monkeypatch):
    path = _db(tmp_path)
    adapter = PublicInventoryAdapter(str(path))

    assert adapter.resolve_internal_id(PUBLIC_ID) == INTERNAL_ID
    assert adapter.public_id_for_internal(INTERNAL_ID) == PUBLIC_ID

    monkeypatch.setattr(appointments_view, "DB_PATH", str(path))
    assert appointments_view._appointment_public_listing_id(INTERNAL_ID) == PUBLIC_ID
    assert appointments_view._appointment_listing_compact(INTERNAL_ID) == PUBLIC_ID


def test_t3_flow_no_longer_directly_creates_new_appointment():
    source = open("qiaolian_dual/appointment_flow.py", encoding="utf-8").read()

    assert "AppointmentAdapter(" in source
    assert "appointment_adapter.submit(appointment_payload)" in source
    assert "appointment_id = db.create_appointment({" not in source

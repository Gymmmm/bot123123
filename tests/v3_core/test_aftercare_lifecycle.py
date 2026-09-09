from __future__ import annotations

import sqlite3

import pytest

from v3_core.storage.aftercare_lifecycle import AftercareLifecycleService
from v3_core.storage.bootstrap import initialize_v3_storage


def _seed(db):
    with sqlite3.connect(db) as conn:
        conn.execute("""INSERT INTO canonical_records
            (canonical_record_id,source_post_id,schema_version,deal_type,facts_json,facts_hash)
            VALUES ('CAN_1','SRC_1','canonical_facts.v1','rent','{}','hash1')""")
        conn.execute("""INSERT INTO listings_v3
            (listing_id,public_listing_id,canonical_record_id,property_type,canonical_facts_hash,canonical_facts_schema)
            VALUES ('L_1','QL-PP-TEST','CAN_1','公寓','hash1','canonical_facts.v1')""")
        conn.execute("""INSERT INTO listing_offers
            (offer_id,listing_id,offer_type,monthly_rent_usd,offer_status,publication_policy,publishable)
            VALUES ('OFF_1','L_1','rent',800,'active','telegram_rent',1)""")
        conn.execute("""INSERT INTO publication_instances
            (instance_id,package_id,listing_id,offer_id,platform,channel_chat_id,channel_message_id)
            VALUES ('PUB_1','PKG_1','L_1','OFF_1','telegram','-1001','101')""")
        conn.execute("""INSERT INTO tenant_bindings_v3
            (user_id,binding_code,property_name,lease_end_date,rent_day,monthly_rent,
             contract_start_date,contract_end_date,deposit_months,status,created_at)
            VALUES (7,'BIND-1','富力城','2027-01-01',5,800,'2026-01-01','2027-01-01',1,'active','2026-01-01')""")
        conn.execute("""INSERT INTO repair_tickets_v3
            (request_token,user_id,binding_id,property_name,issue_key,issue_type,description,time_slot,status,created_at)
            VALUES ('repair-1',7,1,'富力城','water','水管','漏水','上午','new','2026-09-09')""")
        conn.commit()


def test_case_from_binding_is_idempotent_and_audited(tmp_path):
    db = tmp_path / "core.db"
    initialize_v3_storage(db); _seed(db)
    service = AftercareLifecycleService(db)
    first = service.create_case_from_binding(tenant_binding_id=1, listing_id="L_1", offer_id="OFF_1", post_id="PUB_1")
    second = service.create_case_from_binding(tenant_binding_id=1, listing_id="L_1", offer_id="OFF_1", post_id="PUB_1")
    assert first.case_id == second.case_id
    active = service.transition_case(first.case_id, to_status="active", actor_id="ops")
    assert active.status == "active"
    with sqlite3.connect(db) as conn:
        event = conn.execute("SELECT from_status,to_status FROM rental_case_events_v3 WHERE case_id=?", (first.case_id,)).fetchone()
    assert event == ("pending_handover", "active")


def test_repair_task_requires_same_tenant_lineage(tmp_path):
    db = tmp_path / "core.db"
    initialize_v3_storage(db); _seed(db)
    service = AftercareLifecycleService(db)
    case = service.create_case_from_binding(tenant_binding_id=1, listing_id="L_1", offer_id="OFF_1", post_id="PUB_1")
    task = service.create_repair_task(case_id=case.case_id, repair_ticket_id=1, priority="urgent")
    repeated = service.create_repair_task(case_id=case.case_id, repair_ticket_id=1, priority="urgent")
    assert task.task_id == repeated.task_id
    assert task.repair_ticket_id == 1


def test_closed_case_cannot_accept_repair_task(tmp_path):
    db = tmp_path / "core.db"
    initialize_v3_storage(db); _seed(db)
    service = AftercareLifecycleService(db)
    case = service.create_case_from_binding(tenant_binding_id=1, listing_id="L_1")
    service.transition_case(case.case_id, to_status="active", actor_id="ops")
    service.transition_case(case.case_id, to_status="closed", actor_id="ops")
    with pytest.raises(ValueError, match="operations_case_not_open"):
        service.create_repair_task(case_id=case.case_id, repair_ticket_id=1)

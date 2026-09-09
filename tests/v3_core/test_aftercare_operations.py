from __future__ import annotations

import sqlite3

import pytest

from v3_core.storage.aftercare_repository import AftercareOperationsRepository
from v3_core.storage.bootstrap import initialize_v3_storage


def _seed_inventory(db_path):
    with sqlite3.connect(db_path) as conn:
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
        conn.commit()


def test_aftercare_schema_is_initialized_with_core(tmp_path):
    db = tmp_path / "core.db"
    initialize_v3_storage(db)
    with sqlite3.connect(db) as conn:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"rental_cases_v3", "operations_tasks_v3", "operations_task_events_v3"} <= tables


def test_case_preserves_draft_listing_post_lineage(tmp_path):
    db = tmp_path / "core.db"
    initialize_v3_storage(db); _seed_inventory(db)
    repo = AftercareOperationsRepository(db)
    case = repo.create_case(user_id=7, draft_id="DRAFT_9", listing_id="L_1", post_id="PUB_1", offer_id="OFF_1")
    assert case.draft_id == "DRAFT_9"
    assert case.listing_id == "L_1"
    assert case.post_id == "PUB_1"
    assert case.offer_id == "OFF_1"


def test_case_rejects_post_from_another_listing(tmp_path):
    db = tmp_path / "core.db"
    initialize_v3_storage(db); _seed_inventory(db)
    with sqlite3.connect(db) as conn:
        conn.execute("""INSERT INTO listings_v3
            (listing_id,public_listing_id,canonical_record_id,property_type,canonical_facts_hash,canonical_facts_schema)
            VALUES ('L_2','QL-PP-TEST2','CAN_1','公寓','hash1','canonical_facts.v1')""")
        conn.execute("""INSERT INTO publication_instances
            (instance_id,package_id,listing_id,platform,channel_chat_id,channel_message_id)
            VALUES ('PUB_2','PKG_2','L_2','telegram','-1001','102')""")
        conn.commit()
    repo = AftercareOperationsRepository(db)
    with pytest.raises(ValueError, match="aftercare_post_listing_mismatch"):
        repo.create_case(user_id=7, listing_id="L_1", post_id="PUB_2")


def test_operations_task_is_idempotent_and_state_guarded(tmp_path):
    db = tmp_path / "core.db"
    initialize_v3_storage(db); _seed_inventory(db)
    repo = AftercareOperationsRepository(db)
    case = repo.create_case(user_id=7, listing_id="L_1", post_id="PUB_1")
    first = repo.create_task(request_token="req-1", case_id=case.case_id, listing_id="L_1", post_id="PUB_1",
                             task_type="handover", priority="high", summary="交房")
    second = repo.create_task(request_token="req-1", case_id=case.case_id, listing_id="L_1", post_id="PUB_1",
                              task_type="handover", priority="high", summary="重复提交")
    assert first.task_id == second.task_id
    with pytest.raises(ValueError, match="operations_assignee_required"):
        repo.transition_task(first.task_id, to_status="assigned")
    assigned = repo.transition_task(first.task_id, to_status="assigned", assignee_id="ops-1", actor_id="admin")
    assert assigned.status == "assigned"
    resolved = repo.transition_task(first.task_id, to_status="resolved", actor_id="ops-1", note="完成")
    assert resolved.status == "resolved"
    with pytest.raises(ValueError, match="operations_transition_invalid"):
        repo.transition_task(first.task_id, to_status="in_progress", actor_id="ops-1")


def test_operations_queue_prioritizes_urgent(tmp_path):
    db = tmp_path / "core.db"
    initialize_v3_storage(db); _seed_inventory(db)
    repo = AftercareOperationsRepository(db)
    repo.create_task(request_token="normal", listing_id="L_1", task_type="cleaning", priority="normal")
    urgent = repo.create_task(request_token="urgent", listing_id="L_1", task_type="repair", priority="urgent")
    assert repo.queue()[0].task_id == urgent.task_id

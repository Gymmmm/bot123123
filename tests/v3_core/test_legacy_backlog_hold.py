from __future__ import annotations

from pathlib import Path
import sqlite3

from v3_core.publishing.autopilot import AutoPublishRepository
from v3_core.publishing.autopilot_anomalies import FinalAutoPublishRepository
from v3_core.storage.bootstrap import initialize_v3_storage


def _seed_queued(db: Path, *, offer_ids: tuple[str, ...]) -> None:
    initialize_v3_storage(db)
    with sqlite3.connect(db) as conn:
        for index, offer_id in enumerate(offer_ids, start=1):
            listing_id = f"l_{index}"
            review_id = f"r_{index}"
            canonical_id = f"c_{index}"
            conn.execute(
                """INSERT INTO canonical_records
                   (canonical_record_id,source_post_id,schema_version,deal_type,facts_json,facts_hash,
                    quality_score,quality_json,review_status,created_at)
                   VALUES (?,?,'v3','rent','{}',?,90,'{}','unreviewed',CURRENT_TIMESTAMP)""",
                (canonical_id, f"src_{index}", f"hash_{index}"),
            )
            conn.execute(
                """INSERT INTO listings_v3
                   (listing_id,public_listing_id,canonical_record_id,project_name,property_type,
                    public_location_display,layout,display_title,canonical_facts_hash,
                    canonical_facts_schema,inventory_status,created_at,updated_at)
                   VALUES (?,?,?,?, '公寓','BKK1','2房','标题',?,'v3','pending',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
                (listing_id, f"QL-T{index}", canonical_id, f"项目{index}", f"hash_{index}"),
            )
            conn.execute(
                """INSERT INTO listing_offers
                   (offer_id,listing_id,offer_type,offer_status,monthly_rent_usd,publication_policy,publishable)
                   VALUES (?,?,'rent','active',800,'telegram_rent',1)""",
                (offer_id, listing_id),
            )
            conn.execute(
                """INSERT INTO review_items
                   (review_id,canonical_record_id,listing_id,offer_id,review_status,review_score,review_note)
                   VALUES (?,?,?,?, 'approved',90,'')""",
                (review_id, canonical_id, listing_id, offer_id),
            )
            conn.execute(
                """INSERT INTO publisher_auto_items_v3
                   (offer_id,listing_id,review_id,state,ignored,origin)
                   VALUES (?,?,?,'queued',0,'collector')""",
                (offer_id, listing_id, review_id),
            )
        conn.commit()


def test_hold_queued_backlog_pauses_auto_delivery_but_keeps_new_queued(tmp_path: Path):
    db = tmp_path / "hold.db"
    _seed_queued(db, offer_ids=("OFF_OLD_1", "OFF_OLD_2"))
    repo = AutoPublishRepository(db)
    repo.ensure_defaults()

    held = repo.hold_queued_backlog()
    assert held == 2
    assert repo.queue_count() == 0
    assert repo.held_count() == 2
    assert repo.next_item() is None

    with sqlite3.connect(db) as conn:
        conn.execute(
            """INSERT INTO canonical_records
               (canonical_record_id,source_post_id,schema_version,deal_type,facts_json,facts_hash,
                quality_score,quality_json,review_status,created_at)
               VALUES ('c_new','src_new','v3','rent','{}','hash_new',90,'{}','unreviewed',CURRENT_TIMESTAMP)"""
        )
        conn.execute(
            """INSERT INTO listings_v3
               (listing_id,public_listing_id,canonical_record_id,project_name,property_type,
                public_location_display,layout,display_title,canonical_facts_hash,
                canonical_facts_schema,inventory_status,created_at,updated_at)
               VALUES ('l_new','QL-NEW','c_new','新项目','公寓','BKK1','1房','新房','hash_new','v3',
                       'pending',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"""
        )
        conn.execute(
            """INSERT INTO listing_offers
               (offer_id,listing_id,offer_type,offer_status,monthly_rent_usd,publication_policy,publishable)
               VALUES ('OFF_NEW','l_new','rent','active',900,'telegram_rent',1)"""
        )
        conn.execute(
            """INSERT INTO review_items
               (review_id,canonical_record_id,listing_id,offer_id,review_status,review_score,review_note)
               VALUES ('r_new','c_new','l_new','OFF_NEW','approved',90,'')"""
        )
        conn.execute(
            """INSERT INTO publisher_auto_items_v3
               (offer_id,listing_id,review_id,state,ignored,origin)
               VALUES ('OFF_NEW','l_new','r_new','queued',0,'collector')"""
        )
        conn.commit()

    assert repo.queue_count() == 1
    assert repo.held_count() == 2
    nxt = repo.next_item()
    assert nxt is not None
    assert nxt["offer_id"] == "OFF_NEW"


def test_policy_sync_preserves_held_backlog(tmp_path: Path):
    db = tmp_path / "policy_hold.db"
    _seed_queued(db, offer_ids=("OFF_HOLD",))
    repo = FinalAutoPublishRepository(db)
    repo.ensure_defaults()
    assert repo.hold_queued_backlog() == 1

    repo.sync_candidates()
    assert repo.held_count() == 1
    assert repo.queue_count() == 0
    with sqlite3.connect(db) as conn:
        state = conn.execute(
            "SELECT state, reason_code FROM publisher_auto_items_v3 WHERE offer_id='OFF_HOLD'"
        ).fetchone()
    assert state[0] == "held"
    assert state[1] == "legacy_backlog"


def test_release_held_backlog_returns_to_queue(tmp_path: Path):
    db = tmp_path / "release.db"
    _seed_queued(db, offer_ids=("OFF_A", "OFF_B"))
    repo = AutoPublishRepository(db)
    repo.ensure_defaults()
    repo.hold_queued_backlog()
    released = repo.release_held_backlog()
    assert released == 2
    assert repo.queue_count() == 2
    assert repo.held_count() == 0

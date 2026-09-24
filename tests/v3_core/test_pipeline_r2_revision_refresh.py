import sqlite3

from v3_core.ingest.intake_service import IntakeService, SourceIntake
from v3_core.ingest.source_repository import SourceRepository
from v3_core.inventory.canonical_facts import PARSER_REVISION, canonicalize_source
from v3_core.inventory.service import InventoryMaterializationService
from v3_core.parser.worker import CanonicalWorker
from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.storage.inventory_reader import InventoryReader
from v3_core.storage.inventory_repository import InventoryRepository


def _images():
    return [
        {"local_path": f"/tmp/r2-refresh-{i}.jpg", "file_hash": f"r2-{i}"}
        for i in range(4)
    ]


def test_old_revision_reenters_worker_and_manual_override_is_reapplied(tmp_path):
    db = tmp_path / "refresh.sqlite3"
    initialize_v3_storage(db)
    sources = SourceRepository(str(db))
    inventory = InventoryRepository(str(db))
    reader = InventoryReader(str(db))
    intake = IntakeService(sources, min_listing_images=4)
    source = intake.persist(
        SourceIntake(
            source_type="telegram_channel",
            source_name="publisher_admin",
            source_post_id="l271-source",
            source_url="",
            raw_text="""【香格里拉公寓出租】
房型：2+1
楼层：48（T1河景）
租金：1100💵
押金：押一付一
合同：1年""",
            raw_images=_images(),
        )
    )

    old = canonicalize_source("香格里拉公寓出租\n租金：1100💵")
    old["parser_revision"] = "v1.2"
    old["canonical_facts_hash"] = "old-r2-hash"
    canonical = inventory.store_canonical(source_post_id=source.source_post_pk, facts=old)
    InventoryMaterializationService(inventory).materialize(
        canonical_record_id=str(canonical["canonical_record_id"]),
        listing_id="l_271",
        public_listing_id="QL-PP-B8E8",
        create_review=True,
    )
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO listing_identity_reservations_v3(listing_id,public_id,canonical_record_id) VALUES (?,?,?)",
            ("l_271", "QL-PP-B8E8", str(canonical["canonical_record_id"])),
        )
        conn.commit()
    inventory.add_override(
        canonical_record_id=str(canonical["canonical_record_id"]),
        field_name="layout",
        old_value=None,
        new_value="2+1",
        reason="publisher_admin_edit",
        operator_id="admin",
    )
    sources.update_parse_status(source.source_post_pk, "parsed")

    worker = CanonicalWorker(str(db))
    assert worker.pending_ids() == [source.source_post_pk]
    result = worker.process_one(source.source_post_pk)
    assert result.status == "materialized"
    assert result.listing_id == "l_271"
    assert worker.pending_ids() == []

    listing = reader.listing("l_271")
    refreshed = reader.canonical(str(listing["canonical_record_id"]))
    facts = refreshed["facts"]
    assert refreshed["parser_revision"] == PARSER_REVISION == "v1.4"
    assert facts["project_name"] == "The Peak 香格里拉"
    assert facts["monthly_rent_usd"] == 1100
    assert facts["layout"] == "2+1"
    assert {item["field"] for item in facts["manual_overrides"]} == {"layout"}
    assert "missing_layout" not in facts["quality"]["blocking_flags"]
    assert listing["public_listing_id"] == "QL-PP-B8E8"


def test_current_revision_is_not_reselected(tmp_path):
    db = tmp_path / "current.sqlite3"
    initialize_v3_storage(db)
    sources = SourceRepository(str(db))
    inventory = InventoryRepository(str(db))
    source_id = sources.save_source_post(
        source_id=None,
        source_type="telegram_channel",
        source_name="collector",
        source_post_id="current",
        source_url="",
        source_author="",
        raw_text="富力城 公寓 2房1厅 租金 $800/月",
        raw_images=_images(),
        raw_videos=[],
        raw_contact="",
        raw_meta={},
        dedupe_hash="current",
        parse_status="parsed",
    )
    facts = canonicalize_source("富力城 公寓 2房1厅 租金 $800/月")
    inventory.store_canonical(source_post_id=source_id, facts=facts)
    assert CanonicalWorker(str(db)).pending_ids() == []

from v3_core.ingest.intake_service import IntakeService, SourceIntake
from v3_core.ingest.source_repository import SourceRepository
from v3_core.inventory.service import InventoryMaterializationService
from v3_core.parser.service import CanonicalParseService
from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.storage.inventory_repository import InventoryRepository


def _images(count: int = 4):
    return [
        {
            "local_path": f"/tmp/v3-flow-{i}.jpg",
            "file_hash": f"flow-hash-{i}",
            "telegram_file_id": str(100 + i),
            "telegram_file_unique_id": str(200 + i),
        }
        for i in range(count)
    ]


def _services(tmp_path):
    db = tmp_path / "v3-flow.sqlite3"
    initialize_v3_storage(db)
    sources = SourceRepository(str(db))
    inventory = InventoryRepository(str(db))
    intake = IntakeService(sources, min_listing_images=4)
    parser = CanonicalParseService(sources, inventory)
    materializer = InventoryMaterializationService(inventory)
    return db, sources, inventory, intake, parser, materializer


def test_sale_is_parsed_stored_and_materialized_not_skipped(tmp_path):
    _db, sources, inventory, intake, parser, materializer = _services(tmp_path)

    source = intake.persist(
        SourceIntake(
            source_type="telegram_channel",
            source_name="sale-source",
            source_post_id="9001",
            source_url="https://t.me/c/1/9001",
            raw_text="富力城 公寓 2房1厅 售价 $160,000",
            raw_images=_images(),
        )
    )
    parsed = parser.parse_source_post(source.source_post_pk)

    assert parsed.status == "parsed"
    assert parsed.facts["sale_price_usd"] == 160000
    assert sources.get_source_post(source.source_post_pk)["parse_status"] == "parsed"

    result = materializer.materialize(
        canonical_record_id=parsed.canonical_record_id,
        listing_id="v3_l_9001",
        public_listing_id="QL-PP-A2B3",
    )
    assert len(result.offer_ids) == 1

    with inventory._connect() as conn:
        offer = conn.execute(
            "SELECT * FROM listing_offers WHERE offer_id=?", (result.offer_ids[0],)
        ).fetchone()
    assert offer["offer_type"] == "sale"
    assert offer["sale_price_usd"] == 160000
    assert offer["publication_policy"] == "store_only"
    assert offer["publishable"] == 0
    assert offer["publish_block_reason"] == "sale_not_enabled_for_telegram"


def test_rent_flows_to_rent_offer_but_starts_review_blocked(tmp_path):
    _db, _sources, inventory, intake, parser, materializer = _services(tmp_path)

    source = intake.persist(
        SourceIntake(
            source_type="telegram_channel",
            source_name="rent-source",
            source_post_id="9002",
            source_url="https://t.me/c/1/9002",
            raw_text="富力城 公寓 2房1厅 租金 $800/月 押1付1 1年",
            raw_images=_images(),
        )
    )
    parsed = parser.parse_source_post(source.source_post_pk)
    result = materializer.materialize(
        canonical_record_id=parsed.canonical_record_id,
        listing_id="v3_l_9002",
        public_listing_id="QL-PP-C4D5",
    )

    with inventory._connect() as conn:
        offer = conn.execute(
            "SELECT * FROM listing_offers WHERE offer_id=?", (result.offer_ids[0],)
        ).fetchone()
    assert offer["offer_type"] == "rent"
    assert offer["monthly_rent_usd"] == 800
    assert offer["publication_policy"] == "telegram_rent"
    assert offer["publishable"] == 0
    assert offer["publish_block_reason"] == "review_required"
    assert result.review_ids

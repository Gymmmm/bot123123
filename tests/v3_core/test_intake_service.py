import json

from v3_core.ingest.intake_service import IntakeService, SourceIntake
from v3_core.ingest.source_repository import SourceRepository
from v3_core.storage.bootstrap import initialize_v3_storage


def _images(count: int):
    return [
        {
            "local_path": f"/tmp/photo-{index}.jpg",
            "file_hash": f"hash-{index}",
            "telegram_file_id": str(1000 + index),
            "telegram_file_unique_id": str(2000 + index),
        }
        for index in range(count)
    ]


def test_intake_persists_raw_evidence_and_sanitized_copy_without_parsing(tmp_path):
    db = tmp_path / "v3.sqlite3"
    initialize_v3_storage(db)
    repository = SourceRepository(str(db))
    service = IntakeService(repository, min_listing_images=4)

    result = service.persist(
        SourceIntake(
            source_type="telegram_channel",
            source_name="collector-a",
            source_post_id="123",
            source_url="https://t.me/c/1/123",
            raw_text="富力城 2房1厅 租金 $800/月\n联系微信 @agent123",
            raw_images=_images(4),
            meta={"chat_id": "-1001"},
        )
    )

    assert result.status == "inserted"
    assert result.media_count == 4
    row = repository.get_source_post(result.source_post_pk)
    assert row is not None
    assert row["parse_status"] == "pending"
    assert "联系微信" in row["raw_text"]
    assert "@agent123" in row["raw_contact"]
    meta = json.loads(row["raw_meta_json"])
    assert meta["sanitized_text"] == "富力城 2房1厅 租金 $800/月"
    assert meta["source_contact_removed"] is True


def test_intake_duplicate_returns_existing_source_post(tmp_path):
    db = tmp_path / "v3.sqlite3"
    initialize_v3_storage(db)
    repository = SourceRepository(str(db))
    service = IntakeService(repository, min_listing_images=4)
    source = SourceIntake(
        source_type="telegram_channel",
        source_name="collector-a",
        source_post_id="123",
        source_url="https://t.me/c/1/123",
        raw_text="富力城 租金 $800/月",
        raw_images=_images(4),
    )

    first = service.persist(source)
    second = service.persist(source)
    assert first.status == "inserted"
    assert second.status == "duplicate"
    assert second.source_post_pk == first.source_post_pk
    assert second.reason == "source_tuple"


def test_insufficient_media_is_preserved_but_not_parse_pending(tmp_path):
    db = tmp_path / "v3.sqlite3"
    initialize_v3_storage(db)
    repository = SourceRepository(str(db))
    service = IntakeService(repository, min_listing_images=4)

    result = service.persist(
        SourceIntake(
            source_type="telegram_channel",
            source_name="collector-a",
            source_post_id="124",
            source_url="https://t.me/c/1/124",
            raw_text="BKK1 公寓 $600/月",
            raw_images=_images(2),
        )
    )

    assert result.status == "insufficient_media"
    row = repository.get_source_post(result.source_post_pk)
    assert row is not None
    assert row["parse_status"] == "insufficient_media"
    assert len(json.loads(row["raw_images_json"])) == 2

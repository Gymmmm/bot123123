from pathlib import Path

from PIL import Image, ImageDraw

from v3_core.ingest.intake_service import SourceIntake
from v3_core.parser.worker import CanonicalWorker
from v3_core.pipeline import V3CorePipeline


def _photos(tmp_path: Path):
    result = []
    for index in range(4):
        path = tmp_path / f"worker-{index}.jpg"
        image = Image.new("RGB", (960, 720), (150, 135 + index * 5, 115))
        draw = ImageDraw.Draw(image)
        draw.rectangle((80, 70, 880, 650), outline=(245, 245, 238), width=12)
        draw.rectangle((140, 140, 500, 450), fill=(175, 205, 225), outline=(50, 70, 90), width=8)
        draw.line((90, 580, 870, 580), fill=(55, 55, 55), width=10)
        draw.line((180 + index * 15, 110, 690, 540 - index * 10), fill=(110, 75, 45), width=7)
        image.save(path, "JPEG", quality=94)
        result.append({"local_path": str(path), "file_hash": f"worker-{index}"})
    return result


def test_collector_intake_remains_pending_until_independent_worker_runs(tmp_path):
    pipeline = V3CorePipeline(
        db_path=str(tmp_path / "worker.sqlite3"),
        user_bot_username="QiaolianBot",
    )
    intake = pipeline.ingest_source(
        SourceIntake(
            source_type="telegram_channel",
            source_name="collector",
            source_post_id="700",
            source_url="https://t.me/c/1/700",
            raw_text="富力城 公寓 2房1厅 月租 $900/月",
            raw_images=_photos(tmp_path),
        )
    )
    assert intake.status == "inserted"
    source = pipeline.sources.get_source_post(int(intake.source_post_pk))
    assert source["parse_status"] == "pending"

    worker = CanonicalWorker(pipeline)
    assert worker.pending_ids() == [int(intake.source_post_pk)]
    result = worker.process_one(int(intake.source_post_pk))

    assert result.status == "materialized"
    assert result.listing_id == "l_1"
    listing = pipeline.inventory_reader.listing(result.listing_id)
    assert listing["public_listing_id"].startswith("QL-RF-")
    source = pipeline.sources.get_source_post(int(intake.source_post_pk))
    assert source["parse_status"] == "parsed"

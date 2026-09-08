from pathlib import Path

import pytest
from PIL import Image

from v3_core.ingest.intake_service import SourceIntake
from v3_core.pipeline import V3CorePipeline


def _images(tmp_path: Path, prefix: str):
    out = []
    for index, value in enumerate((45, 85, 125, 165), start=1):
        path = tmp_path / f"{prefix}-{index}.jpg"
        Image.new("RGB", (800, 600), (value, value, value)).save(path, "JPEG")
        out.append(
            {
                "local_path": str(path),
                "file_hash": f"{prefix}-hash-{index}",
                "telegram_file_id": str(index),
                "telegram_file_unique_id": f"u{index}",
            }
        )
    return out


def test_rent_source_reaches_approved_frozen_send_command(tmp_path):
    pipeline = V3CorePipeline(
        db_path=str(tmp_path / "v3.sqlite3"),
        user_bot_username="QiaolianBot",
    )
    intake = pipeline.ingest_source(
        SourceIntake(
            source_type="telegram_channel",
            source_name="collector",
            source_post_id="501",
            source_url="https://t.me/c/1/501",
            raw_text="富力城 公寓 2房1厅 95㎡ 19楼 月租 $800/月 押1付1 租期1年",
            raw_images=_images(tmp_path, "rent"),
        )
    )
    assert intake.status == "inserted"

    parsed, materialized = pipeline.parse_and_materialize(
        source_post_id=int(intake.source_post_pk),
        listing_id="l_501",
        public_listing_id="QL-RF-A2B3",
    )
    assert parsed.facts["deal_type"] == "rent"
    assert len(materialized.offer_ids) == 1
    assert len(materialized.review_ids) == 1

    pipeline.approve_review(
        review_id=materialized.review_ids[0],
        operator_user_id="admin",
    )
    media = pipeline.prepare_media(source_post_id=int(intake.source_post_pk))
    package = pipeline.build_package(
        listing_id=materialized.listing_id,
        offer_id=materialized.offer_ids[0],
        cover_style="classic_blue",
        rendered_cover_path=media.cover_source_path,
        media=media,
    )
    approved = pipeline.approve_package(
        package_id=package.package_id,
        approved_by="admin",
    )
    assert approved.status == "approved"

    command = pipeline.prepare_send(
        package_id=approved.package_id,
        channel_chat_id="-100123",
    )
    assert command.cover_path == approved.cover_path
    assert tuple(command.actions) == ("details", "photos", "book")
    assert "QL-RF-A2B3" in command.caption


def test_sale_source_is_saved_but_cannot_build_rental_package(tmp_path):
    pipeline = V3CorePipeline(
        db_path=str(tmp_path / "sale.sqlite3"),
        user_bot_username="QiaolianBot",
    )
    intake = pipeline.ingest_source(
        SourceIntake(
            source_type="telegram_channel",
            source_name="collector",
            source_post_id="601",
            source_url="https://t.me/c/1/601",
            raw_text="BKK1 公寓 2房 出售 售价 $100,000",
            raw_images=_images(tmp_path, "sale"),
        )
    )
    parsed, materialized = pipeline.parse_and_materialize(
        source_post_id=int(intake.source_post_pk),
        listing_id="l_601",
        public_listing_id="QL-BK-C4D5",
    )
    assert parsed.facts["deal_type"] == "sale"
    assert len(materialized.offer_ids) == 1
    pipeline.approve_review(
        review_id=materialized.review_ids[0],
        operator_user_id="admin",
    )
    media = pipeline.prepare_media(source_post_id=int(intake.source_post_pk))

    with pytest.raises(ValueError, match="rent"):
        pipeline.build_package(
            listing_id=materialized.listing_id,
            offer_id=materialized.offer_ids[0],
            cover_style="classic_blue",
            rendered_cover_path=media.cover_source_path,
            media=media,
        )

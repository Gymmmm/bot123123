from pathlib import Path

import pytest
from PIL import Image, ImageDraw

from v3_core.ingest.intake_service import SourceIntake
from v3_core.pipeline import V3CorePipeline
from v3_core.storage.bootstrap import initialize_v3_storage


def _images(tmp_path: Path, prefix: str):
    out = []
    for index, value in enumerate((45, 85, 125, 165), start=1):
        path = tmp_path / f"{prefix}-{index}.jpg"
        image = Image.new("RGB", (960, 720), (value + 45, value + 35, value + 25))
        draw = ImageDraw.Draw(image)
        draw.rectangle((55, 55, 905, 665), outline=(238, 238, 230), width=10)
        draw.rectangle((120, 135, 480, 430), fill=(185, 205, 220), outline=(60, 75, 90), width=8)
        draw.rectangle((545, 150, 835, 510), fill=(215, 190, 150), outline=(80, 65, 45), width=8)
        draw.line((80, 590, 880, 590), fill=(65, 65, 65), width=12)
        draw.line((180, 590, 260, 470), fill=(95, 75, 55), width=9)
        draw.line((760, 590, 680, 470), fill=(95, 75, 55), width=9)
        for x in range(110, 880, 80):
            draw.line((x, 85, x + 35, 115), fill=(110 + value % 60, 90, 80), width=4)
        image.save(path, "JPEG", quality=94)
        out.append(
            {
                "local_path": str(path),
                "file_hash": f"{prefix}-hash-{index}",
                "telegram_file_id": str(index),
                "telegram_file_unique_id": f"u{index}",
            }
        )
    return out


def _pipeline(tmp_path: Path, name: str) -> V3CorePipeline:
    db_path = tmp_path / name
    initialize_v3_storage(db_path)
    return V3CorePipeline(
        db_path=str(db_path),
        user_bot_username="QiaolianBot",
    )


def test_rent_source_reaches_approved_frozen_send_command(tmp_path):
    pipeline = _pipeline(tmp_path, "v3.sqlite3")
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
    pipeline = _pipeline(tmp_path, "sale.sqlite3")
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

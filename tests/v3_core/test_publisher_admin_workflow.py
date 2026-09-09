from pathlib import Path

import pytest
from PIL import Image, ImageDraw

from v3_core.ingest.source_reader import SourceReader
from v3_core.ingest.source_repository import SourceRepository
from v3_core.inventory.service import InventoryMaterializationService
from v3_core.media.cover_service import CoverRenderService
from v3_core.media.service import MediaPreparationService
from v3_core.publishing.admin_workflow import PublisherWorkflowService
from v3_core.publishing.delivery_coordinator import PublicationDeliveryCoordinator
from v3_core.publishing.delivery_state import PublicationDeliveryStateRepository
from v3_core.publishing.package_service import PackageApprovalService, PackageBuildService
from v3_core.publishing.package_store import FrozenPackageStore
from v3_core.publishing.publication_instances import PublicationInstanceRepository
from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.storage.inventory_reader import InventoryReader
from v3_core.storage.inventory_repository import InventoryRepository


def _facts(deal_type="rent"):
    rent = deal_type == "rent"
    return {
        "schema_version": "canonical_facts.v1",
        "parser_revision": "v1.2",
        "canonical_facts_hash": f"workflow-{deal_type}",
        "deal_type": deal_type,
        "project_name": "富力城" if rent else "BKK1 公寓",
        "project_alias": "",
        "project_brand": "",
        "project_key": "rf_city" if rent else "bkk1",
        "property_type": "公寓",
        "property_subtype": "",
        "public_location_key": "rf_city" if rent else "bkk1",
        "public_location_display": "富力城" if rent else "BKK1",
        "publication_location_level": "level_1_project_confirmed",
        "canonical_area_key": "",
        "canonical_area_display": "",
        "layout": "2房1厅",
        "bedrooms": 2,
        "bathrooms": 1,
        "size_sqm": 95,
        "floor": "19",
        "display_title": "富力城 2房1厅" if rent else "BKK1 公寓 2房",
        "monthly_rent_usd": 800 if rent else None,
        "sale_price_usd": None if rent else 100000,
        "deposit_payment_terms": "押1付1" if rent else "",
        "contract_term_display": "1年" if rent else "",
        "available_date": "",
        "highlights": ["采光好", "客厅方正"],
        "quality": {"score": 100, "all_flags": [], "blocking_flags": []},
    }


def _draw_distinct_room(path: Path, index: int) -> None:
    """Create sharp, visibly different 4:3 room-like fixtures for the real ranker."""
    palettes = (
        ((216, 202, 180), (170, 208, 232), (62, 73, 84)),
        ((196, 214, 190), (238, 205, 158), (70, 82, 58)),
        ((220, 194, 194), (184, 214, 204), (86, 58, 64)),
        ((194, 205, 226), (232, 222, 168), (54, 68, 92)),
        ((226, 213, 188), (194, 190, 230), (76, 64, 52)),
        ((202, 222, 218), (235, 190, 176), (55, 82, 78)),
    )
    wall, window, dark = palettes[index % len(palettes)]
    image = Image.new("RGB", (960, 720), wall)
    draw = ImageDraw.Draw(image)

    # Every fixture has a different large-scale luminance geometry so dHash
    # cannot collapse the set as near duplicates.
    if index == 0:
        draw.rectangle((70, 70, 430, 420), fill=window, outline=dark, width=10)
        draw.rectangle((555, 330, 900, 610), fill=(128, 96, 70), outline=dark, width=10)
        draw.line((0, 585, 960, 585), fill=dark, width=12)
    elif index == 1:
        draw.rectangle((510, 65, 900, 380), fill=window, outline=dark, width=10)
        draw.polygon(((80, 610), (350, 300), (540, 610)), fill=(148, 112, 82), outline=dark)
        for x in range(90, 500, 80):
            draw.line((x, 620, x + 240, 330), fill=dark, width=6)
    elif index == 2:
        draw.rectangle((85, 95, 875, 215), fill=window, outline=dark, width=10)
        draw.rectangle((110, 365, 500, 625), fill=(150, 118, 88), outline=dark, width=10)
        draw.rectangle((610, 295, 860, 625), fill=(112, 124, 110), outline=dark, width=10)
        draw.line((480, 230, 480, 700), fill=dark, width=9)
    elif index == 3:
        for x in range(60, 900, 150):
            draw.rectangle((x, 80, x + 88, 520), fill=window if (x // 150) % 2 else (235, 235, 225), outline=dark, width=7)
        draw.rectangle((180, 555, 800, 665), fill=(132, 102, 78), outline=dark, width=9)
    elif index == 4:
        draw.ellipse((90, 90, 430, 430), fill=window, outline=dark, width=11)
        draw.rectangle((520, 100, 885, 265), fill=(236, 232, 215), outline=dark, width=9)
        draw.polygon(((520, 650), (690, 300), (900, 650)), fill=(146, 112, 82), outline=dark)
        draw.line((45, 520, 520, 690), fill=dark, width=9)
    else:
        draw.rectangle((70, 80, 300, 630), fill=(235, 235, 225), outline=dark, width=9)
        draw.rectangle((360, 90, 890, 330), fill=window, outline=dark, width=10)
        for y in range(390, 660, 55):
            draw.line((330, y, 900, y - 90), fill=dark, width=7)
        draw.rectangle((90, 500, 275, 650), fill=(142, 108, 80), outline=dark, width=8)

    image.save(path, "JPEG", quality=95)


def _images(tmp_path: Path, prefix: str):
    result = []
    # Use six intentionally distinct source images. Production still requires
    # at least four usable images after exact/near dedupe and severe rejects.
    for index in range(6):
        path = tmp_path / f"{prefix}-{index}.jpg"
        _draw_distinct_room(path, index)
        result.append(
            {
                "local_path": str(path),
                "file_hash": f"{prefix}-{index}",
                "telegram_file_id": f"f{index}",
                "telegram_file_unique_id": f"u{index}",
            }
        )
    return result


def _workflow(tmp_path: Path, *, deal_type="rent"):
    db = tmp_path / f"publisher-{deal_type}.sqlite3"
    initialize_v3_storage(db)
    sources = SourceRepository(str(db))
    inventory = InventoryRepository(str(db))
    reader = InventoryReader(str(db))

    images = _images(tmp_path, deal_type)
    source_post_id = sources.save_source_post(
        source_id=None,
        source_type="telegram_channel",
        source_name="collector",
        source_post_id=f"source-{deal_type}",
        source_url="https://t.me/c/1/1",
        source_author="channel",
        raw_text="test",
        raw_images=images,
        raw_videos=[],
        raw_contact="",
        raw_meta={},
        dedupe_hash=f"hash-{deal_type}",
    )
    sources.save_source_images(source_post_id, images)

    facts = _facts(deal_type)
    canonical = inventory.store_canonical(source_post_id=source_post_id, facts=facts)
    listing_id = "l_1" if deal_type == "rent" else "l_2"
    public_id = "QL-RF-A2B3" if deal_type == "rent" else "QL-BK-C4D5"
    materialized = InventoryMaterializationService(inventory).materialize(
        canonical_record_id=str(canonical["canonical_record_id"]),
        listing_id=listing_id,
        public_listing_id=public_id,
        create_review=True,
    )
    offer_id = materialized.offer_ids[0]
    review_id = materialized.review_ids[0]

    def fake_renderer(**kwargs):
        Path(kwargs["output_path"]).write_bytes(b"final-cover")
        return kwargs["output_path"]

    media = MediaPreparationService(SourceReader(str(db)))
    covers = CoverRenderService(
        reader=reader,
        output_dir=tmp_path / "covers",
        renderer=fake_renderer,
    )
    packages = FrozenPackageStore(str(db))
    package_builder = PackageBuildService(
        reader=reader,
        store=packages,
        user_bot_username="QiaolianBot",
    )
    package_approver = PackageApprovalService(
        reader=reader,
        inventory_repository=inventory,
        store=packages,
    )
    deliveries = PublicationDeliveryStateRepository(str(db))
    publications = PublicationInstanceRepository(str(db))
    delivery = PublicationDeliveryCoordinator(
        reader=reader,
        packages=packages,
        deliveries=deliveries,
        publications=publications,
    )
    workflow = PublisherWorkflowService(
        reader=reader,
        inventory=inventory,
        media=media,
        covers=covers,
        packages=packages,
        package_builder=package_builder,
        package_approver=package_approver,
        delivery=delivery,
    )
    return workflow, inventory, reader, review_id, offer_id


def test_review_must_be_approved_before_package_build(tmp_path):
    workflow, _, _, review_id, _ = _workflow(tmp_path)
    assert [row["review_id"] for row in workflow.pending_reviews()] == [review_id]

    with pytest.raises(ValueError, match="review_must_be_approved"):
        workflow.build_package_for_review(review_id=review_id)

    approved = workflow.approve_review(review_id=review_id, operator_user_id="admin")
    assert approved.review["review_status"] == "approved"
    package = workflow.build_package_for_review(
        review_id=review_id,
        cover_style="black_gold",
    )
    assert package.status == "package_ready"
    assert package.cover_style == "black_gold"
    assert tuple(package.actions) == ("details", "photos", "book")
    assert len(package.gallery) >= 4


def test_package_requires_second_approval_before_delivery(tmp_path):
    workflow, _, reader, review_id, offer_id = _workflow(tmp_path)
    workflow.approve_review(review_id=review_id, operator_user_id="admin")
    package = workflow.build_package_for_review(review_id=review_id)

    with pytest.raises(Exception):
        workflow.prepare_send(package_id=package.package_id, channel_chat_id="-100123")

    frozen = workflow.approve_package(package_id=package.package_id, approved_by="admin")
    assert frozen.status == "approved"
    assert int(reader.offer(offer_id)["publishable"]) == 1
    command = workflow.prepare_send(
        package_id=package.package_id,
        channel_chat_id="-100123",
    )
    assert command.package_id == package.package_id
    assert command.cover_path == package.cover_path


def test_sale_review_is_stored_and_approvable_but_never_builds_rental_package(tmp_path):
    workflow, _, _, review_id, _ = _workflow(tmp_path, deal_type="sale")
    detail = workflow.approve_review(review_id=review_id, operator_user_id="admin")
    assert detail.review["review_status"] == "approved"
    assert detail.offer["offer_type"] == "sale"

    with pytest.raises(ValueError, match="publisher_only_builds_rent_packages"):
        workflow.build_package_for_review(review_id=review_id)

from pathlib import Path

from PIL import Image, ImageDraw

from v3_core.media.cover_service import CoverRenderService
from v3_core.media.service import PreparedSourceMedia
from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.storage.inventory_reader import InventoryReader
from v3_core.storage.inventory_repository import InventoryRepository


def _facts():
    return {
        "schema_version": "canonical_facts.v1",
        "parser_revision": "v1.2",
        "canonical_facts_hash": "cover-contract-hash",
        "deal_type": "rent",
        "project_name": "富力城",
        "project_alias": "R&F City",
        "project_brand": "",
        "project_key": "rf_city",
        "property_type": "公寓",
        "property_subtype": "",
        "public_location_key": "rf_city",
        "public_location_display": "富力城",
        "publication_location_level": "level_1_project_confirmed",
        "canonical_area_key": "",
        "canonical_area_display": "",
        "layout": "2房1厅",
        "bedrooms": 2,
        "bathrooms": 1,
        "size_sqm": 95,
        "floor": "19",
        "display_title": "富力城 2房1厅",
        "monthly_rent_usd": 800,
        "sale_price_usd": None,
        "deposit_payment_terms": "押1付1",
        "contract_term_display": "1年",
        "available_date": "",
        "highlights": ["采光好", "客厅方正", "实拍房源"],
        "quality": {"score": 100, "all_flags": [], "blocking_flags": []},
    }


def _source_image(path: Path) -> str:
    image = Image.new("RGB", (960, 720), (155, 138, 118))
    draw = ImageDraw.Draw(image)
    draw.rectangle((80, 60, 880, 650), outline=(245, 245, 238), width=12)
    draw.rectangle((140, 130, 520, 450), fill=(175, 205, 225), outline=(50, 70, 90), width=8)
    draw.line((90, 580, 870, 580), fill=(55, 55, 55), width=10)
    image.save(path, "JPEG", quality=94)
    return str(path)


def test_cover_service_passes_only_resolved_v3_identity_and_facts(tmp_path):
    db = tmp_path / "cover.sqlite3"
    initialize_v3_storage(db)
    repo = InventoryRepository(str(db))
    facts = _facts()
    canonical = repo.store_canonical(source_post_id="source-1", facts=facts)
    repo.upsert_listing(
        listing_id="l_10",
        public_listing_id="QL-RF-A2B3",
        canonical_record_id=str(canonical["canonical_record_id"]),
        facts=facts,
    )
    offer = repo.sync_offers(listing_id="l_10", facts=facts)[0]
    source = _source_image(tmp_path / "source.jpg")
    media = PreparedSourceMedia(
        source_post_id=1,
        cover_source_path=source,
        gallery_paths=(source,),
        source_identity={"source_post_db_id": 1},
        duplicates=(),
        rejected_paths=(),
        ranking=(),
    )

    calls = []

    def fake_renderer(**kwargs):
        calls.append(kwargs)
        Path(kwargs["output_path"]).write_bytes(b"rendered-cover")
        return kwargs["output_path"]

    service = CoverRenderService(
        reader=InventoryReader(str(db)),
        output_dir=tmp_path / "covers",
        renderer=fake_renderer,
    )
    result = service.render(
        listing_id="l_10",
        offer_id=str(offer["offer_id"]),
        media=media,
        style="right_price_fixed",
    )

    assert result.style == "right_price"
    assert Path(result.output_path).is_file()
    assert len(calls) == 1
    call = calls[0]
    assert call["style"] == "right_price"
    assert call["source_image"] == str(Path(source).resolve())
    assert call["data"].public_listing_id == "QL-RF-A2B3"
    assert call["data"].project == "富力城"
    assert call["data"].project_alias == "R&F City"
    assert call["data"].layout == "2房1厅"
    assert call["data"].area == "富力城"
    assert call["data"].size == "95.0" or call["data"].size == "95"
    assert call["data"].floor == "19"
    assert call["data"].price in {"800", "800.0"}
    assert (call["data"].highlight_1, call["data"].highlight_2, call["data"].highlight_3) == (
        "",
        "",
        "",
    )


def test_cover_service_rejects_offer_from_another_listing(tmp_path):
    db = tmp_path / "cover-mismatch.sqlite3"
    initialize_v3_storage(db)
    repo = InventoryRepository(str(db))
    facts = _facts()
    canonical = repo.store_canonical(source_post_id="source-1", facts=facts)
    for listing_id, public_id in (("l_1", "QL-RF-A111"), ("l_2", "QL-RF-B222")):
        repo.upsert_listing(
            listing_id=listing_id,
            public_listing_id=public_id,
            canonical_record_id=str(canonical["canonical_record_id"]),
            facts=facts,
        )
    offer = repo.sync_offers(listing_id="l_2", facts=facts)[0]
    source = _source_image(tmp_path / "source.jpg")
    media = PreparedSourceMedia(
        source_post_id=1,
        cover_source_path=source,
        gallery_paths=(source,),
        source_identity={},
        duplicates=(),
        rejected_paths=(),
        ranking=(),
    )
    service = CoverRenderService(
        reader=InventoryReader(str(db)),
        output_dir=tmp_path / "covers",
        renderer=lambda **kwargs: kwargs["output_path"],
    )

    import pytest

    with pytest.raises(ValueError, match="cover_offer_listing_mismatch"):
        service.render(
            listing_id="l_1",
            offer_id=str(offer["offer_id"]),
            media=media,
        )

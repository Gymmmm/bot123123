import json

import pytest

from v3_core.inventory.materializer_v3 import offer_projections_v3
from v3_core.publishing.eligibility import evaluate_offer_eligibility
from v3_core.storage.inventory_repository import InventoryRepository


def _facts(**overrides):
    facts = {
        "schema_version": "canonical_facts.v1",
        "parser_revision": "v1.2",
        "canonical_facts_hash": "hash-1",
        "deal_type": "rent",
        "project_name": "富力城",
        "project_alias": "",
        "project_brand": "",
        "property_type": "公寓",
        "property_subtype": "",
        "public_location_key": "富力城",
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
        "quality": {"score": 100, "all_flags": [], "blocking_flags": []},
    }
    facts.update(overrides)
    return facts


def test_mixed_source_creates_two_offers_not_mixed_listing():
    offers = offer_projections_v3(
        _facts(deal_type="mixed", monthly_rent_usd=800, sale_price_usd=160000)
    )
    assert [offer["offer_type"] for offer in offers] == ["rent", "sale"]
    assert offers[0]["publication_policy"] == "telegram_rent"
    assert offers[1]["publication_policy"] == "store_only"
    assert offers[1]["publish_block_reason"] == "sale_not_enabled_for_telegram"


def test_repository_stores_canonical_listing_and_separate_offers(tmp_path):
    db = tmp_path / "v3.sqlite3"
    repo = InventoryRepository(str(db))
    facts = _facts(deal_type="mixed", sale_price_usd=160000)

    canonical = repo.store_canonical(source_post_id=123, facts=facts)
    listing = repo.upsert_listing(
        listing_id="l_123",
        public_listing_id="QL-RF-A2B3",
        canonical_record_id=canonical["canonical_record_id"],
        facts=facts,
    )
    offers = repo.sync_offers(listing_id="l_123", facts=facts)

    assert listing["project_name"] == "富力城"
    assert listing["canonical_facts_hash"] == "hash-1"
    assert {row["offer_type"] for row in offers} == {"rent", "sale"}
    sale = next(row for row in offers if row["offer_type"] == "sale")
    assert sale["sale_price_usd"] == 160000
    assert sale["publishable"] == 0

    saved = repo.canonical_facts(canonical["canonical_record_id"])
    assert saved["monthly_rent_usd"] == 800
    assert saved["sale_price_usd"] == 160000


def test_sale_offer_can_never_be_marked_publishable(tmp_path):
    db = tmp_path / "v3.sqlite3"
    repo = InventoryRepository(str(db))
    facts = _facts(monthly_rent_usd=None, sale_price_usd=160000, deal_type="sale")
    canonical = repo.store_canonical(source_post_id=456, facts=facts)
    repo.upsert_listing(
        listing_id="l_456",
        public_listing_id="QL-PP-C4D5",
        canonical_record_id=canonical["canonical_record_id"],
        facts=facts,
    )
    sale = repo.sync_offers(listing_id="l_456", facts=facts)[0]

    with pytest.raises(ValueError, match="only telegram_rent"):
        repo.set_offer_publication(offer_id=sale["offer_id"], publishable=True)


def test_offer_eligibility_is_rent_only_and_review_aware():
    facts = _facts(deal_type="mixed", sale_price_usd=160000)
    rent, sale = offer_projections_v3(facts)

    not_reviewed = evaluate_offer_eligibility(
        facts=facts,
        offer=rent,
        review_approved=False,
        media_count=4,
        cover_exists=True,
    )
    assert not not_reviewed.ok
    assert "review_required" in not_reviewed.blocking

    approved_rent = evaluate_offer_eligibility(
        facts=facts,
        offer=rent,
        review_approved=True,
        media_count=4,
        cover_exists=True,
    )
    assert approved_rent.ok

    approved_sale = evaluate_offer_eligibility(
        facts=facts,
        offer=sale,
        review_approved=True,
        media_count=4,
        cover_exists=True,
    )
    assert not approved_sale.ok
    assert "offer_type_not_rent" in approved_sale.blocking

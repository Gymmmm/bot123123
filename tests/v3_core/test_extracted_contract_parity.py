from qiaolian_dual.cover_styles import normalize_cover_style as legacy_normalize_cover_style
from qiaolian_dual.publishability_contract import evaluate_publishability as legacy_evaluate_publishability
from v3_core.inventory.publishability import evaluate_publishability
from v3_core.media.cover_styles import normalize_cover_style


def test_publishability_extraction_matches_locked_production_contract():
    facts = {
        "schema_version": "canonical_facts.v1",
        "deal_type": "rent",
        "publication_location_level": "level_1_project_confirmed",
        "public_location_display": "富力城",
        "public_location_key": "富力城",
        "property_type": "公寓",
        "layout": "2房1厅",
        "monthly_rent_usd": 800,
        "canonical_facts_hash": "locked-sha-test",
        "deposit_payment_terms": "押1付1",
        "contract_term_display": "1年",
        "size_sqm": 95,
        "highlights": ["测试"],
        "quality": {"blocking_flags": []},
    }
    assert evaluate_publishability(facts, media_count=4, cover_exists=True) == legacy_evaluate_publishability(
        facts, media_count=4, cover_exists=True
    )


def test_cover_style_extraction_matches_locked_production_contract():
    for style in (None, "classic", "blue_banner", "right_price", "villa_premium", "dark_glass", "unknown"):
        assert normalize_cover_style(style) == legacy_normalize_cover_style(style)

from qiaolian_dual.canonical_facts import canonicalize_source
from qiaolian_dual.v3_shadow_store import normalize_v3_facts


def _facts(text: str):
    return normalize_v3_facts(canonicalize_source(text, media_summary={"image_count": 4, "media_type": "image"}))


def test_rent_sale_mixed_are_preserved_as_real_estate_facts():
    assert _facts("BKK1 公寓 1房1卫 月租 $800/月")['deal_type'] == 'rent'
    sale = _facts("BKK1 公寓 1房1卫 售价 $160,000")
    assert sale['deal_type'] == 'sale'
    assert 'non_rental_source' not in sale['quality']['hard_flags']
    mixed = _facts("BKK1 公寓 1房1卫 月租 $800/月，售价 $160,000")
    assert mixed['deal_type'] == 'mixed'

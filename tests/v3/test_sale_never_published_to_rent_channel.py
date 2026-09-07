from qiaolian_dual.canonical_facts import canonicalize_source
from qiaolian_dual.publishability_contract import evaluate_publishability
from qiaolian_dual.v3_shadow_store import normalize_v3_facts, shadow_write_v3
from tests.v3._helpers import make_conn


def test_sale_is_stored_but_never_publishable_to_rent_channel():
    conn = make_conn()
    conn.execute("INSERT INTO source_posts (id,source_type,source_name,source_post_id,raw_text) VALUES (1,'telegram_channel','x','1','sale')")
    facts = normalize_v3_facts(canonicalize_source('BKK1 公寓 1房1卫 售价 $160,000', media_summary={'image_count': 4, 'media_type': 'image'}))
    result = shadow_write_v3(conn, source_post_id=1, facts=facts)
    assert result['status'] == 'written'
    offer = conn.execute("SELECT offer_type,publishable,publish_block_reason FROM listing_offers").fetchone()
    assert offer == ('sale', 0, 'sale_not_enabled_for_rent_channel')
    gate = evaluate_publishability(facts, media_count=4, cover_exists=True)
    assert gate['ok'] is False
    assert 'deal_type_not_rent' in gate['blocking']

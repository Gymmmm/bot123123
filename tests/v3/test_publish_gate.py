from qiaolian_dual.canonical_facts import canonicalize_source
from qiaolian_dual.publishability_contract import evaluate_publishability
from qiaolian_dual.v3_shadow_store import normalize_v3_facts


def test_publish_gate_hard_blocks_non_rent():
    facts = normalize_v3_facts(canonicalize_source('BKK1 公寓 1房1卫 售价 $160,000'))
    result = evaluate_publishability(facts, media_count=10, cover_exists=True)
    assert result['ok'] is False
    assert 'deal_type_not_rent' in result['blocking']

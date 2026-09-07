from qiaolian_dual.canonical_facts import canonicalize_source
from qiaolian_dual.publishability_contract import evaluate_publishability
from qiaolian_dual.v3_shadow_store import normalize_v3_facts


def test_collector_origin_does_not_override_sale_gate():
    ingest_origin = 'collector'
    facts = normalize_v3_facts(canonicalize_source('BKK1 公寓 1房1卫 售价 $160,000'))
    gate = evaluate_publishability(facts, media_count=4, cover_exists=True)
    auto_publish_allowed = ingest_origin == 'collector' and gate['ok']
    assert auto_publish_allowed is False

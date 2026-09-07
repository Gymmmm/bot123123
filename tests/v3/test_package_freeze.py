from qiaolian_dual.canonical_facts import canonicalize_source
from qiaolian_dual.canonical_fact_projection import package_snapshot
from qiaolian_dual.v3_shadow_store import normalize_v3_facts


def test_package_snapshot_is_hash_bound_to_canonical_facts():
    facts = normalize_v3_facts(canonicalize_source('BKK1 公寓 1房1卫 月租 $800/月'))
    snapshot = package_snapshot(facts, listing_id='L1', media_hashes=['m1','m2'])
    assert snapshot['canonical_facts_hash'] == facts['canonical_facts_hash']
    assert snapshot['listing_id'] == 'L1'
    assert snapshot['media_asset_hashes'] == ['m1','m2']

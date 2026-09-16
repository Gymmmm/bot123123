from __future__ import annotations

from v3_core.adviser_copy import adviser_tags_from_facts, generate_adviser_text
from v3_core.inventory.canonical_facts import canonicalize_source
from v3_core.parser.authoritative_enrichment import enrich_authoritative_facts


def _parse(text: str):
    base = canonicalize_source(text)
    return enrich_authoritative_facts(text, base)


def test_scanner_extracts_explicit_adviser_signals_without_changing_core_facts():
    raw = (
        "BKK1 公寓出租\n2房1厅\n租金 $800/月\n面积95㎡ 19楼\n"
        "每周保洁两次，物业费包含，宽带已经装好，可养宠物，带阳台，河景"
    )
    facts = _parse(raw)

    assert facts["monthly_rent_usd"] == 800
    assert facts["layout"] == "2房1厅"
    assert set(facts["adviser_signals"]).issuperset(
        {"cleaning_2x", "management_included", "wifi_ready", "pet_allowed", "balcony", "river_view"}
    )


def test_admin_supplement_reparse_adds_adviser_signals():
    original = "BKK1 公寓出租\n2房1厅\n租金 $800/月\n面积95㎡"
    first = _parse(original)
    assert "cleaning_2x" not in first.get("adviser_signals", [])

    supplemented = original + "\n补充：每周保洁2次，物业费已含，网络费也包含。"
    second = _parse(supplemented)
    assert set(second["adviser_signals"]).issuperset(
        {"cleaning_2x", "management_included", "wifi_included"}
    )
    assert adviser_tags_from_facts(second)[0] == "cleaning_2x"
    assert "management_wifi" in adviser_tags_from_facts(second)


def test_negative_feature_wording_is_not_promoted():
    facts = _parse(
        "BKK1 公寓出租 2房1厅 租金$800/月 面积95㎡\n"
        "不包物业费，不包网络费，无阳台，不可养宠物。"
    )
    signals = set(facts.get("adviser_signals") or [])
    assert "management_included" not in signals
    assert "wifi_included" not in signals
    assert "balcony" not in signals
    assert "pet_allowed" not in signals


def test_adviser_copy_resolves_overlaps_and_is_deterministic():
    facts = {
        "adviser_signals": [
            "management_included",
            "wifi_included",
            "pool",
            "gym",
            "never_lived",
            "new_condition",
        ]
    }
    tags = adviser_tags_from_facts(facts)
    assert "management_wifi" in tags
    assert "management_included" not in tags
    assert "wifi_included" not in tags
    assert "pool_gym" in tags
    assert "pool" not in tags
    assert "gym" not in tags
    assert "never_lived" in tags
    assert "new_condition" not in tags

    first = generate_adviser_text(facts, seed="QL-RF-A2B3|hash", max_points=2)
    second = generate_adviser_text(facts, seed="QL-RF-A2B3|hash", max_points=2)
    assert first == second
    assert len(first.splitlines()) == 2


def test_adviser_copy_uses_neutral_fallback_when_no_signal_exists():
    text = generate_adviser_text({}, seed="empty", max_points=2, allow_fallback=True)
    assert text
    assert len(text.splitlines()) == 1

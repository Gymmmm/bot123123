from v3_core.adviser_copy import generate_adviser_text
from v3_core.publishing.package_service import _publisher_adviser_facts


def _copy(facts: dict) -> str:
    return generate_adviser_text(
        _publisher_adviser_facts(facts),
        seed="QL-POLICY-TEST",
        max_points=2,
        allow_fallback=False,
    )


def test_generic_view_floor_furniture_and_cost_never_create_auto_adviser_copy():
    facts = {
        "adviser_signals": ["river_view", "city_view", "high_floor", "furnished"],
        "house": {
            "features": ["河景", "市景", "阳台"],
            "furnished": True,
        },
        "amenities": ["泳池", "健身房"],
        "included": ["物业费", "网费"],
    }

    assert _copy(facts) == ""


def test_useful_uncommon_signals_can_still_create_auto_adviser_copy():
    facts = {
        "house": {
            "features": ["全新未入住", "河景"],
            "furnished": True,
            "pets": "允许",
        },
        "amenities": ["泳池", "私人泳池"],
        "included": ["物业费", "网费"],
    }

    copy = _copy(facts)
    assert copy
    assert "河景" not in copy
    assert "实际视野" not in copy
    assert "家具" not in copy
    assert "物业费" not in copy
    assert "网费" not in copy

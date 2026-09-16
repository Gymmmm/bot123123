from v3_core.adviser_copy import generate_adviser_text
from v3_core.publishing.package_service import _publisher_adviser_facts


def _copy(facts: dict) -> str:
    return generate_adviser_text(
        _publisher_adviser_facts(facts),
        seed="QL-POLICY-TEST",
        max_points=2,
        allow_fallback=False,
    )


def test_generic_single_fields_do_not_create_publisher_boilerplate():
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


def test_floor_and_river_view_become_a_useful_viewing_judgement():
    copy = _copy(
        {
            "floor": "48",
            "house": {"features": ["河景"]},
        }
    )
    assert copy == "48 楼这套又有河景信息，看房时重点看客厅视野、窗面和采光。"
    assert "以现场为准" not in copy
    assert "资料标注" not in copy


def test_plus_layout_and_view_can_produce_two_distinct_useful_points():
    copy = _copy(
        {
            "layout": "2+1",
            "floor": "48",
            "monthly_rent_usd": 1100,
            "house": {"features": ["河景"]},
        }
    )
    lines = copy.splitlines()
    assert lines == [
        "2+1 的布局多一个可用空间，做书房、储物或临时房会更灵活。",
        "48 楼这套又有河景信息，看房时重点看客厅视野、窗面和采光。",
    ]


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
    assert "以现场为准" not in copy
    assert "资料标注" not in copy
    assert "物业费" not in copy
    assert "网费" not in copy

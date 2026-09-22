from types import SimpleNamespace

import pytest

from v3_core.adviser_copy import (
    FORBIDDEN_ADVISER_PHRASES,
    build_adviser_copy,
    validate_adviser_copy,
)
from v3_core.publishing.package_service import _publisher_adviser_facts
from v3_core.publishing.publisher_adviser_ui import PublisherAdviserAdminController


def _copy(facts: dict, *, listing: dict | None = None) -> str:
    return build_adviser_copy(
        _publisher_adviser_facts(facts, listing=listing),
        seed="QL-POLICY-TEST",
        max_points=2,
    )


def test_generic_furniture_amenities_and_cost_do_not_create_auto_adviser_copy():
    facts = {
        "adviser_signals": ["furnished"],
        "house": {"features": ["阳台"], "furnished": True},
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
        "amenities": ["泳池", "私人泳池", "停车位"],
        "included": ["物业费", "网费"],
    }

    copy = _copy(facts)
    assert copy
    assert "河景" not in copy
    assert "实际视野" not in copy
    assert "家具" not in copy
    assert "物业费" not in copy
    assert "网费" not in copy
    validate_adviser_copy(copy)


def test_layout_plus_high_floor_river_view_creates_decision_copy_not_disclaimer():
    facts = {
        "layout": "2+1",
        "floor": "48",
        "included": ["物业费", "网费"],
        "amenities": ["停车位"],
        "house": {"features": ["河景"], "pets": "不允许"},
    }

    copy = _copy(facts)

    assert "2+1" in copy
    assert "48 楼" in copy
    assert "河景" in copy
    assert "书房" in copy
    assert "客厅视野" in copy
    assert "以现场为准" not in copy
    assert "资料标注" not in copy


def test_single_generic_view_still_does_not_create_copy():
    facts = {
        "house": {"features": ["河景"], "pets": "不允许"},
        "included": ["物业费", "网费"],
        "parking_fee": "另计",
        "internet_fee": "已含",
    }
    # Avoid incomplete-field reminders masking the empty-insight contract.
    assert _copy(facts) == ""


def test_public_location_is_not_turned_into_adviser_copy():
    """Alias/location tables feed parse — not 侨联说 prose."""
    copy = _copy(
        {
            "included": ["物业费", "网费"],
            "amenities": ["停车位"],
            "house": {"pets": "不允许"},
        },
        listing={"public_location_display": "BKK1", "layout": "1房1厅"},
    )
    assert "BKK1" not in copy
    assert "一带活动" not in copy
    assert "一个人" in copy or "一房" in copy
    validate_adviser_copy(copy)


def test_forbidden_adviser_phrases_are_rejected():
    with pytest.raises(ValueError, match="unsupported phrase"):
        validate_adviser_copy("这套房源性价比极高，不容错过")
    for phrase in sorted(FORBIDDEN_ADVISER_PHRASES)[:3]:
        with pytest.raises(ValueError):
            validate_adviser_copy(f"看看这套，{phrase}。")


def test_manual_labelled_two_plus_one_is_recovered_without_owner_reentry():
    controller = object.__new__(PublisherAdviserAdminController)
    edits = []

    class Workflow:
        def edit_review_field(self, **kwargs):
            edits.append(kwargs)

        def review_detail(self, review_id):
            return SimpleNamespace(
                review={"review_id": review_id},
                listing={"layout": "2+1"},
            )

    controller.workflow = Workflow()
    detail = SimpleNamespace(
        review={"review_id": "REV_TEST"},
        listing={"layout": ""},
    )
    state = {
        "text": "【香格里拉公寓出租】\n房型：2+1\n楼层：48（T1河景）\n租金：1100💵\n押金：押一付一\n合同：1年",
        "package_id": "OLD_PREVIEW",
        "mode": "preview",
    }

    repaired = controller._repair_manual_layout_if_needed(detail, state)

    assert repaired.listing["layout"] == "2+1"
    assert edits == [{
        "review_id": "REV_TEST",
        "field_name": "layout",
        "value": "2+1",
        "operator_user_id": "system:manual_layout_repair",
    }]
    assert "package_id" not in state
    assert state["mode"] == "confirm"


@pytest.mark.asyncio
async def test_manual_pending_inventory_is_not_a_publish_blocker(monkeypatch):
    controller = object.__new__(PublisherAdviserAdminController)

    async def fake_parent(self, detail):
        return ["listing_not_publishable", "missing_rent"], object()

    parent = PublisherAdviserAdminController.__mro__[1]
    monkeypatch.setattr(parent, "_manual_blockers", fake_parent)

    blockers, _media = await controller._manual_blockers(object())

    assert "listing_not_publishable" not in blockers
    assert blockers == ["missing_rent"]

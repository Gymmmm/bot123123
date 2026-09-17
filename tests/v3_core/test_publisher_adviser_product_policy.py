from types import SimpleNamespace

import pytest

from v3_core.adviser_copy import generate_adviser_text
from v3_core.publishing.package_service import _publisher_adviser_facts
from v3_core.publishing.publisher_adviser_ui import PublisherAdviserAdminController


def _copy(facts: dict) -> str:
    return generate_adviser_text(
        _publisher_adviser_facts(facts),
        seed="QL-POLICY-TEST",
        max_points=1,
        allow_fallback=False,
    )


def test_generic_furniture_and_common_amenities_do_not_create_auto_adviser_copy():
    facts = {
        "adviser_signals": ["furnished"],
        "house": {"features": ["阳台"], "furnished": True},
        "amenities": ["泳池", "健身房"],
    }

    assert _copy(facts) == ""


def test_verified_cost_inclusions_may_create_one_practical_viewing_focus():
    copy = _copy({"included": ["物业费", "网费"]})
    assert copy
    assert "物业费" in copy and "网费" in copy
    assert "\n" not in copy


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


def test_layout_plus_high_floor_river_view_creates_decision_copy_not_disclaimer():
    facts = {
        "layout": "2+1",
        "floor": "48",
        "house": {"features": ["河景"]},
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
    facts = {"house": {"features": ["河景"]}}
    assert _copy(facts) == ""


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

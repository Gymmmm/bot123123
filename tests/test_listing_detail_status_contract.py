from __future__ import annotations

from pathlib import Path

import pytest


STATUS_LINE = {
    "active": "<b>🟢 当前可预约</b>",
    "reserved": "<b>🟡 已有预约 · 仍可预约</b>",
    "pending": "<b>🔵 房态待确认</b>",
    "rented": "<b>🔴 已租出</b>",
    "inactive": "<b>⚫ 已下架</b>",
}

VISIT_HINT = "支持实地看房，也可以安排实时视频看房。"


def _item(status: str, **extra) -> dict:
    data = {
        "listing_id": "l_89",
        "project": "钻石岛",
        "layout": "2房2卫",
        "property_type": "公寓",
        "price": 680,
        "deposit": "押2付1",
        "status": status,
        "normalized_data": {"contract_term": "1年"},
    }
    data.update(extra)
    return data


def test_detail_runtime_patch_module_is_gone():
    root = Path("qiaolian_dual")
    assert not (root / "detail_runtime_patch.py").exists()
    user_bot = (root / "user_bot.py").read_text(encoding="utf-8")
    listing = (root / "listing.py").read_text(encoding="utf-8")
    assert "install_detail_runtime_patch" not in user_bot
    assert "detail_runtime_patch" not in user_bot
    assert "listing.listing_cost_text =" not in listing
    assert "想实地看看" not in listing
    assert "from .talk_engine import generate_talk" in listing
    assert "水电进入住服务" in listing


@pytest.mark.parametrize("status", ["active", "reserved", "pending", "rented", "inactive"])
def test_listing_cost_text_renders_each_canonical_status(monkeypatch, status):
    from qiaolian_dual import listing as listing_mod

    monkeypatch.setattr(listing_mod, "listing_context", lambda _lid: _item(status))
    text = listing_mod.listing_cost_text("l_89")
    assert STATUS_LINE[status] in text
    for other_status, line in STATUS_LINE.items():
        if other_status != status:
            assert line not in text
    assert text.count(STATUS_LINE[status]) == 1
    assert "QC0089" in text
    if status in {"active", "reserved"}:
        assert VISIT_HINT in text
    else:
        assert VISIT_HINT not in text


def test_listing_cost_text_includes_talk_when_facts_support_it(monkeypatch):
    from qiaolian_dual import listing as listing_mod

    monkeypatch.setattr(
        listing_mod,
        "listing_context",
        lambda _lid: _item("active", highlights=["可养宠物", "河景"]),
    )
    text = listing_mod.listing_cost_text("l_89")
    assert "💬 <b>侨联说</b>" in text
    assert STATUS_LINE["active"] in text
    assert "水费" not in text
    assert "电费" not in text


def test_listing_cost_text_omits_talk_when_no_distinctive_fact(monkeypatch):
    from qiaolian_dual import listing as listing_mod

    monkeypatch.setattr(listing_mod, "listing_context", lambda _lid: _item("reserved"))
    text = listing_mod.listing_cost_text("l_89")
    assert "💬 <b>侨联说</b>" not in text
    assert STATUS_LINE["reserved"] in text
    assert VISIT_HINT in text


def test_listing_is_available_matches_five_statuses(monkeypatch):
    from qiaolian_dual import listing as listing_mod

    def fake_get(listing_id: str):
        return {"listing_id": listing_id, "status": listing_id.split("_", 1)[1]}

    monkeypatch.setattr(listing_mod.db, "get_listing", fake_get)
    monkeypatch.setattr(listing_mod.db, "is_listing_public", lambda _lid: True)
    assert listing_mod.listing_is_available("x_active") == (True, "active")
    assert listing_mod.listing_is_available("x_reserved") == (True, "reserved")
    assert listing_mod.listing_is_available("x_pending") == (False, "pending")
    assert listing_mod.listing_is_available("x_rented") == (False, "rented")
    assert listing_mod.listing_is_available("x_inactive") == (False, "offline")


def test_five_appointment_lock_is_covered_without_manual_booking():
    from qiaolian_dual.channel_status_sync import (
        APPOINTMENT_LOCK_COUNT,
        _keyboard,
        _status_label,
    )

    assert APPOINTMENT_LOCK_COUNT == 5
    assert _status_label("pending", 5) == "🔵 已有5份预约看房，房态待确认"
    labels = [[button.text for button in row] for row in _keyboard("Bot", "", "l_89", "pending").inline_keyboard]
    assert labels == [["📋 租赁详情", "📸 更多实拍"]]
    bookable = [[button.text for button in row] for row in _keyboard("Bot", "", "l_89", "reserved").inline_keyboard]
    assert bookable == [["📋 租赁详情", "📸 更多实拍"], ["📅 预约看房"]]

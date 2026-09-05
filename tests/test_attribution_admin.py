from types import SimpleNamespace
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from qiaolian_dual.attribution import (
    admin_source_group_zh,
    classify_bot_event,
    classify_start_arg,
    merge_touch,
    public_deep_link_ok,
)
from qiaolian_dual.admin_consult import admin_home_keyboard, consult_action_keyboard
from qiaolian_dual import admin_consult


def test_public_deep_links_classify_without_changing_contract():
    expected = {
        "property_QC0001_details": "channel_listing_detail",
        "property_QC0001_photos": "channel_listing_photos",
        "property_QC0001_book": "channel_listing_book",
        "find_area": "channel_index_area",
        "find_budget": "channel_index_budget",
        "find_layout": "channel_index_layout",
        "latest": "channel_latest",
        "advisor": "channel_advisor",
    }
    for payload, source_type in expected.items():
        assert public_deep_link_ok(payload)
        got = classify_start_arg(payload)
        assert got["source_type"] == source_type


def test_first_touch_never_gets_overwritten_by_later_touch():
    first = merge_touch(
        None,
        classify_start_arg("property_QC0001_details"),
        user_id=1001,
        username="demo",
        display_name="张三",
        now="2026-09-05 09:00:00",
    )
    later = merge_touch(
        first,
        {"source_type": "bot_search", "source_detail": "user_search", "entry_action": "smart_search"},
        user_id=1001,
        username="demo",
        display_name="张三",
        now="2026-09-05 09:05:00",
    )
    assert later["first_source_type"] == "channel_listing_detail"
    assert later["first_entry_at"] == "2026-09-05 09:00:00"
    assert later["latest_source_type"] == "bot_search"
    assert later["latest_touch_at"] == "2026-09-05 09:05:00"


def test_legacy_discussion_entry_is_marked_but_grouped_as_listing_detail():
    got = classify_start_arg("discussion_entry__abc__l_1")
    assert got["legacy"] is True
    assert got["source_type"] == "channel_listing_detail"
    assert got["source_detail"] == "legacy_discussion_entry"


def test_admin_source_groups_are_chinese():
    assert admin_source_group_zh("channel_listing_detail") == "频道房源详情"
    assert admin_source_group_zh("bot_search") == "Bot 找房"
    assert admin_source_group_zh("bot_assurance") == "侨联保障"
    assert admin_source_group_zh("unknown") == "其他"


def test_admin_keyboards_are_mobile_safe_and_callbacks_short():
    keyboards = [
        admin_home_keyboard(),
        consult_action_keyboard(lead_id=8, appointment_id=9, user_id=1001, listing_id="l_1"),
    ]
    for keyboard in keyboards:
        for row in keyboard.inline_keyboard:
            assert len(row) <= 2
            for button in row:
                if button.callback_data:
                    assert len(button.callback_data.encode("utf-8")) <= 64
    actions = [
        b.callback_data
        for row in keyboards[1].inline_keyboard
        for b in row
        if b.callback_data
    ]
    assert "adminlead:claim:8:9:1001" in actions
    assert "adminlead:contacted:8:9:1001" in actions
    assert "adminlead:done:8:9:1001" in actions
    assert "adminlead:invalid:8:9:1001" in actions
    assert "adminq:view:8" in actions
    assert not any(str(action).startswith("adminlead:view:") for action in actions)
    assert [button.text for button in keyboards[1].inline_keyboard[0]] == ["✅ 我来跟进", "📞 已联系"]
    assert [button.text for button in keyboards[1].inline_keyboard[1]] == ["✅ 完成", "🚫 结束跟进"]


def test_listing_button_requires_real_listing_id():
    keyboard = consult_action_keyboard(lead_id=8, appointment_id=9, user_id=1001)
    actions = [button.callback_data for row in keyboard.inline_keyboard for button in row]
    assert not any(str(action).startswith("adminq:view:") for action in actions)


def test_latest_override_drops_old_deep_link_detail():
    incoming = classify_bot_event(
        action="smart_search",
        payload={"start_arg": "property_QC0001_details"},
    )
    merged = merge_touch(None, incoming, user_id=1001, now="2026-09-05 09:00:00")
    assert merged["latest_source_type"] == "bot_search"
    assert merged["latest_source_detail"] == "bot_search"
    assert merged["source_detail"] == "bot_search"


def test_done_runtime_returns_callback_admin_main():
    source = Path("qiaolian_dual/attribution_runtime.py").read_text(encoding="utf-8")
    assert "return callback_admin.MAIN" in source
    assert "if handled:\n                    return 0" not in source


@pytest.mark.asyncio
async def test_admin_view_edits_current_message_with_real_listing_summary(monkeypatch):
    monkeypatch.setattr(admin_consult, "_is_admin", lambda _user_id: True)
    monkeypatch.setattr(admin_consult.db, "get_lead", lambda lead_id: {"id": lead_id, "listing_id": "l_1"})
    monkeypatch.setattr(
        "qiaolian_dual.listing.listing_context",
        lambda listing_id: {"listing_id": listing_id, "project": "测试项目", "layout": "2房", "price": 800},
    )
    query = SimpleNamespace(data="adminq:view:8", answer=AsyncMock(), edit_message_text=AsyncMock())
    update = SimpleNamespace(callback_query=query, effective_user=SimpleNamespace(id=1))

    await admin_consult.handle_admin_query(update, SimpleNamespace())

    query.edit_message_text.assert_awaited_once()
    text = query.edit_message_text.await_args.args[0]
    assert "QC0001" in text
    assert "测试项目" in text
    assert "2房" in text
    assert "$800/月" in text


@pytest.mark.asyncio
async def test_admin_view_without_listing_reports_expired(monkeypatch):
    monkeypatch.setattr(admin_consult, "_is_admin", lambda _user_id: True)
    monkeypatch.setattr(admin_consult.db, "get_lead", lambda _lead_id: {"id": 8, "listing_id": ""})
    query = SimpleNamespace(data="adminq:view:8", answer=AsyncMock(), edit_message_text=AsyncMock())
    update = SimpleNamespace(callback_query=query, effective_user=SimpleNamespace(id=1))

    await admin_consult.handle_admin_query(update, SimpleNamespace())

    query.edit_message_text.assert_not_awaited()
    assert any(call.kwargs.get("text") == "房源信息已失效" for call in query.answer.await_args_list)


@pytest.mark.asyncio
async def test_admin_view_missing_listing_record_reports_expired(monkeypatch):
    monkeypatch.setattr(admin_consult, "_is_admin", lambda _user_id: True)
    monkeypatch.setattr(admin_consult.db, "get_lead", lambda _lead_id: {"id": 8, "listing_id": "l_404"})
    monkeypatch.setattr("qiaolian_dual.listing.listing_context", lambda listing_id: {"listing_id": listing_id, "caption_variant": "a"})
    query = SimpleNamespace(data="adminq:view:8", answer=AsyncMock(), edit_message_text=AsyncMock())
    update = SimpleNamespace(callback_query=query, effective_user=SimpleNamespace(id=1))

    await admin_consult.handle_admin_query(update, SimpleNamespace())

    query.edit_message_text.assert_not_awaited()
    assert any(call.kwargs.get("text") == "房源信息已失效" for call in query.answer.await_args_list)

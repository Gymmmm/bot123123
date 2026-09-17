from __future__ import annotations

from types import SimpleNamespace

import pytest
from telegram import ReplyKeyboardRemove

from v3_core.user_bot.app import admin_command_menu, public_command_menu, remove_legacy_reply_keyboard
from v3_core.user_bot.assurance_views import build_assurance_home_view
from v3_core.user_bot.legacy_routes import legacy_home_action
from v3_core.user_bot.listing_contact import build_listing_contact_view
from v3_core.user_bot.search_cards import build_search_card
from v3_core.user_bot.service_product_views import service_home_view
from v3_core.user_bot.service_views import property_view, repair_home_view, nearby_view
from v3_core.user_bot.telegram_listing_callback import _render_contact
from v3_core.user_bot.tenant_v1 import tenant_home_view


def _commands(items):
    return [(item.command, item.description) for item in items]


def _labels(view):
    return [choice.label for row in view.rows for choice in row]


def test_public_command_menu_hides_retired_and_admin_entries():
    commands = _commands(public_command_menu())
    assert commands == [
        ("start", "回到首页"),
        ("find", "开始找房"),
        ("appointments", "我的预约"),
        ("service", "入住服务"),
    ]
    assert not {"about", "contact", "help", "admin", "contracts"} & {name for name, _ in commands}


def test_admin_commands_are_scoped_extension_not_public_pollution():
    public = _commands(public_command_menu())
    admin = _commands(admin_command_menu())
    assert admin[: len(public)] == public
    assert admin[len(public):] == [("admin", "管理后台"), ("contracts", "租客与合同")]


@pytest.mark.asyncio
async def test_reply_keyboard_is_explicitly_removed_once():
    class Notice:
        def __init__(self): self.deleted = False
        async def delete(self): self.deleted = True
    class Message:
        def __init__(self): self.calls = []; self.notice = Notice()
        async def reply_text(self, text, **kwargs):
            self.calls.append((text, kwargs)); return self.notice
    message = Message()
    update = SimpleNamespace(effective_message=message)
    context = SimpleNamespace(user_data={})
    assert await remove_legacy_reply_keyboard(update, context) is True
    assert isinstance(message.calls[0][1]["reply_markup"], ReplyKeyboardRemove)
    assert message.notice.deleted
    assert await remove_legacy_reply_keyboard(update, context) is False
    assert len(message.calls) == 1


def test_legacy_navigation_maps_to_frozen_surfaces_without_reexposing_old_ui():
    assert legacy_home_action("hub:service") == "rental"
    assert legacy_home_action("hub:assurance") == "rental"
    assert legacy_home_action("home_brand") == "rental"
    assert legacy_home_action("menu_about") == "rental"
    assert legacy_home_action("hub:advisor") == "contact"
    assert legacy_home_action("appointment_menu:contact") == "contact"
    assert legacy_home_action("hub:precise") == "search"
    assert legacy_home_action("hub:favorites") == "search"
    assert legacy_home_action("hub:help") == "home"


class _Details:
    subject = "富力城｜两房"
    location = "富力城"
    monthly_rent_usd = 800

class _Inventory:
    def resolve(self, public_listing_id):
        assert public_listing_id == "QL-RF-A2B3"
        return object()


def test_property_advisor_view_keeps_public_ql_context(monkeypatch):
    import v3_core.user_bot.listing_contact as mod
    monkeypatch.setattr(mod, "build_public_listing_details", lambda view: _Details())
    intent = SimpleNamespace(public_listing_id="QL-RF-A2B3")
    view = build_listing_contact_view(intent, _Inventory(), advisor_url="https://t.me/advisor")
    assert "QL-RF-A2B3" in view.text
    assert "l_" not in view.text.lower()
    assert "问这套房" in view.text


@pytest.mark.asyncio
async def test_property_advisor_buttons_are_only_handoff_and_return():
    class Query:
        def __init__(self): self.message = SimpleNamespace(photo=[]); self.markup = None
        async def edit_message_text(self, text, **kwargs): self.markup = kwargs["reply_markup"]
    query = Query()
    await _render_contact(query, text="QL-RF-A2B3", public_listing_id="QL-RF-A2B3", advisor_url="https://t.me/advisor")
    labels = [button.text for row in query.markup.inline_keyboard for button in row]
    callbacks = [button.callback_data for row in query.markup.inline_keyboard for button in row if button.callback_data]
    assert labels == ["💬 打开中文顾问", "⬅️ 返回这套房"]
    assert callbacks == ["v3u:listing:details:QL-RF-A2B3"]
    assert all(term not in labels for term in ("📅 预约看房", "📸 更多实拍", "🔍 继续找房"))


def test_aftercare_and_unbound_resident_buttons_stay_frozen():
    assert _labels(build_assurance_home_view()) == ["📋 入住交接留档", "🔍 开始找房", "💬 中文顾问", "⬅️ 回首页"]
    assert _labels(service_home_view()) == ["💬 中文顾问", "🛡 看租后服务", "🔍 开始找房", "⬅️ 回首页"]
    class Service:
        def active_binding(self, user_id): return None
    assert _labels(tenant_home_view(Service(), 1)) == ["💬 中文顾问", "🛡 看租后服务", "🔍 开始找房", "⬅️ 回首页"]


def test_search_card_order_and_public_identity(monkeypatch):
    class D:
        public_listing_id = "QL-RF-A2B3"; project_name = "富力城"; location = "富力城"; layout = "2房"; property_type = "公寓"; monthly_rent_usd = 800; size_sqm = None; floor = ""; status_icon = "🟢"; status_label = "可预约"; bookable = True
    class V:
        public_listing_id = "QL-RF-A2B3"; package = {}
    import v3_core.user_bot.search_cards as mod
    monkeypatch.setattr(mod, "build_public_listing_details", lambda view: D())
    card = build_search_card((V(),), 0)
    labels = [choice.label for row in card.action_rows for choice in row]
    assert labels == ["📋 租赁详情", "📸 更多实拍", "📅 预约看房", "💬 问这套房", "✏️ 换个条件找"]
    assert "l_" not in repr(card).lower()


def test_public_service_buttons_use_generic_chinese_advisor_name():
    for view in (property_view(), repair_home_view(), nearby_view()):
        labels = _labels(view)
        assert "💬 联系顾问" not in labels
        assert all("联系我们" not in label for label in labels)
        if any("顾问" in label for label in labels):
            assert "💬 中文顾问" in labels

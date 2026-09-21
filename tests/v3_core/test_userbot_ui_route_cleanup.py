from __future__ import annotations

from types import SimpleNamespace

import pytest
from telegram import ReplyKeyboardRemove

from v3_core.user_bot.app import admin_command_menu, public_command_menu, remove_legacy_reply_keyboard
from v3_core.user_bot.assurance_views import build_assurance_home_view
from v3_core.user_bot.legacy_routes import legacy_home_action, legacy_reply_text_action, legacy_start_payload
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
        ("service", "侨联服务"),
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


def test_property_advisor_view_hides_public_ql_but_keeps_internal_identity(monkeypatch):
    import v3_core.user_bot.listing_contact as mod
    monkeypatch.setattr(mod, "build_public_listing_details", lambda view: _Details())
    intent = SimpleNamespace(public_listing_id="QL-RF-A2B3")
    view = build_listing_contact_view(intent, _Inventory(), advisor_url="https://t.me/advisor")
    assert "QL-RF-A2B3" not in view.text
    assert view.public_listing_id == "QL-RF-A2B3"
    assert "l_" not in view.text.lower()
    assert "咨询这套" in view.text


@pytest.mark.asyncio
async def test_property_advisor_buttons_are_only_handoff_and_return():
    class Query:
        def __init__(self): self.message = SimpleNamespace(photo=[]); self.markup = None
        async def edit_message_text(self, text, **kwargs): self.markup = kwargs["reply_markup"]
    query = Query()
    await _render_contact(query, text="QL-RF-A2B3", public_listing_id="QL-RF-A2B3", advisor_url="https://t.me/advisor")
    labels = [button.text for row in query.markup.inline_keyboard for button in row]
    callbacks = [button.callback_data for row in query.markup.inline_keyboard for button in row if button.callback_data]
    assert labels == ["中文顾问", "返回房源详情"]
    assert callbacks == ["v3u:listing:details:QL-RF-A2B3"]
    assert all(term not in labels for term in ("预约看房", "更多实拍", "继续找房"))


def test_aftercare_and_public_resident_buttons_stay_frozen():
    assert _labels(build_assurance_home_view()) == ["🔍 开始找房", "💬 中文顾问", "⬅️ 返回首页"]
    expected = [
        "📋 我的租约", "🛡️ 入住服务", "🏠 安心租房",
        "💬 中文顾问", "⬅️ 返回首页",
    ]
    assert _labels(service_home_view()) == expected
    class Service:
        def active_binding(self, user_id): return None
    assert _labels(tenant_home_view(Service(), 1)) == expected


def test_search_card_order_and_public_identity(monkeypatch):
    class D:
        public_listing_id = "QL-RF-A2B3"; project_name = "富力城"; location = "富力城"; layout = "2房"; property_type = "公寓"; monthly_rent_usd = 800; size_sqm = None; floor = ""; status_icon = "🟢"; status_label = "可预约"; bookable = True
    class V:
        public_listing_id = "QL-RF-A2B3"; package = {}
    import v3_core.user_bot.search_cards as mod
    monkeypatch.setattr(mod, "build_public_listing_details", lambda view: D())
    card = build_search_card((V(),), 0)
    labels = [choice.label for row in card.action_rows for choice in row]
    assert labels == ["📷 房源详情", "📅 预约看房", "换条件"]
    assert "l_" not in repr(card).lower()


def test_public_service_buttons_use_generic_chinese_advisor_name():
    for view in (property_view(), repair_home_view(), nearby_view()):
        labels = _labels(view)
        assert "联系顾问" not in labels
        assert all("联系我们" not in label for label in labels)
        if any("顾问" in label for label in labels):
            assert "中文顾问" in labels


def test_legacy_reply_button_texts_have_explicit_frozen_destinations():
    expected = {
        "开始找房": "search",
        "帮我找房": "search",
        "精准筛选": "search",
        "预约看房": "search",
        "我的收藏": "search",
        "我的预约": "appointments",
        "我的租约": "service",
        "入住服务": "service",
        "售后服务": "rental",
        "服务保障": "rental",
        "租后服务": "rental",
        "联系顾问": "contact",
        "联系我们": "contact",
        "中文顾问": "contact",
        "使用说明": "home",
        "关于侨联": "rental",
        "首页": "home",
        "返回首页": "home",
    }
    assert {label: legacy_reply_text_action(label) for label in expected} == expected
    assert legacy_reply_text_action("普通搜索文字") is None


def test_legacy_reply_destinations_reuse_existing_new_product_start_payloads():
    assert legacy_start_payload("search") == "find_home"
    assert legacy_start_payload("appointments") == "appointments"
    assert legacy_start_payload("service") == "service"
    assert legacy_start_payload("rental") == "assurance"
    assert legacy_start_payload("contact") == "advisor"
    assert legacy_start_payload("home") == ""


def test_legacy_reply_semantics_do_not_turn_old_navigation_into_keywords():
    for label in ("精准筛选", "我的收藏", "预约看房", "帮我找房"):
        assert legacy_reply_text_action(label) == "search"
        assert legacy_start_payload(legacy_reply_text_action(label)) == "find_home"
    assert legacy_reply_text_action("我的租约") == "service"
    assert legacy_reply_text_action("联系顾问") == "contact"
    for label in ("售后服务", "服务保障", "关于侨联"):
        assert legacy_reply_text_action(label) == "rental"


def test_legacy_reply_text_intercept_precedes_all_normal_text_pipelines():
    source = open("v3_core/user_bot/app.py", encoding="utf-8").read()
    text_start = source.index("    async def text(update, context):")
    text_end = source.index("    async def heartbeat(context):", text_start)
    block = source[text_start:text_end]
    intercept = block.index("legacy_reply_text_action")
    early_return = block.index("            return", intercept)
    transition = block.index("handle_v3_transition_text")
    service = block.index("handle_v3_service_text")
    keyword = block.index("handle_v3_keyword_search_text")
    remove = block.index("remove_legacy_reply_keyboard")
    assert remove < intercept < early_return < transition < service < keyword


def test_legacy_reply_text_never_exposes_internal_listing_ids():
    labels = (
        "开始找房", "帮我找房", "精准筛选", "预约看房", "我的收藏",
        "我的预约", "我的租约", "入住服务", "售后服务", "服务保障",
        "租后服务", "联系顾问", "联系我们", "中文顾问", "使用说明",
        "关于侨联", "首页", "返回首页",
    )
    rendered_contract = " ".join(
        f"{label}:{legacy_reply_text_action(label)}:{legacy_start_payload(legacy_reply_text_action(label))}"
        for label in labels
    )
    assert "l_" not in rendered_contract.lower()


def test_legacy_contact_and_aftercare_destinations_render_new_names():
    from v3_core.user_bot.home_views import build_contact_view
    contact = build_contact_view(advisor_url="https://t.me/advisor")
    assert "中文顾问" in contact.text
    assert "联系顾问" not in _labels(contact)
    rental = build_assurance_home_view()
    assert "侨联地产｜金边中文租房" in rental.text
    assert "签约不是服务的结束。" in rental.text
    assert "关于侨联" not in rental.text

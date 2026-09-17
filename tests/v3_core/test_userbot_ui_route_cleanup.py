from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from telegram import ReplyKeyboardRemove

from v3_core.user_bot.app import (
    ADMIN_BOT_COMMANDS,
    LEGACY_REPLY_TEXT_ROUTES,
    PUBLIC_BOT_COMMANDS,
    legacy_callback_action,
    remove_legacy_reply_keyboard,
)
from v3_core.user_bot.assurance_views import build_assurance_home_view
from v3_core.user_bot.consult import ConsultIntent
from v3_core.user_bot.home_views import build_contact_view
from v3_core.user_bot.listing_contact import build_listing_contact_view
from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.search_cards import build_search_card
from v3_core.user_bot.service_product_views import service_home_view
from v3_core.user_bot.telegram_listing_callback import _render_contact
from v3_core.user_bot.tenant_v1 import tenant_home_view


def _labels(view) -> list[str]:
    return [choice.label for row in view.rows for choice in row]


def _public_view(*, internal_id: str = "l_203", public_id: str = "QL-RF-A2B3") -> PublishedListingView:
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": internal_id,
        "public_listing_id": public_id,
        "canonical_facts": {},
        "listing": {
            "project_name": "富力城",
            "layout": "2房1厅",
            "public_location_display": "BKK1",
        },
        "offer": {
            "offer_type": "rent",
            "monthly_rent_usd": 800,
            "publication_policy": "telegram_rent",
        },
    }
    return PublishedListingView(
        listing={"listing_id": internal_id, "public_listing_id": public_id, "inventory_status": "active"},
        offer={"offer_id": "OFF_1", "offer_type": "rent", "offer_status": "active", "publication_policy": "telegram_rent"},
        publication={"instance_id": "PUB_1"},
        package={"snapshot_json": json.dumps(snapshot, ensure_ascii=False), "gallery_json": "[]", "cover_path": ""},
    )


def test_public_command_menu_uses_frozen_product_names_only():
    commands = [(item.command, item.description) for item in PUBLIC_BOT_COMMANDS]
    assert commands == [
        ("start", "打开侨联小管家"),
        ("find", "开始找房"),
        ("appointments", "我的预约"),
        ("service", "入住服务"),
        ("rental", "租后服务"),
        ("advisor", "中文顾问"),
    ]
    assert not {"/about", "/contact", "/help"} & {f"/{name}" for name, _ in commands}
    assert all(old not in {desc for _, desc in commands} for old in ("帮我找房", "联系我们", "使用帮助", "关于侨联"))


def test_admin_commands_are_not_in_public_command_scope():
    public_names = {item.command for item in PUBLIC_BOT_COMMANDS}
    admin_names = {item.command for item in ADMIN_BOT_COMMANDS}
    assert "admin" not in public_names
    assert "contracts" not in public_names
    assert {"admin", "contracts"} <= admin_names


@pytest.mark.asyncio
async def test_reply_keyboard_cleanup_uses_reply_keyboard_remove_and_deletes_cleanup_message():
    calls = []

    class Cleanup:
        async def delete(self):
            calls.append(("delete",))

    class Message:
        async def reply_text(self, text, **kwargs):
            calls.append(("reply", text, kwargs))
            return Cleanup()

    update = SimpleNamespace(effective_message=Message())
    await remove_legacy_reply_keyboard(update)

    assert calls[0][0] == "reply"
    assert isinstance(calls[0][2]["reply_markup"], ReplyKeyboardRemove)
    assert calls[1] == ("delete",)


def test_legacy_reply_labels_map_only_to_new_surfaces():
    assert LEGACY_REPLY_TEXT_ROUTES["精准筛选"] == "find_home"
    assert LEGACY_REPLY_TEXT_ROUTES["我的收藏"] == ""
    assert LEGACY_REPLY_TEXT_ROUTES["售后服务"] == "assurance"
    assert LEGACY_REPLY_TEXT_ROUTES["服务保障"] == "assurance"
    assert LEGACY_REPLY_TEXT_ROUTES["使用说明"] == ""
    assert LEGACY_REPLY_TEXT_ROUTES["联系顾问"] == "advisor"


def test_legacy_callback_mapping_keeps_compatibility_without_old_pages():
    assert legacy_callback_action("home_brand") == "rental"
    assert legacy_callback_action("hub:promise") == "rental"
    assert legacy_callback_action("service:promise") == "rental"
    assert legacy_callback_action("hub:advisor") == "contact"
    assert legacy_callback_action("appointment_menu:contact") == "contact"
    assert legacy_callback_action("hub:precise") == "search"
    assert legacy_callback_action("hub:favorites") == "root"
    assert legacy_callback_action("hub:help") == "root"
    assert legacy_callback_action("hub:service") == "service"
    assert legacy_callback_action("contract:view") == "service"


def test_legacy_commands_remain_handlers_but_are_not_public_menu():
    source = Path(__import__("v3_core.user_bot.app", fromlist=["x"]).__file__).read_text(encoding="utf-8")
    assert 'CommandHandler("about", about)' in source
    assert 'CommandHandler("contact", contact)' in source
    assert 'CommandHandler("help", help_command)' in source
    assert 'payload="assurance"' in source
    assert 'payload="advisor"' in source


def test_generic_and_property_advisor_names_are_separate():
    generic = build_contact_view(advisor_url="https://t.me/qiaolian_advisor")
    assert "中文顾问" in generic.text
    assert "问这套房" not in generic.text

    view = _public_view()
    inventory = SimpleNamespace(resolve=lambda public_id: view if public_id == "QL-RF-A2B3" else None)
    property_view = build_listing_contact_view(
        ConsultIntent(
            listing_id="l_203",
            public_listing_id="QL-RF-A2B3",
            source="search_result",
            inventory_status="active",
            offer_status="active",
            publication_instance_id="PUB_1",
        ),
        inventory,
        advisor_url="https://t.me/qiaolian_advisor",
    )
    assert "💬 <b>问这套房</b>" in property_view.text
    assert "QL-RF-A2B3" in property_view.text
    assert "l_203" not in property_view.text


@pytest.mark.asyncio
async def test_property_advisor_page_has_only_handoff_and_return_with_public_context():
    class Query:
        def __init__(self):
            self.message = SimpleNamespace(photo=[])
            self.kwargs = None

        async def edit_message_text(self, text, **kwargs):
            self.kwargs = {"text": text, **kwargs}

    query = Query()
    await _render_contact(
        query,
        text="💬 <b>问这套房</b>\n\n🆔 QL-RF-A2B3\n🏠 富力城｜2房1厅｜$800/月",
        public_listing_id="QL-RF-A2B3",
        advisor_url="https://t.me/qiaolian_advisor",
    )
    markup = query.kwargs["reply_markup"]
    buttons = [button for row in markup.inline_keyboard for button in row]
    assert [button.text for button in buttons] == ["💬 打开中文顾问", "⬅️ 返回这套房"]
    assert "QL-RF-A2B3" in buttons[0].url
    assert buttons[1].callback_data == "v3u:listing:details:QL-RF-A2B3"
    assert all(label not in [button.text for button in buttons] for label in ("📅 预约看房", "📸 更多实拍", "🔍 继续找房"))


def test_search_result_card_button_order_is_frozen():
    first = _public_view(public_id="QL-RF-A2B3")
    second = _public_view(internal_id="l_204", public_id="QL-BK-C4D5")
    labels = [[action.label for action in row] for row in build_search_card((first, second), 0).action_rows]
    assert labels == [
        ["⬅️ 上一套", "下一套 ➡️"],
        ["📋 租赁详情", "📸 更多实拍"],
        ["📅 预约看房"],
        ["💬 问这套房"],
        ["✏️ 换个条件找"],
    ]


def test_unbound_resident_buttons_match_frozen_product():
    view = service_home_view()
    assert _labels(view) == [
        "💬 中文顾问",
        "🛡 看租后服务",
        "🔍 开始找房",
        "⬅️ 回首页",
    ]


def test_rent_after_buttons_match_frozen_product():
    view = build_assurance_home_view()
    assert _labels(view) == [
        "📋 入住交接留档",
        "🔍 开始找房",
        "💬 中文顾问",
        "⬅️ 回首页",
    ]


def test_bound_resident_buttons_have_no_change_home():
    binding = SimpleNamespace(property_name="富力城")

    class Service:
        def active_binding(self, user_id):
            return binding

    labels = _labels(tenant_home_view(Service(), 1))
    assert labels == [
        "📋 我的租约",
        "🔧 报修",
        "🏢 物业协调",
        "📍 周边服务",
        "💬 中文顾问",
        "⬅️ 回首页",
    ]
    assert all("换房" not in label for label in labels)


def test_public_ui_never_exposes_internal_listing_id():
    card = build_search_card((_public_view(),), 0)
    assert "l_203" not in card.text
    assert "l_203" not in repr(card.action_rows)
    assert "QL-RF-A2B3" in repr(card.action_rows)

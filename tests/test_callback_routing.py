from __future__ import annotations

import re
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from qiaolian_dual import callbacks
from qiaolian_dual.admin_commands import cmd_lead_response, cmd_repair_update
from qiaolian_dual.callback_admin import handle_admin_callback, matches as matches_admin
from qiaolian_dual.callback_appointment import matches as matches_appointment
from qiaolian_dual.admin_contract import _binding_contract_text
from qiaolian_dual.callback_contract import handle_contract_callback, matches as matches_contract
from qiaolian_dual.callback_listing import handle_listing_callback, matches as matches_listing
from qiaolian_dual.callback_navigation import matches as matches_navigation
from qiaolian_dual.callback_preference import matches as matches_preference
from qiaolian_dual.callback_search import matches as matches_search
from qiaolian_dual.callback_service import handle_service_callback, matches as matches_service
from qiaolian_dual.common import APPT_CONFIRM, APPT_DATE, APPT_FOCUS, APPT_MODE, APPT_TIME, MAIN, _APPT_CB_PATTERN, _MAIN_CB_PATTERN
from qiaolian_dual.app import build_application
from qiaolian_dual.appointment_ui import _appointment_date_keyboard
from qiaolian_dual.keyboards_common import contact_handoff_keyboard
from qiaolian_dual.keyboards_search import rfcity_keyboard, service_hub_keyboard
from qiaolian_dual.messages import advisor_response_notice_text, deposit_text, home_text, repair_progress_text, service_hub_text, viewing_delivery_assurance_text, want_home_text
from qiaolian_dual.listing import listing_cost_text
from qiaolian_dual.flows import show_service_hub
from qiaolian_dual.results_admin import _find_result_card_content, admin_repair_keyboard, send_listing_photo_preview


class _Query:
    def __init__(self, data: str):
        self.data = data
        self.message = SimpleNamespace(photo=None)
        self.edits: list[tuple[str, object]] = []

    async def answer(self, *args, **kwargs):
        return None

    async def edit_message_text(self, text, **kwargs):
        self.edits.append((text, kwargs.get("reply_markup")))


class _Bot:
    def __init__(self):
        self.messages: list[tuple[int, str]] = []

    async def send_message(self, *, chat_id, text, **kwargs):
        self.messages.append((chat_id, text))


class CallbackRoutingTests(unittest.IsolatedAsyncioTestCase):
    def test_main_states_accept_all_non_appointment_callback_families(self):
        samples = [
            "home", "home_smart_search", "hub:latest", "resume:continue",
            "unavail:more:BKK1", "find:show_more", "findcard:0",
            "findmode:guided", "findtype:住宅", "findarea:rf", "findbudget:r2",
            "findback:area", "roompick:2房", "keyword:handoff",
            "listing:open:demo", "listing:photos:demo", "listing:appoint:demo",
            "listing:consult:demo", "listing:detail:demo", "listing:similar:demo",
            "appointment_menu:list", "adminlead:claim:1:2:3",
            "service:hub", "service_request:repair_ac", "service_slot:repair_ac:today",
            "pref:submit", "profile:repeat", "contract:view",
            "lead_capture:phone", "local:rfcity", "rfcity:restaurant",
        ]
        self.assertTrue(all(re.match(_MAIN_CB_PATTERN, value) for value in samples))

    def test_appointment_callbacks_have_a_dedicated_pattern(self):
        samples = [
            "apmode:offline", "apfocus:next", "apdate:08-30", "aptime:pm",
            "apconfirm:yes", "apedit:time", "appoint_back_mode",
            "appoint_back_date", "appoint_back_time", "home",
        ]
        self.assertTrue(all(re.match(_APPT_CB_PATTERN, value) for value in samples))

    def test_active_appointment_states_keep_appointment_priority_and_accept_navigation(self):
        application = build_application(token="123:synthetic")
        conversation = application.handlers[0][0]
        for state in (APPT_MODE, APPT_FOCUS, APPT_DATE, APPT_TIME, APPT_CONFIRM):
            callback_handlers = [handler for handler in conversation.states[state] if handler.__class__.__name__ == "CallbackQueryHandler"]
            with self.subTest(state=state):
                self.assertGreaterEqual(len(callback_handlers), 2)
                self.assertEqual(_APPT_CB_PATTERN, callback_handlers[0].pattern.pattern)
                self.assertEqual(_MAIN_CB_PATTERN, callback_handlers[1].pattern.pattern)

    def test_every_live_callback_family_has_a_domain_handler(self):
        checks = [
            (matches_navigation, "hub:service"),
            (matches_search, "findmode:guided"),
            (matches_listing, "find:show_more"),
            (matches_listing, "findcard:0"),
            (matches_listing, "listing:photos:demo"),
            (matches_appointment, "appointment_menu:list"),
            (matches_contract, "contract:view"),
            (matches_preference, "pref:submit"),
            (matches_service, "service:local_life"),
            (matches_admin, "adminlead:claim:1:2:3"),
        ]
        for matcher, data in checks:
            with self.subTest(data=data):
                self.assertTrue(matcher(data))

    async def test_listing_photo_does_not_discard_active_appointment_draft(self):
        query = _Query("listing:photos:demo")
        update = SimpleNamespace(
            callback_query=query,
            effective_user=SimpleNamespace(id=1),
            effective_chat=SimpleNamespace(id=100),
        )
        context = SimpleNamespace(
            user_data={"appt": {"listing_id": "demo", "date": "08-30"}},
            bot=_Bot(),
        )
        with patch.object(callbacks, "handle_listing_callback", new=AsyncMock(return_value=MAIN)):
            result = await callbacks.handle_ui_callback(
                update,
                context,
                hooks={"upsert_user_profile": lambda _user: None},
            )
        self.assertEqual(MAIN, result)
        self.assertEqual("demo", context.user_data["appt"]["listing_id"])

    def test_appointment_date_choices_are_mobile_friendly(self):
        keyboard = _appointment_date_keyboard()
        self.assertTrue(all(len(row) <= 2 for row in keyboard.inline_keyboard))
        self.assertEqual(["今天", "明天"], [button.text for button in keyboard.inline_keyboard[0]])
        self.assertEqual(["后天", "📅 其他日期"], [button.text for button in keyboard.inline_keyboard[1]])

    def test_handoff_buttons_use_plain_customer_language(self):
        keyboard = contact_handoff_keyboard()
        labels = [button.text for row in keyboard.inline_keyboard for button in row]
        self.assertIn("🎥 视频看房", labels)
        self.assertNotIn("🎥 改视频代看", labels)
        self.assertEqual("💬 直接联系顾问", keyboard.inline_keyboard[0][0].text)
        self.assertEqual("hub:latest", keyboard.inline_keyboard[-1][0].callback_data)

    def test_common_followup_notifications_are_warm_and_actionable(self):
        progress = repair_progress_text("空调/家电", "scheduled", "明天下午物业上门检查")
        advisor = advisor_response_notice_text()
        self.assertIn("已经安排处理", progress)
        self.assertIn("有新的情况，直接回复", progress)
        self.assertIn("补充：明天下午物业上门检查", progress)
        self.assertIn("顾问已经接手", advisor)
        self.assertIn("直接回复这条消息", advisor)

    async def test_advisor_repair_button_updates_customer_without_command(self):
        user = SimpleNamespace(id=7, full_name="顾问")
        query = _Query("adminrepair:scheduled:12")
        query.message.text = "🔔 新报修请求"
        update = SimpleNamespace(effective_user=user)
        bot = _Bot()
        context = SimpleNamespace(bot=bot)
        ticket = {"id": 12, "user_id": 88, "issue_type": "空调/家电"}
        with (
            patch("qiaolian_dual.admin_contract._is_admin_user", return_value=True),
            patch("qiaolian_dual.callback_admin.db.update_repair_ticket_status", return_value=ticket),
        ):
            result = await handle_admin_callback(update, context, query, query.data, user)
        self.assertEqual(MAIN, result)
        self.assertEqual(1, len(bot.messages))
        self.assertEqual(88, bot.messages[0][0])
        self.assertIn("已经安排处理", bot.messages[0][1])
        self.assertIn("已安排", query.edits[0][0])
        keyboard = admin_repair_keyboard(12)
        labels = [button.text for row in keyboard.inline_keyboard for button in row]
        self.assertIn("✅ 已接手", labels)
        self.assertIn("💬 需要客户补充", labels)
        self.assertNotIn("/repair_update", " ".join(labels))

    async def test_admin_lead_response_sends_customer_notice(self):
        reply = AsyncMock()
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=7),
            effective_message=SimpleNamespace(reply_text=reply),
        )
        bot = _Bot()
        context = SimpleNamespace(args=["9", "advisor_zhang"], bot=bot)
        with (
            patch("qiaolian_dual.admin_contract._is_admin_user", return_value=True),
            patch("qiaolian_dual.admin_commands.db.mark_lead_responded", return_value=True),
            patch("qiaolian_dual.admin_commands.db.get_lead", return_value={"user_id": 88}),
        ):
            result = await cmd_lead_response(update, context)
        self.assertEqual(MAIN, result)
        self.assertEqual(1, len(bot.messages))
        self.assertEqual(88, bot.messages[0][0])
        self.assertIn("顾问已经接手", bot.messages[0][1])
        reply.assert_awaited_once()

    async def test_admin_repair_update_sends_customer_progress(self):
        reply = AsyncMock()
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=7),
            effective_message=SimpleNamespace(reply_text=reply),
        )
        bot = _Bot()
        context = SimpleNamespace(args=["12", "已安排", "明天下午物业上门检查"], bot=bot)
        ticket = {"id": 12, "user_id": 88, "issue_type": "空调/家电"}
        with (
            patch("qiaolian_dual.admin_contract._is_admin_user", return_value=True),
            patch("qiaolian_dual.admin_commands.db.update_repair_ticket_status", return_value=ticket),
        ):
            result = await cmd_repair_update(update, context)
        self.assertEqual(MAIN, result)
        self.assertEqual(1, len(bot.messages))
        self.assertEqual(88, bot.messages[0][0])
        self.assertIn("报修进度更新", bot.messages[0][1])
        self.assertIn("已经安排处理", bot.messages[0][1])
        reply.assert_awaited_once()

    def test_service_hub_prioritizes_active_tenant_tasks(self):
        binding = {"property_name": "示例公寓"}
        with patch("qiaolian_dual.keyboards_search.db.get_active_binding", return_value=binding):
            tenant_keyboard = service_hub_keyboard(1)
        labels = [button.text for row in tenant_keyboard.inline_keyboard for button in row]
        self.assertEqual(["🔧 报修", "🏢 物业沟通"], [button.text for button in tenant_keyboard.inline_keyboard[0]])
        self.assertIn("🔁 续租 / 换房", labels)
        self.assertIn("📋 我的租约", labels)
        self.assertIn("🗺️ 周边生活", labels)

    async def test_non_tenant_historical_links_render_the_same_safe_service_split(self):
        user = SimpleNamespace(id=1, username="visitor", full_name="Visitor")
        update = SimpleNamespace(effective_user=user)
        query = SimpleNamespace()
        expected = ["🗂 找回老客档案", "🔍 帮我找房"]
        service_links = ("service:repair_hub", "service_request:property", "service:renew_change")
        contract_links = ("contract:view", "contract:renew", "contract:change")
        for handler, links in ((handle_service_callback, service_links), (handle_contract_callback, contract_links)):
            for data in links:
                context = SimpleNamespace(user_data={})
                with self.subTest(data=data), \
                    patch("qiaolian_dual.callback_service.db.get_active_binding", return_value=None), \
                    patch("qiaolian_dual.texts.render_panel", new=AsyncMock()) as render:
                    result = await handler(update, context, query, data, user)
                self.assertEqual(MAIN, result)
                text = render.await_args.kwargs["text"]
                keyboard = render.await_args.kwargs["reply_markup"]
                labels = [button.text for row in keyboard.inline_keyboard for button in row]
                self.assertIn("这里给已入住客户办事", text)
                self.assertEqual(expected, [button.text for button in keyboard.inline_keyboard[0]])
                self.assertNotIn("🔧 报修", labels)

    async def test_non_tenant_historical_service_buttons_return_to_service_split(self):
        update = SimpleNamespace(effective_user=SimpleNamespace(id=1, username="tenant", full_name="Tenant"))
        context = SimpleNamespace(user_data={})
        query = SimpleNamespace()
        for data in ("service:repair_hub", "service_request:property", "service:renew_change"):
            with self.subTest(data=data), \
                patch("qiaolian_dual.callback_service.db.get_active_binding", return_value=None), \
                patch("qiaolian_dual.flows.show_service_hub", new=AsyncMock(return_value=MAIN)) as show_hub:
                result = await handle_service_callback(update, context, query, data, update.effective_user)
                self.assertEqual(MAIN, result)
                show_hub.assert_awaited_once_with(update, context)

    async def test_repair_success_copy_is_natural_and_actionable(self):
        user = SimpleNamespace(id=1, username="tenant", full_name="Tenant")
        update = SimpleNamespace(effective_user=user)
        query = SimpleNamespace()
        context = SimpleNamespace(user_data={"service_request_detail": {"detail": "空调不制冷"}})
        with (
            patch("qiaolian_dual.callback_service.db.get_active_binding", return_value={"id": 3, "property_name": "示例公寓"}),
            patch("qiaolian_dual.callback_service.db.create_repair_ticket", return_value=8),
            patch("qiaolian_dual.search.create_lead", return_value=1),
            patch("qiaolian_dual.results_admin._notify_admins", new=AsyncMock()),
            patch("qiaolian_dual.texts.render_panel", new=AsyncMock()) as render,
        ):
            result = await handle_service_callback(update, context, query, "service_slot:repair_ac:today", user)
        self.assertEqual(MAIN, result)
        text = render.await_args.kwargs["text"]
        self.assertIn("已帮你记下了", text)
        self.assertIn("希望 今天内安排", text)
        self.assertIn("情况有变化", text)
        self.assertNotIn("工单编号", text)
        self.assertNotIn("60 分钟", text)

    async def test_non_tenant_service_hub_shows_service_scope_without_exposing_tenant_actions(self):
        user = SimpleNamespace(id=1, username="visitor", full_name="Visitor")
        update = SimpleNamespace(effective_user=user)
        context = SimpleNamespace(user_data={})
        with (
            patch("qiaolian_dual.flows.db.get_active_binding", return_value=None),
            patch("qiaolian_dual.keyboards_search.db.get_active_binding", return_value=None),
            patch("qiaolian_dual.texts.render_panel", new=AsyncMock()) as render,
        ):
            result = await show_service_hub(update, context)
        self.assertEqual(MAIN, result)
        text = render.await_args.kwargs["text"]
        keyboard = render.await_args.kwargs["reply_markup"]
        labels = [button.text for row in keyboard.inline_keyboard for button in row]
        self.assertIn("房子有问题，帮你跟进报修", text)
        self.assertIn("物业沟通、续租 / 换房、退租交接，帮你接着协调", text)
        self.assertIn("需要周边生活信息，也可以问我们", text)
        self.assertEqual(["🗂 找回老客档案", "🔍 帮我找房"], [button.text for button in keyboard.inline_keyboard[0]])
        self.assertNotIn("🔧 报修", labels)

    def test_service_hub_non_tenant_has_only_relevant_exits(self):
        with patch("qiaolian_dual.keyboards_search.db.get_active_binding", return_value=None):
            keyboard = service_hub_keyboard(1)
        labels = [button.text for row in keyboard.inline_keyboard for button in row]
        self.assertEqual(["🗂 找回老客档案", "🔍 帮我找房"], [button.text for button in keyboard.inline_keyboard[0]])
        self.assertNotIn("🔧 报修", labels)
        self.assertIn("报修、物业、续租或换房", service_hub_text())

    async def test_full_photo_preview_uses_short_followup_not_duplicate_cost_page(self):
        class PreviewBot(_Bot):
            def __init__(self):
                super().__init__()
                self.media_groups = []
                self.message_markups = []

            async def send_media_group(self, *, chat_id, media, **kwargs):
                self.media_groups.append((chat_id, media))

            async def send_message(self, *, chat_id, text, **kwargs):
                await super().send_message(chat_id=chat_id, text=text, **kwargs)
                self.message_markups.append(kwargs.get("reply_markup"))

        with tempfile.TemporaryDirectory() as directory:
            first = f"{directory}/one.jpg"
            second = f"{directory}/two.jpg"
            open(first, "wb").write(b"a")
            open(second, "wb").write(b"b")
            bot = PreviewBot()
            with (
                patch("qiaolian_dual.listing.listing_context", return_value={"project": "示例公寓", "layout": "2房", "media_files": [first, second]}),
                patch("qiaolian_dual.listing.listing_cost_text", return_value="<b>费用确认</b>"),
            ):
                await send_listing_photo_preview(bot, 88, "demo")
        self.assertEqual(1, len(bot.media_groups))
        self.assertEqual(1, len(bot.messages))
        self.assertIn("实拍已全部显示", bot.messages[0][1])
        self.assertNotIn("费用确认", bot.messages[0][1])
        labels = [button.text for row in bot.message_markups[0].inline_keyboard for button in row]
        self.assertIn("📋 租赁详情", labels)
        self.assertNotIn("在频道查看原帖", labels)

    def test_cost_page_uses_one_fact_per_line_and_clear_followup(self):
        item = {
            "project": "示例公寓", "layout": "2房", "price": 680, "deposit": "押2付1",
            "normalized_data": {"management_fee": "$20/月", "internet_fee": "$15/月", "electric_rate": "按表", "parking_fee": "$30/月"},
        }
        with patch("qiaolian_dual.listing.listing_context", return_value=item):
            text = listing_cost_text("demo")
        self.assertIn("租赁详情", text)
        self.assertIn("💰 <b>租金：$680/月</b>", text)
        self.assertIn("🔑 <b>押付：</b> 押2付1", text)
        self.assertIn("🏢 <b>物业费：</b> $20/月", text)
        self.assertIn("📅 <b>想实地看看？</b>", text)
        self.assertNotIn("　·　", text)

    def test_contract_and_deposit_pages_use_short_grouped_sections(self):
        binding = {"property_name": "示例公寓", "rent_day": 5, "monthly_rent": 680, "deposit_months": 2, "lease_end_date": "2026-12-31"}
        with patch("qiaolian_dual.admin_contract.db.is_lease_reminder_enabled", return_value=True):
            contract = _binding_contract_text(binding, 1)
        deposit = deposit_text()
        self.assertIn("房源与账期", contract)
        self.assertIn("金额与到期", contract)
        self.assertIn("当前状态", contract)
        self.assertNotIn("房号/项目", contract)
        self.assertIn("看房和签约前", deposit)
        self.assertIn("入住当天", deposit)
        self.assertIn("准备退租时", deposit)
        self.assertNotIn("押金保障（方向性）", deposit)

    def test_home_and_life_platform_entries_keep_current_scope_clear(self):
        text = home_text()
        self.assertNotIn("套 ·", text)
        self.assertNotIn("已有预约", text)
        self.assertIn("区域和预算", text)
        labels = [button.text for row in rfcity_keyboard().inline_keyboard for button in row]
        self.assertNotIn("🤝 商家入驻", labels)

    def test_key_assurance_and_find_copy_is_short_and_actionable(self):
        assurance = viewing_delivery_assurance_text()
        demand = want_home_text()
        self.assertIn("看中后：费用逐项核对", assurance)
        self.assertIn("入住时：验房、水电表和钥匙/门卡确认留档", assurance)
        self.assertNotIn("可选条件包括", demand)
        self.assertIn("缩小到 1–3 套", demand)

    def test_recommendation_card_navigation_has_only_two_real_actions(self):
        item = {
            "listing_id": "demo",
            "area": "BKK1",
            "layout": "1房",
            "price": 500,
            "status": "active",
        }
        with patch("qiaolian_dual.listing.listing_context", return_value={}):
            caption, keyboard, _ = _find_result_card_content(item, 1, 3)
        self.assertIn("第 2/3 套", caption)
        self.assertEqual(2, len(keyboard.inline_keyboard[0]))
        self.assertEqual(["⬅️ 上一套", "下一套 ➡️"], [button.text for button in keyboard.inline_keyboard[0]])
        labels = [button.text for row in keyboard.inline_keyboard for button in row]
        self.assertNotIn("💎 顾问帮我选", labels)
        self.assertEqual(["✏️ 换条件"], [button.text for button in keyboard.inline_keyboard[-1]])
        self.assertNotIn("findcard:noop", [button.callback_data for button in keyboard.inline_keyboard[0]])

    async def test_find_show_more_empty_set_has_recovery_actions(self):
        query = _Query("find:show_more")
        update = SimpleNamespace(
            callback_query=query,
            effective_user=SimpleNamespace(id=1),
            effective_chat=SimpleNamespace(id=100),
        )
        bot = _Bot()
        context = SimpleNamespace(user_data={"find_more_listing_ids": []}, bot=bot)
        result = await handle_listing_callback(update, context, query, "find:show_more", update.effective_user)
        self.assertEqual(MAIN, result)
        self.assertEqual(1, len(bot.messages))
        self.assertIn("已经看完", bot.messages[0][1])

    async def test_unknown_callback_returns_an_actionable_message(self):
        query = _Query("removed:old_button")
        update = SimpleNamespace(
            callback_query=query,
            effective_user=SimpleNamespace(id=1),
            effective_chat=SimpleNamespace(id=100),
        )
        context = SimpleNamespace(user_data={}, bot=_Bot())
        result = await callbacks.handle_ui_callback(
            update,
            context,
            hooks={"upsert_user_profile": lambda _user: None},
        )
        self.assertEqual(MAIN, result)
        self.assertEqual(1, len(query.edits))
        self.assertIn("操作已失效", query.edits[0][0])


if __name__ == "__main__":
    unittest.main()

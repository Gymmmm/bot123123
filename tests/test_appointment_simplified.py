import unittest
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from qiaolian_dual import user_bot


class SimplifiedAppointmentTests(unittest.IsolatedAsyncioTestCase):
    def test_legacy_and_current_buttons_share_one_compatibility_route(self):
        callbacks = (
            "apmode:offline",
            "apfocus:toggle:ac",
            "apfocus:next",
            "apdate:08-12",
            "aptime:pm",
            "apconfirm:yes",
            "appoint_back_mode",
            "appoint_back_date",
            "appoint_back_time",
        )
        for callback in callbacks:
            self.assertIsNotNone(re.match(user_bot._APPT_CB_PATTERN, callback), callback)

    async def test_known_viewing_mode_skips_customer_focus_form(self):
        context = SimpleNamespace(user_data={})

        with patch.object(user_bot, "render_panel", new=AsyncMock()) as render:
            state = await user_bot.start_appointment(
                SimpleNamespace(),
                context,
                "待推荐",
                touch_payload={"listing_unknown": True},
                initial_mode="offline",
            )

        self.assertEqual(state, user_bot.APPT_DATE)
        text = render.await_args.kwargs["text"]
        self.assertIn("下一步：选择你方便的日期", text)
        self.assertIn("重点核对房屋情况、费用和周边环境", text)
        self.assertNotIn("请选择你最关注的验房点", text)
        self.assertEqual(
            context.user_data["appt"]["focus_keys"],
            list(user_bot.APPOINTMENT_FOCUS_ORDER),
        )

    async def test_back_paths_preserve_listing_source_and_default_checks(self):
        query = SimpleNamespace(
            data="appoint_back_date",
            answer=AsyncMock(),
            edit_message_text=AsyncMock(),
        )
        update = SimpleNamespace(callback_query=query)
        context = SimpleNamespace(
            user_data={
                "appt": {
                    "listing_id": "L_0315",
                    "source": "channel_post",
                    "touch_payload": {"post_token": "p1"},
                    "mode": "video",
                    "focus_keys": list(user_bot.APPOINTMENT_FOCUS_ORDER),
                    "date": "08-12",
                }
            }
        )

        state = await user_bot.appoint_flow_cb(update, context)

        self.assertEqual(state, user_bot.APPT_DATE)
        self.assertEqual(context.user_data["appt"]["listing_id"], "L_0315")
        self.assertEqual(context.user_data["appt"]["source"], "channel_post")
        self.assertEqual(context.user_data["appt"]["touch_payload"], {"post_token": "p1"})
        self.assertEqual(
            context.user_data["appt"]["focus_keys"],
            list(user_bot.APPOINTMENT_FOCUS_ORDER),
        )

    async def test_stale_old_button_fails_closed_without_creating_empty_draft(self):
        query = SimpleNamespace(
            data="apfocus:next",
            answer=AsyncMock(),
            edit_message_text=AsyncMock(),
        )
        update = SimpleNamespace(callback_query=query)
        context = SimpleNamespace(user_data={})

        state = await user_bot.appoint_flow_cb(update, context)

        self.assertEqual(state, user_bot.MAIN)
        self.assertNotIn("appt", context.user_data)
        text = query.edit_message_text.await_args.args[0]
        self.assertIn("预约已失效", text)

    async def test_submit_keeps_full_context_and_notifies_advisor(self):
        query = SimpleNamespace(
            data="apconfirm:yes",
            answer=AsyncMock(),
            edit_message_text=AsyncMock(),
        )
        user = SimpleNamespace(id=1001, username="tenant", full_name="Tenant")
        update = SimpleNamespace(callback_query=query, effective_user=user)
        context = SimpleNamespace(
            user_data={
                "appt": {
                    "listing_id": "L_0315",
                    "source": "channel_post",
                    "touch_payload": {"post_token": "p1"},
                    "mode": "offline",
                    "date": "08-12",
                    "time": "pm",
                    "focus_keys": list(user_bot.APPOINTMENT_FOCUS_ORDER),
                }
            }
        )

        with (
            patch.object(user_bot.db, "create_appointment", return_value=88) as create_appt,
            patch.object(user_bot, "create_lead", return_value=99) as create_lead,
            patch.object(user_bot, "_notify_admins", new=AsyncMock()) as notify,
        ):
            state = await user_bot.appoint_flow_cb(update, context)

        self.assertEqual(state, user_bot.MAIN)
        saved = create_appt.call_args.args[0]
        self.assertEqual(saved["listing_id"], "L_0315")
        self.assertEqual(saved["viewing_mode"], "offline")
        self.assertEqual(saved["appointment_date"], "08-12")
        self.assertEqual(saved["appointment_time"], "pm")
        self.assertEqual(saved["status"], "pending")
        self.assertIn("空调", saved["note"])
        payload = create_lead.call_args.kwargs["payload"]
        self.assertEqual(payload["post_token"], "p1")
        self.assertEqual(payload["focus_keys"], list(user_bot.APPOINTMENT_FOCUS_ORDER))
        notify_lines = notify.await_args.kwargs["lines"]
        self.assertTrue(any("房源：L_0315" in line for line in notify_lines))
        self.assertTrue(any("方式：实地看房" in line for line in notify_lines))
        self.assertNotIn("appt", context.user_data)


if __name__ == "__main__":
    unittest.main()

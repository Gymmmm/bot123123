import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from qiaolian_dual import attribution as attr
from qiaolian_dual.attribution_store import (
    apply_lead_attribution_columns,
    ensure_attribution_schema,
    source_stats,
    update_lead_status,
    upsert_user_attribution,
)
from qiaolian_dual.db import Database


class AdminCopyTests(unittest.TestCase):
    def test_admin_source_is_chinese(self):
        self.assertEqual(attr.admin_source_group_zh("channel_listing_detail"), "频道房源详情")
        self.assertEqual(attr.admin_source_group_zh("channel_listing_book"), "频道预约看房")
        self.assertEqual(attr.admin_source_group_zh("channel_listing_photos"), "频道更多实拍")
        self.assertEqual(attr.admin_source_group_zh("bot_search"), "Bot 找房")
        self.assertEqual(attr.admin_source_group_zh("bot_listing_consult"), "Bot 房源咨询")
        self.assertEqual(attr.admin_source_group_zh("bot_move_in_service"), "入住服务")
        self.assertEqual(attr.admin_source_group_zh("bot_assurance"), "侨联保障")
        self.assertEqual(attr.lead_status_zh("new"), "新咨询")
        self.assertEqual(attr.lead_status_zh("claimed"), "已接手")
        self.assertEqual(attr.lead_status_zh("contacted"), "已联系")
        self.assertEqual(attr.lead_status_zh("booked"), "已预约")
        self.assertEqual(attr.lead_status_zh("done"), "已完成")
        self.assertEqual(attr.lead_status_zh("invalid"), "无效")

    def test_notify_card_hides_internal_keys(self):
        from qiaolian_dual.admin_consult import format_consult_notify
        user = SimpleNamespace(id=9, username="xxxx", first_name="张三", full_name="张三")
        title, lines = format_consult_notify(
            user=user,
            touch={
                "first_source_type": "channel_listing_detail",
                "latest_source_type": "channel_listing_detail",
                "entry_action": "consult_menu_click",
                "deep_link_payload": "property_QC0001_details",
                "listing_id": "",
            },
            listing_id="",
            current_action="consult_menu_click",
        )
        blob = title + "\n".join(lines)
        self.assertIn("张三", blob)
        self.assertIn("@xxxx", blob)
        self.assertIn("频道房源详情", blob)
        self.assertIn("联系我们", blob)
        self.assertIn("property_QC0001_details", blob)
        self.assertNotIn("source_type", blob)
        self.assertNotIn("callback_data", blob)
        self.assertNotIn("uuid", blob.lower())
        self.assertNotIn("listing:consult", blob)

    def test_admin_keyboards_fit_mobile_and_callback_limit(self):
        from qiaolian_dual.admin_consult import admin_home_keyboard, consult_action_keyboard
        home = admin_home_keyboard()
        for row in home.inline_keyboard:
            self.assertLessEqual(len(row), 2)
            for button in row:
                self.assertLessEqual(len(str(button.callback_data).encode("utf-8")), 64)
        actions = consult_action_keyboard(lead_id=8, appointment_id=9, user_id=1001, listing_id="l_1")
        labels = [button.text for row in actions.inline_keyboard for button in row]
        callbacks = [button.callback_data for row in actions.inline_keyboard for button in row]
        self.assertIn("✅ 我来跟进", labels)
        self.assertIn("📞 已联系", labels)
        self.assertIn("✅ 完成", labels)
        self.assertIn("🚫 结束跟进", labels)
        self.assertIn("🏠 查看房源", labels)
        self.assertTrue(all(len(row) <= 2 for row in actions.inline_keyboard))
        self.assertEqual(2, len(actions.inline_keyboard[0]))
        self.assertEqual(2, len(actions.inline_keyboard[1]))
        self.assertTrue(all(len(value.encode("utf-8")) <= 64 for value in callbacks))
        self.assertIn("adminlead:claim:8:9:1001", callbacks)
        self.assertIn("adminlead:done:8:9:1001", callbacks)
        self.assertIn("adminlead:invalid:8:9:1001", callbacks)
        self.assertIn("adminlead:view:8:9:1001", callbacks)


class StoreCompatTests(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.td.name) / "attr.db")
        from qiaolian_dual import attribution_store
        from qiaolian_dual import common
        self._old_store_db = attribution_store.db
        self._old_common_db = common.db
        attribution_store.db = self.db
        common.db = self.db
        ensure_attribution_schema()

    def tearDown(self):
        from qiaolian_dual import attribution_store
        from qiaolian_dual import common
        attribution_store.db = self._old_store_db
        common.db = self._old_common_db
        self.td.cleanup()

    def test_schema_is_additive(self):
        tables = self.db._table_names()
        self.assertIn("leads", tables)
        self.assertIn("user_attribution", tables)
        cols = self.db._table_columns("leads")
        for name in ("source_type", "source_detail", "first_source_type", "first_entry_at", "latest_touch_at", "entry_action", "deep_link_payload"):
            self.assertIn(name, cols)

    def test_status_update_done_without_rebuild(self):
        lead_id = self.db.create_lead({
            "user_id": 7, "action": "consult_menu_click", "source": "listing_card",
            "listing_id": "l_3", "created_at": "2026-09-05 12:00:00",
        })
        self.assertTrue(update_lead_status(lead_id, "done", advisor_id="1", advisor_name="顾问"))
        lead = self.db.get_lead(lead_id)
        self.assertEqual(lead["lead_status"], "done")

    def test_source_stats_uses_first_source(self):
        upsert_user_attribution({
            "user_id": 3, "first_source_type": "channel_listing_detail",
            "latest_source_type": "bot_listing_consult", "latest_touch_at": "2026-09-05 12:00:00",
        })
        lead_id = self.db.create_lead({
            "user_id": 3, "action": "consult_menu_click", "source": "listing_card",
            "created_at": "2026-09-05 12:00:00",
        })
        apply_lead_attribution_columns(lead_id, {
            "source_type": "bot_listing_consult",
            "first_source_type": "channel_listing_detail",
            "latest_source_type": "bot_listing_consult",
            "latest_touch_at": "2026-09-05 12:01:00",
        })
        rows = source_stats()
        self.assertTrue(any(item.get("src") == "channel_listing_detail" for item in rows))


if __name__ == "__main__":
    unittest.main()

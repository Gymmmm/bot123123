import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from qiaolian_dual import attribution as attr
from qiaolian_dual.attribution_store import (
    apply_lead_attribution_columns,
    ensure_attribution_schema,
    get_user_attribution,
    source_stats,
    update_lead_status,
    upsert_user_attribution,
)
from qiaolian_dual.db import Database
from qiaolian_dual.search import create_lead
from qiaolian_dual.session_deeplink import parse_start_arg_payload


class _AttrDBGuard(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.td.name) / "attr.db")
        from qiaolian_dual import attribution_store
        from qiaolian_dual import common
        from qiaolian_dual import search as search_mod
        self._old_store_db = attribution_store.db
        self._old_common_db = common.db
        self._old_search_db = search_mod.db
        attribution_store.db = self.db
        common.db = self.db
        search_mod.db = self.db
        ensure_attribution_schema()

    def tearDown(self):
        from qiaolian_dual import attribution_store
        from qiaolian_dual import common
        from qiaolian_dual import search as search_mod
        attribution_store.db = self._old_store_db
        common.db = self._old_common_db
        search_mod.db = self._old_search_db
        self.td.cleanup()


class DeepLinkAttributionTests(unittest.TestCase):
    def test_official_public_formats(self):
        cases = {
            "property_QC0001_details": "channel_listing_detail",
            "property_QC0001_photos": "channel_listing_photos",
            "property_QC0001_book": "channel_listing_book",
            "find_area": "channel_index_area",
            "find_budget": "channel_index_budget",
            "find_layout": "channel_index_layout",
            "latest": "channel_latest",
            "advisor": "channel_advisor",
        }
        for raw, source_type in cases.items():
            parsed = parse_start_arg_payload(raw)
            self.assertIsNotNone(parsed, raw)
            classified = attr.classify_start_arg(raw)
            self.assertEqual(classified["source_type"], source_type, raw)
            self.assertEqual(classified["deep_link_payload"], raw)
            self.assertFalse(classified["legacy"], raw)
            self.assertTrue(attr.public_deep_link_ok(raw), raw)

    def test_discussion_entry_is_legacy_only(self):
        classified = attr.classify_start_arg("discussion_entry__abc__l_42")
        self.assertTrue(classified["legacy"])
        self.assertEqual(classified["source_detail"], "legacy_discussion_entry")
        self.assertNotEqual(classified["source_type"], "discussion_entry")
        self.assertEqual(classified["source_type"], "channel_listing_detail")
        self.assertEqual(attr.admin_source_group_zh(classified["source_type"]), "频道房源详情")

    def test_empty_start_is_bot_direct(self):
        classified = attr.classify_start_arg("")
        self.assertEqual(classified["source_type"], "bot_direct_start")
        self.assertEqual(classified["entry_action"], "direct_start")

    def test_unknown_fallback_is_other(self):
        classified = attr.classify_bot_event(action="mystery_event", source="nowhere")
        self.assertEqual(classified["source_type"], "other")
        self.assertEqual(attr.admin_source_group_zh(classified["source_type"]), "其他")
        self.assertEqual(attr.source_type_zh(""), "其他")


class FirstTouchLockTests(_AttrDBGuard):
    def test_first_source_not_overwritten_latest_updates(self):
        user = SimpleNamespace(id=1001, username="zhang", first_name="张三", full_name="张三")
        first = attr.remember_touch(
            user, action="listing_detail_view", source="channel_deeplink",
            listing_id="l_1", start_arg="property_QC0001_details",
        )
        self.assertEqual(first["first_source_type"], "channel_listing_detail")
        self.assertEqual(first["listing_id"], "l_1")
        second = attr.remember_touch(
            user, action="consult_menu_click", source="listing_card", listing_id="l_1",
        )
        self.assertEqual(second["first_source_type"], "channel_listing_detail")
        self.assertEqual(second["first_entry_at"], first["first_entry_at"])
        self.assertEqual(second["latest_source_type"], "bot_listing_consult")
        stored = get_user_attribution(1001)
        self.assertEqual(stored["first_source_type"], "channel_listing_detail")
        self.assertEqual(stored["latest_source_type"], "bot_listing_consult")

    def test_create_lead_listing_id_is_kept(self):
        user = SimpleNamespace(id=2002, username="li", first_name="李四", full_name="李四")
        attr.remember_touch(
            user, action="details", source="channel_deeplink", listing_id="l_8",
            payload={"channel_message_id": 555}, start_arg="property_QC0008_details",
        )
        lead_id = create_lead(
            user, action="consult_menu_click", source="listing_card", listing_id="l_8",
            payload={"channel_message_id": 555, "start_arg": "property_QC0008_details"},
        )
        self.assertIsNotNone(lead_id)
        lead = self.db.get_lead(lead_id)
        self.assertEqual(str(lead.get("listing_id") or ""), "l_8")

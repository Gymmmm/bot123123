import tempfile
from pathlib import Path
from types import SimpleNamespace

from qiaolian_dual import attribution_store
from qiaolian_dual.admin_consult import consult_action_keyboard, format_consult_notify
from qiaolian_dual.attribution import (
    admin_source_group_zh,
    classify_start_arg,
    merge_touch,
    public_deep_link_ok,
)
from qiaolian_dual.db import Database


def test_public_deeplink_classification():
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
        assert classify_start_arg(payload)["source_type"] == source_type


def test_first_source_is_frozen_latest_touch_changes():
    first = merge_touch(
        None,
        classify_start_arg("property_QC0001_details"),
        user_id=1001,
        username="tester",
        display_name="张三",
        now="2026-09-05 09:00:00",
    )
    later = merge_touch(
        first,
        {"source_type": "bot_search", "source_detail": "user_search", "entry_action": "smart_search"},
        user_id=1001,
        username="tester",
        display_name="张三",
        now="2026-09-05 09:05:00",
    )
    assert later["first_source_type"] == "channel_listing_detail"
    assert later["first_entry_at"] == "2026-09-05 09:00:00"
    assert later["latest_source_type"] == "bot_search"
    assert later["latest_touch_at"] == "2026-09-05 09:05:00"


def test_legacy_discussion_entry_maps_to_listing_detail():
    got = classify_start_arg("discussion_entry__abc__l_1")
    assert got["legacy"] is True
    assert got["source_type"] == "channel_listing_detail"
    assert got["source_detail"] == "legacy_discussion_entry"


def test_chinese_source_labels_and_short_callbacks():
    assert admin_source_group_zh("channel_listing_detail") == "频道房源详情"
    assert admin_source_group_zh("bot_search") == "Bot 找房"
    with_listing = consult_action_keyboard(lead_id=8, appointment_id=9, user_id=1001, listing_id="l_1")
    callbacks = [button.callback_data for row in with_listing.inline_keyboard for button in row]
    assert "adminlead:claim:8:9:1001" in callbacks
    assert "adminlead:contacted:8:9:1001" in callbacks
    assert "adminlead:done:8:9:1001" in callbacks
    assert "adminlead:invalid:8:9:1001" in callbacks
    assert "adminlead:view:8:9:1001" in callbacks
    assert all(len(value.encode("utf-8")) <= 64 for value in callbacks)
    assert all(len(row) <= 2 for row in with_listing.inline_keyboard)
    assert [button.text for button in with_listing.inline_keyboard[0]] == ["✅ 我来跟进", "📞 已联系"]
    assert [button.text for button in with_listing.inline_keyboard[1]] == ["✅ 完成", "🚫 结束跟进"]

    without_listing = consult_action_keyboard(lead_id=8, appointment_id=9, user_id=1001)
    bare = [button.callback_data for row in without_listing.inline_keyboard for button in row]
    assert "adminlead:view:8:9:1001" not in bare
    assert "adminlead:invalid:8:9:1001" in bare


def test_schema_migration_keeps_existing_leads_read_write(monkeypatch):
    with tempfile.TemporaryDirectory() as td:
        test_db = Database(Path(td) / "prod-copy.db")
        original_id = test_db.create_lead({
            "user_id": 1001,
            "username": "before",
            "display_name": "迁移前",
            "source": "channel",
            "action": "consult_click",
            "listing_id": "l_1",
            "created_at": "2026-09-05 09:00:00",
        })
        monkeypatch.setattr(attribution_store, "db", test_db)
        attribution_store.ensure_attribution_schema()
        attribution_store.ensure_attribution_schema()

        with test_db.connect() as conn:
            cols = {row["name"] for row in conn.execute("PRAGMA table_info(leads)").fetchall()}
            before = conn.execute("SELECT id, user_id, listing_id FROM leads WHERE id=?", (original_id,)).fetchone()
        assert before["user_id"] == 1001
        assert before["listing_id"] == "l_1"
        for required in {
            "source_type", "source_detail", "first_source_type", "first_source_detail",
            "first_entry_at", "latest_touch_at", "entry_action", "deep_link_payload", "channel_message_id",
        }:
            assert required in cols

        after_id = test_db.create_lead({
            "user_id": 1002,
            "username": "after",
            "display_name": "迁移后",
            "source": "bot",
            "action": "consult_click",
            "listing_id": "l_2",
            "created_at": "2026-09-05 09:10:00",
        })
        assert test_db.get_lead(after_id)["listing_id"] == "l_2"


def test_notification_card_uses_chinese_attribution():
    user = SimpleNamespace(id=1001, username="tester", first_name="张三", full_name="张三")
    title, lines = format_consult_notify(
        user=user,
        touch={
            "first_source_type": "channel_listing_detail",
            "latest_source_type": "bot_listing_consult",
            "entry_action": "consult_menu_click",
            "latest_deep_link": "property_QC0001_details",
        },
        listing_id="",
        title="房源咨询",
        current_action="consult_menu_click",
    )
    text = "\n".join(lines)
    assert title == "房源咨询"
    assert "客户：张三 @tester" in text
    assert "来源：Bot 房源咨询" in text
    assert "首次进入：频道房源详情" in text
    assert "本次动作：联系我们" in text
    assert "入口：property_QC0001_details" in text
    assert "source_type" not in text
    assert "<code>" not in text

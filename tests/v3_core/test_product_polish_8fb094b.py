from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from v3_core.publishing.telegram_adapter import build_channel_keyboard
from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.user_bot.admin_appointments import AdminAppointmentReader, handle_admin_callback
from v3_core.user_bot.admin_notification_plans import display_contact_source
from v3_core.user_bot.channel_status_sync import (
    appointment_status_label,
    caption_with_appointment_status,
    sync_published_listing_status,
)
from v3_core.user_bot.home_views import build_contact_view, build_home_view
from v3_core.user_bot.listing_presenter import inventory_status_presentation
from v3_core.user_bot.search_no_match_view import build_search_no_match_view
from v3_core.user_bot.search_query import SearchCriteria
from v3_core.user_bot.service_views import rfcity_category_view, rfcity_home_view
from v3_core.user_bot.telegram_home_ui import build_home_keyboard
from v3_core.user_bot.transition_actions import SearchSubmitIntent
from v3_core.user_bot.transition_callbacks import encode_transition_choice


ACTIONS = {
    "details": "https://t.me/TestBot?start=property_QL-RF-A2B3_details",
    "photos": "https://t.me/TestBot?start=property_QL-RF-A2B3_photos",
    "book": "https://t.me/TestBot?start=property_QL-RF-A2B3_book",
}


def _button_labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


@pytest.mark.parametrize(
    ("status", "has_book"),
    [
        ("active", True),
        ("reserved", True),
        ("pending", False),
        ("unknown", False),
        ("rented", False),
        ("inactive", False),
        ("offline", False),
    ],
)
def test_channel_keyboard_follows_inventory_status(status, has_book):
    markup = build_channel_keyboard(dict(ACTIONS), inventory_status=status)
    labels = _button_labels(markup)
    assert labels[:2] == ["🏠 房源详情", "📸 更多实拍"]
    assert ("📅 预约看房" in labels) is has_book


def test_reserved_channel_status_uses_single_public_semantics():
    assert inventory_status_presentation("reserved") == ("🟡", "已有预约 · 仍可预约")
    assert appointment_status_label("reserved", 1) == "🟡 已有预约 · 仍可预约"
    caption = caption_with_appointment_status(
        "🏡 富力城｜2房1厅\n\n🟢 当前可预约　QL-RF-A2B3",
        status="reserved",
        active_count=1,
        public_listing_id="QL-RF-A2B3",
    )
    assert "🟡 已有预约 · 仍可预约　QL-RF-A2B3" in caption


def _seed_today_date_db(db_path):
    initialize_v3_storage(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO listings_v3
               (listing_id,public_listing_id,canonical_record_id,canonical_facts_hash,
                canonical_facts_schema,inventory_status)
               VALUES ('l_today','QL-RF-A2B3','cr_today','hash','v3','active')"""
        )
        for user_id, value in enumerate(("2026-09-10", "09-10", "9月10日"), start=101):
            conn.execute(
                """INSERT INTO appointments_v3
                   (user_id,listing_id,appointment_date,appointment_time,status)
                   VALUES (?,?,?,?,?)""",
                (user_id, "l_today", value, "pm", "pending"),
            )
        conn.commit()


def test_admin_today_appointments_accepts_all_production_date_formats(tmp_path):
    db_path = tmp_path / "dates.sqlite3"
    _seed_today_date_db(db_path)
    rows = AdminAppointmentReader(db_path).list_today(datetime(2026, 9, 10, 12, 0, 0))
    assert {row["appointment_date"] for row in rows} == {
        "2026-09-10",
        "09-10",
        "9月10日",
    }


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("hub", "首页联系我们"),
        ("listing_callback", "房源咨询"),
        ("daily_broadcast", "每日广播咨询"),
        ("user_search", "找房咨询"),
        ("channel", "频道房源"),
        ("channel_deeplink", "频道房源"),
        ("search_result", "找房结果"),
        ("future_internal_slug", "用户咨询"),
    ],
)
def test_admin_contact_source_is_user_facing_only(source, expected):
    assert display_contact_source(source) == expected


def test_search_no_match_has_existing_contact_flow_button():
    intent = SearchSubmitIntent(
        criteria=SearchCriteria(),
        source="user_search",
        goal="any",
        area_display="BKK1",
        budget_label="$800以内",
        touch_payload={},
    )
    view = build_search_no_match_view(intent)
    choices = [choice for row in view.rows for choice in row]
    contact = next(choice for choice in choices if choice.label == "💬 联系我们")
    assert encode_transition_choice(contact) == "v3u:home:contact"


class _FakeQuery:
    def __init__(self, data):
        self.data = data
        self.edits = []

    async def answer(self, *args, **kwargs):
        return None

    async def edit_message_text(self, text, **kwargs):
        self.edits.append((text, kwargs))


class _ModeReader:
    def __init__(self, mode):
        self.mode = mode

    def list_today(self):
        return ()

    def get(self, appointment_id):
        assert appointment_id == 7
        return {
            "id": 7,
            "display_name": "张女士",
            "public_listing_id": "QL-RF-A2B3",
            "appointment_date": "09-10",
            "appointment_time": "pm",
            "viewing_mode": self.mode,
            "contact_value": "@guest",
            "status": "pending",
        }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "expected"),
    [("offline", "方式：实地看房"), ("video", "方式：实时视频看房")],
)
async def test_admin_appointment_mode_is_human_readable(mode, expected):
    query = _FakeQuery("adminq:appointment:7")
    await handle_admin_callback(
        SimpleNamespace(callback_query=query),
        _ModeReader(mode),
    )
    assert expected in query.edits[-1][0]
    assert f"方式：{mode}" not in query.edits[-1][0]


def test_published_public_id_fallback_uses_real_snapshot_only():
    resolved = AdminAppointmentReader._public_row(
        {
            "public_listing_id": "",
            "published_snapshot_json": json.dumps(
                {"public_listing_id": "QL-RF-A2B3"}, ensure_ascii=False
            ),
        }
    )
    assert resolved["public_listing_id"] == "QL-RF-A2B3"

    unresolved = AdminAppointmentReader._public_row(
        {"public_listing_id": "", "published_snapshot_json": "{}"}
    )
    assert not str(unresolved.get("public_listing_id") or "").strip()


def test_rfcity_category_returns_to_rfcity_navigation():
    home = rfcity_home_view()
    category = rfcity_category_view("restaurant")
    assert "富力生活导航" in home.text
    assert len(category.rows) == 1
    assert category.rows[0][0].label == "⬅️ 返回富力导航"
    assert category.rows[0][0].callback_data == "v3u:service:rfcity"


def test_missing_advisor_url_uses_existing_bot_contact_callback():
    view = build_contact_view(advisor_url="")
    markup = build_home_keyboard(view)
    assert markup is not None
    button = markup.inline_keyboard[0][0]
    assert button.text == "💬 联系我们"
    assert not button.url
    assert button.callback_data == "v3u:home:contact"


def test_missing_channel_url_does_not_create_dead_latest_listing_button():
    view = build_home_view(channel_url="")
    markup = build_home_keyboard(view)
    assert markup is not None
    labels = _button_labels(markup)
    assert "🏠 最新房源" not in labels
    assert "💬 联系我们" in labels


class _EditBot:
    def __init__(self):
        self.calls = []

    async def edit_message_caption(self, **kwargs):
        self.calls.append(kwargs)


def _seed_published_message(db_path):
    initialize_v3_storage(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO listings_v3
               (listing_id,public_listing_id,canonical_record_id,canonical_facts_hash,
                canonical_facts_schema,inventory_status)
               VALUES ('l_sync','QL-RF-A2B3','cr_sync','hash','v3','rented')"""
        )
        conn.execute(
            """INSERT INTO publication_instances
               (instance_id,package_id,listing_id,offer_id,platform,channel_chat_id,
                channel_message_id,publish_status,post_text,published_at,updated_at)
               VALUES ('PUB_SYNC','PKG_SYNC','l_sync','OFF_SYNC','telegram','-100123','333',
                       'published','🏡 富力城｜2房1厅\n\n🟢 当前可预约　QL-RF-A2B3',
                       '2026-09-09 10:00:00','2026-09-09 10:00:00')"""
        )
        conn.commit()


@pytest.mark.asyncio
async def test_published_status_edit_updates_caption_and_removes_booking(tmp_path):
    db_path = tmp_path / "sync.sqlite3"
    _seed_published_message(db_path)
    bot = _EditBot()
    result = await sync_published_listing_status(
        bot=bot,
        db_path=db_path,
        user_bot_username="QiaolianBot",
        listing_id="l_sync",
        status="rented",
    )
    assert result.synced
    assert len(bot.calls) == 1
    call = bot.calls[0]
    assert "🔴 已租出　QL-RF-A2B3" in call["caption"]
    assert "📅 预约看房" not in _button_labels(call["reply_markup"])
    with sqlite3.connect(db_path) as conn:
        saved = conn.execute(
            "SELECT post_text FROM publication_instances WHERE instance_id='PUB_SYNC'"
        ).fetchone()[0]
    assert "🔴 已租出　QL-RF-A2B3" in saved


def test_publisher_status_callback_keeps_channel_sync_glue_registered():
    source = Path("v3_core/publishing/publisher_app.py").read_text(encoding="utf-8")
    assert 'raw.startswith("v3smp|status|")' in source
    assert "sync_published_listing_status(" in source

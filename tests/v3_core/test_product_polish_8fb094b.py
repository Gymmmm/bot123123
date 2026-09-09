from __future__ import annotations

from datetime import datetime
import json
import sqlite3
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from v3_core.publishing.channel_renderer import render_channel_caption
from v3_core.publishing.telegram_adapter import build_channel_keyboard
from v3_core.user_bot.admin_appointments import (
    AdminAppointmentReader,
    build_admin_appointment_detail_text,
)
from v3_core.user_bot.home_views import build_contact_view, build_home_view
from v3_core.user_bot.listing_responses import build_details_response
from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.search_no_match_view import build_search_no_match_view
from v3_core.user_bot.search_query import SearchCriteria
from v3_core.user_bot.source_display import source_display_label
from v3_core.user_bot.telegram_home_ui import encode_home_choice
from v3_core.user_bot.telegram_listing_callback import _render_contact
from v3_core.user_bot.telegram_service_handler import _rfcity_category_product_view
from v3_core.user_bot.transition_actions import SearchSubmitIntent


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
        ("rented", False),
        ("inactive", False),
        ("offline", False),
    ],
)
def test_channel_keyboard_booking_follows_inventory_status(status, has_book):
    labels = _button_labels(build_channel_keyboard(dict(ACTIONS), inventory_status=status))
    assert ("📅 预约看房" in labels) is has_book
    assert "🏠 房源详情" in labels
    assert "📸 更多实拍" in labels


def test_channel_reserved_status_uses_locked_user_visible_semantics():
    caption = render_channel_caption(
        listing={
            "project_name": "富力城",
            "layout": "1房",
            "property_type": "公寓",
            "inventory_status": "reserved",
        },
        offer={"offer_type": "rent", "monthly_rent_usd": 680},
        public_listing_id="QL-RF-A2B3",
    )
    assert "🟡 已有预约 · 仍可预约" in caption
    assert "🟢 当前可预约" not in caption


def _make_admin_db(path):
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE listings_v3(
                listing_id TEXT PRIMARY KEY,
                public_listing_id TEXT,
                display_title TEXT,
                project_name TEXT
            );
            CREATE TABLE appointments_v3(
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                username TEXT,
                display_name TEXT,
                listing_id TEXT,
                viewing_mode TEXT,
                appointment_date TEXT,
                appointment_time TEXT,
                contact_value TEXT,
                note TEXT,
                status TEXT,
                created_at TEXT,
                updated_at TEXT
            );
            INSERT INTO listings_v3 VALUES ('l_1','QL-RF-A2B3','富力城一房','富力城');
            INSERT INTO appointments_v3 VALUES
                (1,1,'u1','A','l_1','offline','2026-09-09','am','@u1','','pending','',''),
                (2,2,'u2','B','l_1','video','09-09','pm','@u2','','pending','',''),
                (3,3,'u3','C','l_1','offline','9月9日','evening','@u3','','pending','',''),
                (4,4,'u4','D','l_1','offline','09-10','am','@u4','','pending','','');
            """
        )


def test_admin_today_appointments_accept_all_three_existing_date_formats(tmp_path):
    db_path = tmp_path / "admin.sqlite3"
    _make_admin_db(db_path)
    reader = AdminAppointmentReader(db_path)
    now = datetime(2026, 9, 9, 12, 0, tzinfo=ZoneInfo("Asia/Phnom_Penh"))
    rows = reader.list_today(now)
    assert {row["id"] for row in rows} == {1, 2, 3}
    assert {row["appointment_date"] for row in rows} == {"2026-09-09", "09-09", "9月9日"}
    assert all(row["public_listing_id"] == "QL-RF-A2B3" for row in rows)


def test_admin_appointment_mode_is_human_readable_and_real_public_id_is_used():
    base = {
        "display_name": "A",
        "public_listing_id": "QL-RF-A2B3",
        "appointment_date": "09-09",
        "appointment_time": "am",
        "contact_value": "@u",
        "status": "pending",
    }
    offline = build_admin_appointment_detail_text({**base, "viewing_mode": "offline"})
    video = build_admin_appointment_detail_text({**base, "viewing_mode": "video"})
    assert "房源：QL-RF-A2B3" in offline
    assert "方式：实地看房" in offline
    assert "方式：实时视频看房" in video
    assert "方式：offline" not in offline
    assert "方式：video" not in video


@pytest.mark.parametrize(
    ("source", "label"),
    [
        ("hub", "首页联系我们"),
        ("listing_callback", "房源咨询"),
        ("daily_broadcast", "每日广播咨询"),
        ("unknown_slug", "用户咨询"),
    ],
)
def test_source_display_mapping_is_human_readable(source, label):
    assert source_display_label(source) == label


def test_no_match_page_has_existing_contact_flow_action():
    intent = SearchSubmitIntent(
        criteria=SearchCriteria(location_keys=("BKK1",), budget_max=800),
        source="user_search",
        goal="any",
        area_display="BKK1",
        budget_label="$800以内",
        touch_payload={},
    )
    view = build_search_no_match_view(intent)
    choices = [choice for row in view.rows for choice in row]
    contact = next(choice for choice in choices if choice.label == "💬 联系我们")
    assert contact.kind == "home"
    assert contact.value == "contact"


def _published_view() -> PublishedListingView:
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "canonical_facts": {},
        "listing": {
            "project_name": "富力城",
            "property_type": "公寓",
            "layout": "1房",
            "public_location_display": "富力城",
            "size_sqm": 45,
            "floor": "8",
        },
        "offer": {
            "monthly_rent_usd": 680,
            "payment_terms": "押1付1",
            "contract_term": "1年",
        },
    }
    return PublishedListingView(
        listing={
            "listing_id": "l_1",
            "public_listing_id": "QL-RF-A2B3",
            "inventory_status": "active",
        },
        offer={
            "offer_status": "active",
            "offer_type": "rent",
            "publication_policy": "telegram_rent",
        },
        publication={},
        package={"snapshot_json": json.dumps(snapshot, ensure_ascii=False), "gallery_json": "[]"},
    )


def test_details_labels_public_id_as_listing_number_not_photo_field():
    text = build_details_response(_published_view()).text
    assert "🆔 房源编号：QL-RF-A2B3" in text
    assert "📸 实拍：QL-RF-A2B3" not in text


def test_rfcity_category_returns_to_rfcity_navigation():
    view = _rfcity_category_product_view("restaurant")
    assert view.rows[-1][0].label == "⬅️ 返回富力导航"
    assert view.rows[-1][0].callback_data == "v3u:service:rfcity"


def test_missing_channel_url_does_not_create_latest_listing_url_button():
    view = build_home_view(channel_url="")
    choices = [choice for row in view.rows for choice in row]
    assert all(choice.label != "🏠 最新房源" for choice in choices)
    assert any(choice.label == "💬 联系我们" for choice in choices)


def test_missing_advisor_url_uses_internal_contact_callback_not_dead_url():
    view = build_contact_view(advisor_url="")
    first = view.rows[0][0]
    assert first.url == ""
    button = encode_home_choice(first)
    assert button.url is None
    assert button.callback_data == "v3u:home:contact"


class _FakeContactQuery:
    def __init__(self):
        self.message = SimpleNamespace(photo=None)
        self.kwargs = None

    async def edit_message_text(self, text, **kwargs):
        self.kwargs = kwargs


@pytest.mark.asyncio
async def test_listing_contact_missing_advisor_url_uses_existing_bot_contact_flow():
    query = _FakeContactQuery()
    await _render_contact(
        query,
        text="ok",
        public_listing_id="QL-RF-A2B3",
        advisor_url="",
    )
    first = query.kwargs["reply_markup"].inline_keyboard[0][0]
    assert first.url is None
    assert first.callback_data == "v3u:home:contact"

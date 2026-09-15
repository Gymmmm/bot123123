from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from v3_core.publishing.manual_status_sync import (
    PublisherManualStatusSynchronizer,
    caption_with_inventory_status,
)
from v3_core.user_bot.admin_appointments import AdminAppointmentReader
from v3_core.user_bot.appointment_runtime_effects import _admin_appointment_keyboard
from v3_core.user_bot.telegram_service_handler import (
    _service_home_with_tenant_entry,
    _tenant_binding_view,
)


def _callbacks(markup):
    return [
        button.callback_data
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data
    ]


def test_round2_admin_notification_keyboard_exposes_operational_actions():
    markup = _admin_appointment_keyboard(17, 12345)
    assert _callbacks(markup) == [
        "adminq:appointment:confirm:17",
        "adminq:appointment:contacted:17",
        "adminq:appointment:cancel:17",
        "adminq:appointment:17",
    ]
    assert markup.inline_keyboard[-1][0].url == "tg://user?id=12345"


def test_round2_tenant_service_entry_and_binding_views():
    home = _service_home_with_tenant_entry()
    callbacks = [choice.callback_data for row in home.rows for choice in row]
    assert "v3u:service:tenant" in callbacks

    missing = _tenant_binding_view(
        SimpleNamespace(repository=SimpleNamespace(get_active_binding=lambda user_id: None)),
        99,
    )
    assert missing.kind == "tenant_binding_missing"
    assert "没有绑定" in missing.text

    bound = _tenant_binding_view(
        SimpleNamespace(
            repository=SimpleNamespace(
                get_active_binding=lambda user_id: SimpleNamespace(property_name="富力城 A2-1908")
            )
        ),
        99,
    )
    assert bound.kind == "tenant_binding"
    assert "富力城 A2-1908" in bound.text
    bound_callbacks = [choice.callback_data for row in bound.rows for choice in row]
    assert "v3u:service:repair" in bound_callbacks
    assert "v3u:service:property" in bound_callbacks


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
            INSERT INTO listings_v3 VALUES ('l_1','QL-RF-A2B3','富力城｜2房','富力城');
            INSERT INTO appointments_v3 VALUES
                (1,123,'alice','Alice','l_1','offline','09-11','am','@alice','',
                 'pending','2026-09-10 12:00:00','2026-09-10 12:00:00');
            """
        )


def test_round2_admin_status_transitions_do_not_downgrade_confirmed(tmp_path):
    db_path = tmp_path / "admin.sqlite3"
    _make_admin_db(db_path)
    reader = AdminAppointmentReader(db_path)

    row, changed = reader.update_status(1, "contacted")
    assert changed and row["status"] == "contacted"

    row, changed = reader.update_status(1, "confirmed")
    assert changed and row["status"] == "confirmed"

    row, changed = reader.update_status(1, "contacted")
    assert not changed and row["status"] == "confirmed"

    row, changed = reader.update_status(1, "cancelled")
    assert changed and row["status"] == "cancelled"


def test_round2_manual_channel_caption_status_replaces_old_status_once():
    caption = "🏡 富力城｜2房\n\n💰 $800/月\n\n🟢 当前可预约　QL-RF-A2B3"
    updated = caption_with_inventory_status(
        caption,
        status="rented",
        public_listing_id="QL-RF-A2B3",
    )
    assert "🔴 已租出　QL-RF-A2B3" in updated
    assert "🟢 当前可预约" not in updated
    assert updated.count("QL-RF-A2B3") == 1


class _FakeBot:
    def __init__(self):
        self.calls = []

    async def edit_message_caption(self, **kwargs):
        self.calls.append(kwargs)


@pytest.mark.asyncio
async def test_round2_manual_status_sync_edits_exact_published_post_and_hides_booking(tmp_path):
    db_path = tmp_path / "publisher.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE listings_v3(
                listing_id TEXT PRIMARY KEY,
                public_listing_id TEXT
            );
            CREATE TABLE publication_instances(
                id INTEGER PRIMARY KEY,
                listing_id TEXT,
                platform TEXT,
                publish_status TEXT,
                channel_chat_id TEXT,
                channel_message_id TEXT,
                post_text TEXT,
                updated_at TEXT
            );
            INSERT INTO listings_v3 VALUES ('l_1','QL-RF-A2B3');
            INSERT INTO publication_instances VALUES
                (7,'l_1','telegram','published','-100123','456',
                 '🏡 富力城｜2房\n\n💰 $800/月\n\n🟢 当前可预约　QL-RF-A2B3',
                 '2026-09-10 12:00:00');
            """
        )
    bot = _FakeBot()
    sync = PublisherManualStatusSynchronizer(db_path, user_bot_username="qiaolian_rent_bot")

    result = await sync.sync(bot, listing_id="l_1", status="rented")

    assert result.attempted and result.synced
    assert result.channel_chat_id == "-100123"
    assert result.channel_message_id == "456"
    assert len(bot.calls) == 1
    call = bot.calls[0]
    assert call["chat_id"] == "-100123"
    assert call["message_id"] == 456
    assert "🔴 已租出　QL-RF-A2B3" in call["caption"]
    labels = [button.text for row in call["reply_markup"].inline_keyboard for button in row]
    assert "📅 预约看房" not in labels

    with sqlite3.connect(db_path) as conn:
        stored = conn.execute("SELECT post_text FROM publication_instances WHERE id=7").fetchone()[0]
    assert "🔴 已租出　QL-RF-A2B3" in stored

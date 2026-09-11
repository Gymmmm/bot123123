from __future__ import annotations

from v3_core.user_bot.appointment_history import AppointmentHistoryView
from v3_core.user_bot.home_callbacks import encode_home_callback, parse_home_callback
from v3_core.user_bot.home_views import (
    build_appointment_history_home_view,
    build_contact_view,
    build_home_view,
)
from v3_core.user_bot.telegram_home_ui import build_home_keyboard


def _callbacks(markup):
    return [
        button.callback_data
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data
    ]


def test_home_callback_codec_is_v3_only_and_closed_set():
    for action in ("search", "appointments", "rental", "service", "contact"):
        encoded = encode_home_callback(action)
        assert encoded == f"v3u:home:{action}"
        parsed = parse_home_callback(encoded)
        assert parsed is not None and parsed.action == action

    assert parse_home_callback("hub:appointments") is None
    assert parse_home_callback("home_smart_search") is None
    assert parse_home_callback("v3u:home:unknown") is None


def test_full_home_preserves_v3_labels_and_exposes_direct_search_copy():
    view = build_home_view(channel_url="https://t.me/qiaolian")
    markup = build_home_keyboard(view)
    assert markup is not None
    assert "也可以直接发送找房需求" in view.text
    assert "BKK1，预算 $800以内" in view.text
    labels = [button.text for row in markup.inline_keyboard for button in row]
    assert labels == [
        "🔍 帮我找房",
        "📅 我的预约",
        "🛡 侨联保障",
        "🛠 入住服务",
        "🏠 最新房源",
        "💬 联系中文顾问",
    ]
    callbacks = _callbacks(markup)
    assert callbacks == [
        "v3u:home:search",
        "v3u:home:appointments",
        "v3u:home:rental",
        "v3u:home:service",
        "v3u:home:contact",
    ]
    assert markup.inline_keyboard[2][0].url == "https://t.me/qiaolian"
    assert not any(value.startswith("hub:") for value in callbacks)
    assert "home_smart_search" not in callbacks


def test_contact_and_appointment_views_return_only_v3_navigation():
    contact = build_contact_view(advisor_url="https://t.me/advisor")
    contact_markup = build_home_keyboard(contact)
    assert contact_markup is not None
    assert contact.rows[0][0].label == "💬 联系中文顾问"
    assert _callbacks(contact_markup) == ["v3u:home:search", "v3u:t:home"]
    assert contact_markup.inline_keyboard[0][0].url == "https://t.me/advisor"

    history = AppointmentHistoryView(
        text="📅 <b>我的预约</b>",
        items=(),
        history_count=0,
    )
    appointments = build_appointment_history_home_view(
        history,
        advisor_url="https://t.me/advisor",
    )
    appointment_markup = build_home_keyboard(appointments)
    assert appointment_markup is not None
    assert _callbacks(appointment_markup) == ["v3u:home:search", "v3u:t:home"]
    labels = [button.text for row in appointment_markup.inline_keyboard for button in row]
    assert labels == ["🔍 继续找房", "💬 联系中文顾问", "🏠 返回首页"]
    assert appointment_markup.inline_keyboard[1][0].url == "https://t.me/advisor"

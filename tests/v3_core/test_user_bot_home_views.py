from __future__ import annotations

from v3_core.user_bot.appointment_history import AppointmentHistoryView
from v3_core.user_bot.home_callbacks import encode_home_callback, parse_home_callback
from v3_core.user_bot.home_views import build_appointment_history_home_view, build_contact_view, build_home_view
from v3_core.user_bot.telegram_home_ui import build_home_keyboard


def _callbacks(markup):
    return [button.callback_data for row in markup.inline_keyboard for button in row if button.callback_data]


def test_home_callback_codec_is_v3_only_and_closed_set():
    for action in ("search", "appointments", "rental", "service", "contact"):
        encoded = encode_home_callback(action)
        assert encoded == f"v3u:home:{action}"
        parsed = parse_home_callback(encoded)
        assert parsed is not None and parsed.action == action
    assert parse_home_callback("hub:appointments") is None
    assert parse_home_callback("home_smart_search") is None
    assert parse_home_callback("v3u:home:unknown") is None


def test_full_home_is_exact_final_product_surface():
    view = build_home_view(channel_url="https://t.me/qiaolian")
    markup = build_home_keyboard(view)
    assert markup is not None
    labels = [button.text for row in markup.inline_keyboard for button in row]
    assert labels == [
        "🔍 开始找房",
        "📅 我的预约",
        "🛠 入住服务",
        "🏠 关于侨联",
        "💬 中文顾问",
        "📢 房源频道",
    ]
    callbacks = _callbacks(markup)
    assert callbacks == [
        "v3u:home:search",
        "v3u:home:appointments",
        "v3u:home:service",
        "v3u:home:about",
        "v3u:home:contact",
    ]
    assert markup.inline_keyboard[3][0].url == "https://t.me/qiaolian"
    forbidden = {
        "侨联保障", "安心租房", "帮我找房", "联系我们", "联系顾问", "侨联小管家",
        "小彭", "我想换房", "租赁指南", "租赁服务指南",
    }
    assert not any(any(term in label for term in forbidden) for label in labels)


def test_home_advisor_button_records_contact_before_external_handoff():
    markup = build_home_keyboard(
        build_home_view(advisor_url="https://t.me/advisor")
    )
    assert markup is not None
    button = next(
        button
        for row in markup.inline_keyboard
        for button in row
        if button.text == "💬 中文顾问"
    )
    assert button.url is None
    assert button.callback_data == "v3u:home:contact"


def test_contact_and_appointment_views_return_only_v3_navigation():
    contact = build_contact_view(advisor_url="https://t.me/advisor")
    contact_markup = build_home_keyboard(contact)
    assert contact_markup is not None
    assert contact.rows[0][0].label == "💬 打开中文顾问"
    assert _callbacks(contact_markup) == ["v3u:home:search", "v3u:t:home"]
    assert contact_markup.inline_keyboard[0][0].url.startswith("https://t.me/advisor?text=")

    history = AppointmentHistoryView(text="📅 <b>我的预约</b>", items=(), history_count=0)
    appointments = build_appointment_history_home_view(history)
    appointment_markup = build_home_keyboard(appointments)
    assert appointment_markup is not None
    assert _callbacks(appointment_markup) == ["v3u:home:search", "v3u:t:home"]
    labels = [button.text for row in appointment_markup.inline_keyboard for button in row]
    assert labels == ["🔍 继续找房", "🏠 返回首页"]

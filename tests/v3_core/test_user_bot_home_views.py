
from v3_core.user_bot.appointment_history import AppointmentHistoryView
from v3_core.user_bot.home_callbacks import encode_home_callback, parse_home_callback
from v3_core.user_bot.home_views import build_appointment_history_home_view, build_contact_view, build_home_view
from v3_core.user_bot.telegram_home_ui import build_home_keyboard


def _callbacks(markup):
    return [b.callback_data for row in markup.inline_keyboard for b in row if b.callback_data]


def test_home_callback_codec_remains_v3_only():
    for action in ("search", "appointments", "rental", "service", "contact"):
        encoded = encode_home_callback(action)
        parsed = parse_home_callback(encoded)
        assert encoded == f"v3u:home:{action}"
        assert parsed is not None and parsed.action == action
    assert parse_home_callback("hub:appointments") is None


def test_home_is_final_minimal_surface():
    view = build_home_view(
        channel_url="https://t.me/qiaolian",
        first_name="Gym",
        greeting="晚上好",
    )
    markup = build_home_keyboard(view)
    labels = [b.text for row in markup.inline_keyboard for b in row]
    assert labels == ["🔍 开始找房", "🛎️ 侨联服务", "📢 最新房源", "💬 中文顾问"]
    assert _callbacks(markup) == ["v3u:home:search", "v3u:home:service", "v3u:home:contact"]
    assert markup.inline_keyboard[1][0].url == "https://t.me/qiaolian"
    assert "Gym" in view.text
    assert "晚上好" in view.text


def test_home_without_channel_keeps_other_three_buttons():
    markup = build_home_keyboard(
        build_home_view(channel_url="", first_name="Gym", greeting="上午好")
    )
    labels = [b.text for row in markup.inline_keyboard for b in row]
    assert labels == ["🔍 开始找房", "🛎️ 侨联服务", "💬 中文顾问"]
    assert _callbacks(markup) == ["v3u:home:search", "v3u:home:service", "v3u:home:contact"]


def test_contact_handoff_and_appointment_history_navigation_are_plain_text():
    contact = build_contact_view(advisor_url="https://t.me/advisor")
    markup = build_home_keyboard(contact)
    assert contact.rows[0][0].label == "中文顾问"
    assert markup.inline_keyboard[0][0].url.startswith("https://t.me/advisor?text=")
    assert _callbacks(markup) == ["v3u:home:search", "v3u:t:home"]

    history = AppointmentHistoryView(text="<b>我的预约</b>", items=(), history_count=0)
    appointment = build_appointment_history_home_view(history)
    labels = [b.text for row in build_home_keyboard(appointment).inline_keyboard for b in row]
    assert labels == ["🔍 开始找房", "💬 中文顾问", "⬅️ 返回首页"]

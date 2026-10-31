"""V4.0 UX: service hub, 视频带看, 账单疑问 wiring and error copy."""
import pytest

from v3_core.user_bot.public_appointment import PublicAppointmentDraft
from v3_core.user_bot.service_views import (
    service_home_view,
    utility_stub_view,
    video_service_view,
)
from v3_core.user_bot.telegram_callback_handler import (
    BUTTON_EXPIRED_TEXT,
    NO_SIMILAR_TEXT,
    _error_alert,
    no_similar_view,
)
from v3_core.user_bot.telegram_service_handler import handle_v3_service_callback
from v3_core.user_bot.telegram_transition_action_handler import booking_failed_view, search_failed_view
from v3_core.user_bot.transition_callbacks import encode_transition_choice

from test_user_bot_telegram_service_handler import FakeQuery, _callback_update, _context, _service

BANNED = ("入住有人管", "侨联提醒", "亲", "您好", "尊敬")


def _labels(view):
    return [c.label for row in view.rows for c in row]


def _callbacks(view):
    return [c.callback_data for row in view.rows for c in row if c.callback_data]


def _encoded(view):
    return [encode_transition_choice(c) for row in view.rows for c in row]


def test_service_hub_has_video_entry_and_v4_copy():
    view = service_home_view()
    assert "租房不只是找到房子" in view.text
    assert _labels(view)[0] == "🎥 视频带看"
    assert _callbacks(view)[0] == "v3u:service:video"
    assert all(term not in view.text + "".join(_labels(view)) for term in BANNED)


def test_video_service_view_routes_to_search_contact_and_back():
    view = video_service_view()
    assert "视频带看" in view.text
    assert _labels(view) == ["🔍 开始找房", "💬 中文顾问", "⬅️ 返回侨联服务"]
    callbacks = _callbacks(view)
    assert "v3u:home:search" in callbacks and "v3u:home:service" in callbacks
    assert all(term not in view.text for term in BANNED)


@pytest.mark.asyncio
async def test_video_callback_renders_video_view_in_place(tmp_path):
    query = FakeQuery("v3u:service:video")
    outcome = await handle_v3_service_callback(_callback_update(query), _context(), service=_service(tmp_path, binding=False))
    assert outcome.handled
    edits = [c for c in query.calls if c[0] in {"edit_text", "edit_caption"}]
    assert edits and "视频带看" in str(edits[-1])


def test_bill_question_uses_existing_utilities_handler_with_v4_label():
    view = utility_stub_view("utilities")
    assert "账单" in view.text
    assert "⬅️ 返回租后服务" in _labels(view)


def test_error_copy_views_are_plain_and_encodable():
    assert BUTTON_EXPIRED_TEXT == "这个入口已经更新，请返回重新选择。"
    from types import SimpleNamespace
    assert _error_alert(SimpleNamespace(status="weird")) == BUTTON_EXPIRED_TEXT
    assert "不再展示" in _error_alert(SimpleNamespace(status="not_found"))

    similar = no_similar_view()
    assert NO_SIMILAR_TEXT in similar.text
    assert _encoded(similar) == ["v3u:home:contact", "v3u:change_search"]

    failed = search_failed_view()
    assert "没能完成搜索" in failed.text
    assert _encoded(failed) == ["v3u:change_search", "v3u:home:contact"]

    booking = booking_failed_view(PublicAppointmentDraft("QL-RF-A2B3"))
    assert "预约暂时没提交成功" in booking.text
    assert _encoded(booking) == [
        "v3u:t:appointment_submit", "v3u:home:contact", "v3u:listing:details:QL-RF-A2B3",
    ]
    for view in (similar, failed, booking):
        assert all(term not in view.text for term in BANNED)


def test_unrecognized_prompts_use_plain_copy():
    import inspect
    import v3_core.user_bot.transition_text_actions as mod
    source = inspect.getsource(mod)
    for phrase in ("我还没看懂你说的位置", "我还没看懂你的预算", "没看懂具体时间", "没看懂具体日期"):
        assert phrase in source

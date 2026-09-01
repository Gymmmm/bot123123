from __future__ import annotations

import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from qiaolian_dual import listing
from qiaolian_dual.common import APPT_DATE
from qiaolian_dual.appointment_ui import _appointment_date_keyboard
from qiaolian_dual.keyboards_common import appointment_success_keyboard
from qiaolian_dual.messages import advisor_response_notice_text
from qiaolian_dual.texts import advisor_handoff_text, advisor_text
from qiaolian_dual.flows import start_appointment
from qiaolian_dual.results_admin import send_listing_photo_preview
from qiaolian_dual.session_deeplink import parse_start_arg_payload
from qiaolian_dual.start_routes import route_start_arg
from qiaolian_dual.utils_formatting import _internal_listing_id


def _labels(markup):
    return [[button.text for button in row] for row in markup.inline_keyboard]


def test_property_deep_links_keep_id_and_direct_action():
    for suffix, action in (("details", "details"), ("photos", "photos"), ("book", "book")):
        payload = parse_start_arg_payload(f"property_QC0002_{suffix}")
        assert payload["target"] == "QC0002"
        assert payload["action"] == action
        assert _internal_listing_id(payload["target"]) == "l_2"


@pytest.mark.asyncio
async def test_public_qc_detail_route_resolves_to_internal_listing_id():
    reply = AsyncMock()
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=7, username="tester", first_name="Test"),
        effective_message=SimpleNamespace(reply_text=reply),
    )
    context = SimpleNamespace(user_data={})
    seen = []
    with (
        patch("qiaolian_dual.listing.listing_action_allowed", side_effect=lambda lid, action: (seen.append((lid, action)) or (True, "reserved"))),
        patch("qiaolian_dual.listing.listing_cost_text", side_effect=lambda lid: f"details:{lid}"),
        patch("qiaolian_dual.listing.listing_cost_keyboard", return_value=None),
    ):
        await route_start_arg(update, context, "property_QC0002_details", create_lead_fn=lambda *args, **kwargs: 1)
    assert seen == [("l_2", "detail")]
    assert reply.await_args.args[0] == "details:l_2"


def test_channel_cta_is_fixed_and_has_no_helper(monkeypatch):
    import meihua_publisher

    monkeypatch.setattr(meihua_publisher, "BOT_USERNAME", "QiaolianBot")
    markup = meihua_publisher.build_keyboard("QC0002")
    assert _labels(markup) == [["📋 租赁详情", "📸 更多实拍"], ["📅 预约看房"]]
    urls = [button.url for row in markup.inline_keyboard for button in row]
    assert urls == [
        "https://t.me/QiaolianBot?start=property_QC0002_details",
        "https://t.me/QiaolianBot?start=property_QC0002_photos",
        "https://t.me/QiaolianBot?start=property_QC0002_book",
    ]


def test_detail_hides_empty_fields_and_uses_required_weights(monkeypatch):
    monkeypatch.setattr(listing, "listing_context", lambda _lid: {
        "listing_id": "QC0002", "project": "永旺1", "layout": "1房",
        "price": 1800, "deposit": "", "water_rate": "未知",
        "electric_rate": "--", "floor": "23楼", "status": "reserved",
    })
    text = listing.listing_cost_text("QC0002")
    assert "📋 <b>租赁详情</b>" in text
    assert "💰 <b>租金：$1,800/月</b>" in text
    assert "🏢 <b>楼层：</b> 23楼" in text
    assert "<b>房态：</b> 已有预约 · 仍可预约" in text
    assert "押付" not in text and "水费" not in text and "电费" not in text
    assert all(token not in text for token in ("[暂无]", "未知", "--"))


@pytest.mark.asyncio
async def test_appointment_starts_at_date_and_video_goes_to_video_date():
    context = SimpleNamespace(user_data={})
    render = AsyncMock()
    item = {"listing_id": "QC0002", "project": "永旺1", "layout": "1房", "price": 1800}
    with (
        patch("qiaolian_dual.listing.listing_is_available", return_value=(True, "active")),
        patch("qiaolian_dual.listing.listing_context", return_value=item),
    ):
        state = await start_appointment(SimpleNamespace(), context, "QC0002", render_panel_fn=render)
    assert state == APPT_DATE
    assert "请选择方便看房的日期" in render.await_args.kwargs["text"]
    assert _labels(render.await_args.kwargs["reply_markup"])[-1] == ["🎥 实时视频看房"]
    assert context.user_data["appt"]["mode"] == "offline"

    video_context = SimpleNamespace(user_data={})
    video_render = AsyncMock()
    with (
        patch("qiaolian_dual.listing.listing_is_available", return_value=(True, "active")),
        patch("qiaolian_dual.listing.listing_context", return_value=item),
    ):
        video_state = await start_appointment(SimpleNamespace(), video_context, "QC0002", initial_mode="video", render_panel_fn=video_render)
    assert video_state == APPT_DATE
    assert "请选择方便看房的日期" in video_render.await_args.kwargs["text"]
    assert "实时视频看房" in video_render.await_args.kwargs["text"]
    assert "🎥 实时视频看房" not in sum(_labels(video_render.await_args.kwargs["reply_markup"]), [])


@pytest.mark.asyncio
async def test_album_has_no_summary_and_one_action_box():
    bot = SimpleNamespace(send_media_group=AsyncMock(), send_photo=AsyncMock(), send_message=AsyncMock())
    with tempfile.TemporaryDirectory() as directory:
        paths = []
        for name in ("one.jpg", "two.jpg"):
            path = Path(directory) / name
            path.write_bytes(b"x")
            paths.append(str(path))
        with patch("qiaolian_dual.listing.listing_context", return_value={"media_files": paths, "project": "不应重复"}):
            await send_listing_photo_preview(bot, 1, "QC0002")
    assert bot.send_media_group.await_count == 1
    media = bot.send_media_group.await_args.kwargs["media"]
    assert all(item.caption is None for item in media)
    assert bot.send_message.await_count == 1
    text = bot.send_message.await_args.kwargs["text"]
    assert "不应重复" not in text
    assert _labels(bot.send_message.await_args.kwargs["reply_markup"]) == [["📅 预约看房", "📋 租赁详情"], ["💬 咨询顾问"]]


@pytest.mark.asyncio
async def test_album_empty_copy_keeps_actions():
    bot = SimpleNamespace(send_media_group=AsyncMock(), send_photo=AsyncMock(), send_message=AsyncMock())
    with patch("qiaolian_dual.listing.listing_context", return_value={"media_files": []}):
        await send_listing_photo_preview(bot, 1, "QC0002")
    assert bot.send_message.await_count == 1
    assert "这套房源目前的实拍已经全部显示。" in bot.send_message.await_args.kwargs["text"]
    assert bot.send_message.await_args.kwargs["reply_markup"] is not None


def test_date_keyboard_has_video_branch_without_mode_gate():
    labels = _labels(_appointment_date_keyboard())
    assert labels == [["今天", "明天"], ["后天", "📅 其他日期"], ["🎥 实时视频看房"]]


def test_new_appointment_ui_has_no_focus_or_contact_entry():
    callback_values = [
        button.callback_data
        for row in _appointment_date_keyboard().inline_keyboard
        for button in row
    ]
    assert not any((value or "").startswith("apfocus:") for value in callback_values)
    assert "apedit:contact" not in callback_values


def test_submit_and_advisor_states_are_distinct_and_consistent(monkeypatch):
    monkeypatch.setattr("qiaolian_dual.listing.listing_context", lambda _lid: {
        "area": "永旺1", "layout": "1房", "price": 1800,
    })
    assert "顾问已收到" in advisor_text()
    assert "顾问已收到" in advisor_handoff_text(listing_id="QC0002")
    assert "顾问已接手" in advisor_response_notice_text()
    assert "顾问已收到" not in advisor_response_notice_text()


def test_action_buttons_are_plain_text_without_html():
    markups = [
        _appointment_date_keyboard(),
        appointment_success_keyboard(),
        listing.listing_cost_keyboard("QC0002"),
    ]
    for markup in markups:
        for row in markup.inline_keyboard:
            for button in row:
                assert all(tag not in button.text for tag in ("<b>", "</b>", "<code>", "</code>"))

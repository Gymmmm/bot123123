from __future__ import annotations

import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import meihua_publisher

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
    monkeypatch.setattr(meihua_publisher, "BOT_USERNAME", "QiaolianBot")
    markup = meihua_publisher.build_keyboard("QC0002")
    assert _labels(markup) == [["🏠 房源详情", "📸 更多实拍"], ["📅 预约看房"]]
    urls = [button.url for row in markup.inline_keyboard for button in row]
    assert urls == [
        "https://t.me/QiaolianBot?start=property_QC0002_details",
        "https://t.me/QiaolianBot?start=property_QC0002_photos",
        "https://t.me/QiaolianBot?start=property_QC0002_book",
    ]


def test_channel_caption_has_no_legacy_inline_action_links():
    text = meihua_publisher.build_chinese_listing_post({
        "listing_id": "l_2", "area": "永旺1", "project": "永旺一",
        "layout": "1房1办公2卫", "property_type": "公寓", "price": 1800,
    })
    assert "<a href=" not in text
    assert "💬 问这套" not in text


def test_channel_caption_uses_confirmed_compact_post_format():
    text = meihua_publisher.build_chinese_listing_post({
        "listing_id": "l_5", "project": "钻石岛", "area": "钻石岛",
        "layout": "2房2卫", "property_type": "公寓", "size_sqm": 75,
        "floor": "18/35楼", "price": 680, "deposit_rule": "押2付1",
        "contract_term": "1年", "highlights": ["家具家电齐全", "含物业"],
        "available_date": "随时入住", "status": "reserved",
    })
    assert text.splitlines() == [
        "🏠 <b>钻石岛｜2房2卫</b>", "", "💰 <b>$680/月</b> 📐 <b>75㎡｜18/35楼</b>",
        "📄 <b>押付/合同：押2付1｜1年</b>", "📅 <b>可入住：随时入住</b>", "",
        "🟡 <b>已有预约 · 仍可预约</b> 🆔 <b>QJ0005</b>", "", "<b>#钻石岛 #两房 #金边租房</b>",
    ]
    assert "　" not in text
    assert "\n\n\n" not in text


def test_channel_and_cover_hide_generic_marketing_labels():
    listing_item = {
        "listing_id": "l_38", "project": "侨联地产", "area": "洪森大道",
        "layout": "4房", "price": 1300, "highlights": ["侨联精选", "拠包入住"], "status": "reserved",
    }
    caption = meihua_publisher.build_chinese_listing_post(listing_item)
    cover = meihua_publisher.build_cover_listing_data(listing_item)
    assert "🏠 <b>洪森大道｜4房</b>" in caption
    assert "侨联精选" not in caption
    assert "拠包入住" not in caption
    assert cover["project"] == "洪森大道"
    assert "侨联精选" not in cover.get("highlights", [])


def test_detail_hides_empty_fields_and_uses_required_weights(monkeypatch):
    monkeypatch.setattr(listing, "listing_context", lambda _lid: {
        "listing_id": "QC0002", "project": "永旺1", "layout": "1房",
        "price": 1800, "deposit": "", "water_rate": "未知",
        "electric_rate": "--", "floor": "23楼", "status": "reserved",
    })
    text = listing.listing_cost_text("QC0002")
    assert "🏠 <b>房源详情</b>" in text
    assert "永旺1｜1房" in text
    assert "<b>租金：</b> <b>$1,800/月</b>" in text
    assert "🏢 楼层：23楼" in text
    assert "🟡 房态：已有预约 · 仍可预约" in text
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
    assert "哪天方便看房？" in render.await_args.kwargs["text"]
    assert "📅 <b>预约看房｜QC0002</b>" in render.await_args.kwargs["text"]
    labels = sum(_labels(render.await_args.kwargs["reply_markup"]), [])
    assert "🎥 改为视频看房" in labels
    assert context.user_data["appt"]["mode"] == "offline"

    video_context = SimpleNamespace(user_data={})
    video_render = AsyncMock()
    with (
        patch("qiaolian_dual.listing.listing_is_available", return_value=(True, "active")),
        patch("qiaolian_dual.listing.listing_context", return_value=item),
    ):
        video_state = await start_appointment(SimpleNamespace(), video_context, "QC0002", initial_mode="video", render_panel_fn=video_render)
    assert video_state == APPT_DATE
    assert "哪天方便看房？" in video_render.await_args.kwargs["text"]
    video_labels = sum(_labels(video_render.await_args.kwargs["reply_markup"]), [])
    assert "🎥 改为视频看房" not in video_labels
    assert "🚶 改为实地看房" in video_labels


@pytest.mark.asyncio
async def test_album_has_no_summary_and_one_action_box():
    bot = SimpleNamespace(send_media_group=AsyncMock(), send_photo=AsyncMock(), send_message=AsyncMock())
    with tempfile.TemporaryDirectory() as directory:
        paths = []
        for name in ("one.jpg", "two.jpg"):
            path = Path(directory) / name
            path.write_bytes(b"x")
            paths.append(str(path))
        with patch("qiaolian_dual.listing.listing_context", return_value={"media_files": paths, "project": "不应重复", "status": "active"}):
            await send_listing_photo_preview(bot, 1, "QC0002")
    assert bot.send_media_group.await_count == 1
    media = bot.send_media_group.await_args.kwargs["media"]
    assert all(item.caption is None for item in media)
    assert bot.send_message.await_count == 1
    text = bot.send_message.await_args.kwargs["text"]
    assert "不应重复" not in text
    assert _labels(bot.send_message.await_args.kwargs["reply_markup"]) == [["🏠 房源详情", "📅 预约看房"], ["💬 联系我们"]]


@pytest.mark.asyncio
async def test_album_empty_copy_keeps_actions():
    bot = SimpleNamespace(send_media_group=AsyncMock(), send_photo=AsyncMock(), send_message=AsyncMock())
    with patch("qiaolian_dual.listing.listing_context", return_value={"media_files": []}):
        await send_listing_photo_preview(bot, 1, "QC0002")
    assert bot.send_message.await_count == 1
    assert "这套房的实拍暂时没有加载出来。" in bot.send_message.await_args.kwargs["text"]
    assert bot.send_message.await_args.kwargs["reply_markup"] is not None
    assert _labels(bot.send_message.await_args.kwargs["reply_markup"]) == [["🏠 房源详情", "📅 预约看房"], ["💬 联系我们"]]


def test_date_keyboard_has_video_branch_without_mode_gate():
    labels = _labels(_appointment_date_keyboard())
    assert labels == [["今天", "明天"], ["后天", "📅 其他日期"], ["🎥 改为视频看房"], ["⬅️ 返回房源", "🏠 返回首页"]]


def test_new_appointment_ui_has_no_focus_or_contact_entry():
    callback_values = [button.callback_data for row in _appointment_date_keyboard().inline_keyboard for button in row]
    assert not any((value or "").startswith("apfocus:") for value in callback_values)
    assert "apedit:contact" not in callback_values


def test_submit_and_advisor_states_are_distinct_and_consistent(monkeypatch):
    monkeypatch.setattr("qiaolian_dual.listing.listing_context", lambda _lid: {
        "area": "永旺1", "layout": "1房", "price": 1800,
    })
    assert "联系我们" in advisor_text()
    assert "已记录您咨询的房源" in advisor_handoff_text(listing_id="QC0002")
    assert "顾问已接手" in advisor_response_notice_text()


def test_action_buttons_are_plain_text_without_html():
    markups = [_appointment_date_keyboard(), appointment_success_keyboard(), listing.listing_cost_keyboard("QC0002")]
    for markup in markups:
        for row in markup.inline_keyboard:
            for button in row:
                assert all(tag not in button.text for tag in ("<b>", "</b>", "<code>", "</code>"))

from pathlib import Path
from unittest.mock import AsyncMock, patch
from urllib.parse import unquote

import pytest

from qiaolian_dual.appointment_ui import _appointment_date_keyboard, _appointment_time_keyboard
from qiaolian_dual.keyboards_common import contact_handoff_keyboard
from qiaolian_dual.keyboards_search import appointment_menu_keyboard
from qiaolian_dual.listing import listing_cost_text
from qiaolian_dual.messages import advisor_response_notice_text
from qiaolian_dual.results_admin import send_listing_photo_preview
from qiaolian_dual.session_deeplink import parse_start_arg_payload
from qiaolian_dual.texts import advisor_text, help_text
from v2.qiaolian_publisher_v2.keyboards import publish_post_keyboard


LOCKED_ALBUM_LABELS = ["🏠 房源详情", "📅 预约看房", "💬 联系我们"]


def _buttons(keyboard):
    return [button for row in keyboard.inline_keyboard for button in row]


def _callback_values(keyboard):
    return [button.callback_data for button in _buttons(keyboard) if button.callback_data]


def test_channel_post_has_exact_three_locked_ctas_and_listing_context():
    keyboard = publish_post_keyboard(
        listing_id="l_42",
        area="BKK1",
        user_bot_username="QiaolianTestBot",
        post_token="qlabc123",
    )
    assert [[button.text for button in row] for row in keyboard.inline_keyboard] == [
        ["🏠 房源详情", "📸 更多实拍"],
        ["📅 预约看房"],
    ]
    urls = [unquote(button.url or "") for button in _buttons(keyboard)]
    assert any("start=property_QC0042_details" in url for url in urls)
    assert any("start=property_QC0042_photos" in url for url in urls)
    assert any("start=property_QC0042_book" in url for url in urls)
    assert all("l_42" not in url for url in urls)
    assert all("侨联找房助手" not in button.text for button in _buttons(keyboard))


def test_meihua_publisher_keyboard_uses_same_public_qc_contract():
    import meihua_publisher

    with patch.object(meihua_publisher, "BOT_USERNAME", "@QiaolianTestBot"):
        keyboard = meihua_publisher.build_keyboard(
            "l_42",
            area="BKK1",
            post_token="qlabc123",
            caption_variant="a",
        )

    assert [[button.text for button in row] for row in keyboard.inline_keyboard] == [
        ["🏠 房源详情", "📸 更多实拍"],
        ["📅 预约看房"],
    ]
    urls = [unquote(button.url or "") for button in _buttons(keyboard)]
    assert any("start=property_QC0042_details" in url for url in urls)
    assert any("start=property_QC0042_photos" in url for url in urls)
    assert any("start=property_QC0042_book" in url for url in urls)
    assert all("l_42" not in url for url in urls)


def test_channel_detail_photos_book_payloads_preserve_public_qc_target():
    detail = parse_start_arg_payload("property_QC0042_details")
    photos = parse_start_arg_payload("property_QC0042_photos")
    book = parse_start_arg_payload("property_QC0042_book")

    assert detail == {"action": "details", "target": "QC0042", "post_token": "", "channel_message_id": None}
    assert photos == {"action": "photos", "target": "QC0042", "post_token": "", "channel_message_id": None}
    assert book == {"action": "book", "target": "QC0042", "post_token": "", "channel_message_id": None}


def test_listing_detail_hides_unknown_or_empty_rows():
    listing = {
        "listing_id": "l_42",
        "project": "永旺1",
        "layout": "1房1卫",
        "price": 800,
        "deposit": "",
        "deposit_rule": "待确认",
        "floor": "未知",
        "size": "--",
        "water_rate": "[暂无]",
        "electric_rate": None,
        "status": "active",
        "normalized_data": {
            "contract_term": "-",
            "management_fee": "暂无",
            "internet_fee": "待确认",
            "parking_fee": "未知",
        },
    }
    with patch("qiaolian_dual.listing.listing_context", return_value=listing):
        text = listing_cost_text("l_42")

    assert "永旺1｜1房1卫" in text
    assert "<b>租金：</b> <b>$800/月</b>" in text
    for forbidden in ("[暂无]", "未知", "--", "押付：", "租期：", "面积：", "楼层：", "电费：", "水费：", "物业费：", "网络费：", "停车费："):
        assert forbidden not in text


@pytest.mark.asyncio
async def test_gallery_sends_source_photos_then_one_operation_box(tmp_path):
    paths = []
    for index in range(11):
        path = tmp_path / f"image_{index + 1:02d}.jpg"
        path.write_bytes(f"photo-{index}".encode())
        paths.append(str(path))

    bot = type("Bot", (), {})()
    bot.send_media_group = AsyncMock()
    bot.send_photo = AsyncMock()
    bot.send_message = AsyncMock()

    with patch("qiaolian_dual.listing.listing_context", return_value={"media_files": paths, "status": "active"}):
        await send_listing_photo_preview(bot, 123, "l_42")

    assert bot.send_media_group.await_count == 1
    assert bot.send_photo.await_count == 1
    assert bot.send_message.await_count == 1
    kwargs = bot.send_message.await_args.kwargs
    assert "以上是这套房目前保存的现场实拍。" in kwargs["text"]
    labels = [button.text for button in _buttons(kwargs["reply_markup"])]
    assert labels == LOCKED_ALBUM_LABELS
    assert "联系中文顾问" not in " ".join(labels)


@pytest.mark.asyncio
async def test_gallery_no_more_copy_is_locked():
    bot = type("Bot", (), {})()
    bot.send_media_group = AsyncMock()
    bot.send_photo = AsyncMock()
    bot.send_message = AsyncMock()

    with patch("qiaolian_dual.listing.listing_context", return_value={"media_files": [], "status": "active"}):
        await send_listing_photo_preview(bot, 123, "l_42")

    bot.send_media_group.assert_not_awaited()
    bot.send_photo.assert_not_awaited()
    bot.send_message.assert_awaited_once()
    text = bot.send_message.await_args.kwargs["text"]
    assert "这套房的实拍暂时没有加载出来。" in text
    assert "联系我们" in text


def test_new_appointment_ui_has_no_focus_confirm_or_edit_entry():
    current_keyboards = [
        _appointment_date_keyboard(),
        _appointment_time_keyboard(),
        appointment_menu_keyboard(),
        contact_handoff_keyboard(),
    ]
    callbacks = [value for keyboard in current_keyboards for value in _callback_values(keyboard)]
    assert not any(value.startswith("apfocus:") for value in callbacks)
    assert not any(value.startswith("apconfirm:") for value in callbacks)
    assert not any(value.startswith("apedit:") for value in callbacks)


def test_video_is_only_exposed_as_date_page_branch():
    date_labels = [button.text for button in _buttons(_appointment_date_keyboard())]
    appointment_menu_labels = [button.text for button in _buttons(appointment_menu_keyboard())]
    handoff_labels = [button.text for button in _buttons(contact_handoff_keyboard())]

    assert "🎥 改为视频看房" in date_labels
    assert not any("视频" in label for label in appointment_menu_labels)
    assert not any("视频" in label for label in handoff_labels)


def test_help_copy_matches_date_time_direct_submit():
    text = help_text()
    assert "预约看房" in text
    assert "日期页可切换实地/视频" in text
    assert "确认看房预约" not in text
    assert "提交预约" not in text


def test_advisor_received_and_taken_over_are_distinct_states():
    entry = advisor_text()
    taken_over = advisor_response_notice_text()
    assert "联系我们" in entry
    assert "顾问已接手" not in entry
    assert "顾问已接手" in taken_over


def test_current_buttons_do_not_contain_html_markup():
    keyboards = [
        publish_post_keyboard("l_42", "BKK1", "QiaolianTestBot", post_token="qlabc123"),
        _appointment_date_keyboard(),
        _appointment_time_keyboard(),
        appointment_menu_keyboard(),
        contact_handoff_keyboard(),
    ]
    for keyboard in keyboards:
        for button in _buttons(keyboard):
            assert "<" not in button.text
            assert ">" not in button.text


def test_new_time_paths_directly_call_shared_submit_helper():
    source = Path("qiaolian_dual/appointment_flow.py").read_text(encoding="utf-8")
    time_branch = source.split("if data.startswith('aptime:'):", 1)[1].split("if data == 'appoint_back_time':", 1)[0]
    custom_branch = source.split("if appt.pop('awaiting_custom_time', False):", 1)[1].split("if appt.pop('awaiting_contact', False):", 1)[0]
    assert "return await _submit_appointment(" in time_branch
    assert "_appointment_confirm_text" not in time_branch
    assert "return await _submit_appointment(update, context, appt)" in custom_branch
    assert "_appointment_confirm_text" not in custom_branch

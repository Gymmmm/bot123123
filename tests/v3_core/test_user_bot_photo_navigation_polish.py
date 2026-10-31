from telegram import InlineKeyboardMarkup
from v3_core.user_bot.telegram_callback_handler import _single_detail_keyboard


def test_photo_navigation_has_no_self_page_button():
    keyboard = _single_detail_keyboard(InlineKeyboardMarkup([]), 'QL-TEST-A1', 1, 10)
    labels = [button.text for row in keyboard.inline_keyboard for button in row]
    assert '⬅️ 上一组' in labels
    assert '下一组 ➡️' in labels
    assert '2/3' not in labels
    assert '🔍 查看原始照片' in labels


def test_first_last_pages_only_offer_valid_direction():
    first = _single_detail_keyboard(InlineKeyboardMarkup([]), 'QL-TEST-A1', 0, 9)
    last = _single_detail_keyboard(InlineKeyboardMarkup([]), 'QL-TEST-A1', 2, 9)
    first_labels = [b.text for row in first.inline_keyboard for b in row]
    last_labels = [b.text for row in last.inline_keyboard for b in row]
    assert '⬅️ 上一组' not in first_labels
    assert '下一组 ➡️' not in last_labels

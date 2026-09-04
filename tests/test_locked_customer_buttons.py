from pathlib import Path

from qiaolian_dual.keyboards_common import contact_handoff_keyboard, lead_capture_keyboard, main_keyboard
from qiaolian_dual.keyboards_search import guided_search_keyboard
from qiaolian_dual.messages import help_text, listing_match_footer_text
from qiaolian_dual.results_admin import _listing_card_keyboard, _photo_action_keyboard


BANNED = (
    '查看这套',
    '查看实拍',
    '查看更多实拍',
    '咨询这套',
    '咨询顾问',
    '完整实拍',
    '让顾问帮我找',
    '打开中文顾问',
    '侨联找房助手',
    '小彭',
)

LOCKED_HOME = ['🔍 帮我找房', '📅 我的预约', '🛡 租房服务', '🛠 入住服务', '💬 联系中文顾问']
LOCKED_LISTING = {'📋 租赁详情', '📸 更多实拍', '📅 预约看房', '💬 联系中文顾问'}


def _labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def test_home_is_five_locked_buttons():
    assert _labels(main_keyboard()) == LOCKED_HOME


def test_current_available_lives_inside_find_home():
    home = ' '.join(_labels(main_keyboard()))
    find = ' '.join(_labels(guided_search_keyboard()))
    assert '当前可预约' not in home
    assert '当前可预约' in find


def test_find_card_and_album_use_locked_advisor_label():
    card = ' '.join(_labels(_listing_card_keyboard('l_2')))
    album = ' '.join(_labels(_photo_action_keyboard('l_2', available=True)))
    assert '💬 联系中文顾问' in card
    assert '💬 联系中文顾问' in album
    assert '📋 租赁详情' in album
    assert '📅 预约看房' in album
    assert '咨询顾问' not in card
    assert '咨询顾问' not in album


def test_handoff_buttons_do_not_say_open_advisor():
    labels = _labels(contact_handoff_keyboard()) + _labels(lead_capture_keyboard())
    assert all('打开中文顾问' not in text for text in labels)
    assert any(text == '💬 联系中文顾问' for text in labels)


def test_help_copy_does_not_teach_old_button_words():
    blob = help_text() + listing_match_footer_text()
    for word in ('咨询这套', '咨询顾问', '查看这套', '查看实拍'):
        assert word not in blob


def test_listing_detail_source_has_no_utility_lines():
    source = Path('qiaolian_dual/listing.py').read_text(encoding='utf-8')
    block = source[source.index('def listing_cost_text'):source.index('def listing_cost_keyboard')]
    assert '电费' not in block
    assert '水费' not in block
    assert '物业：' not in block
    assert '停车' not in block

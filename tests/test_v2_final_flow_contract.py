from unittest.mock import patch

from qiaolian_dual.appointment_ui import _appointment_date_keyboard, _appointment_time_keyboard
from qiaolian_dual.callback_rental import rental_home_keyboard, rental_home_text
from qiaolian_dual.keyboards_common import main_keyboard, room_type_keyboard
from qiaolian_dual.keyboards_search import guided_search_keyboard, service_hub_keyboard
from qiaolian_dual.listing import listing_cost_keyboard, listing_cost_text, listing_unavailable_keyboard, listing_unavailable_text
from qiaolian_dual.results_admin import _photo_action_keyboard
from qiaolian_dual.texts import welcome_text


def labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def test_v2_home_is_locked():
    home_labels = labels(main_keyboard())
    assert home_labels[:4] == [
        '🔍 帮我找房', '📅 我的预约',
        '🛡 侨联保障', '🛠 入住服务',
    ]
    assert home_labels[-1] == '💬 联系我们'
    assert set(home_labels[4:-1]).issubset({'房源频道'})
    text = welcome_text()
    assert '侨联地产｜您在金边的自己人' in text
    assert '区域 + 预算 + 户型' in text


def test_v2_find_home_short_buttons():
    assert labels(guided_search_keyboard()) == [
        '📍 按区域', '💰 按预算', '🏠 按户型', '🏘 当前可约', '⬅️ 返回首页'
    ]
    assert labels(room_type_keyboard()) == ['单间', '一房', '两房', '三房', '四房+', '不限', '⬅️ 返回']


def test_v2_appointment_starts_on_date_with_inline_mode_toggle():
    date_labels = labels(_appointment_date_keyboard(show_video=True))
    assert date_labels == [
        '今天', '明天', '后天', '📅 其他日期',
        '🎥 改为视频看房', '⬅️ 返回房源', '🏠 返回首页'
    ]
    video_labels = labels(_appointment_date_keyboard(show_video=False))
    assert '🚶 改为实地看房' in video_labels
    assert labels(_appointment_time_keyboard()) == [
        '上午 09:00–12:00', '下午 14:00–17:00', '晚上 17:00–19:00',
        '✍️ 其他时间', '⬅️ 修改日期', '🏠 返回首页'
    ]


def test_v2_listing_status_and_contacts():
    base = {'project': '永旺1', 'layout': '1房', 'price': 800, 'status': 'active'}
    with patch('qiaolian_dual.listing.listing_context', return_value=base):
        text = listing_cost_text('l_1')
        assert '🏠 <b>房源详情</b>' in text
        assert '<b>租金：</b> <b>$800/月</b>' in text
        assert '房态：当前可预约' in text
        assert labels(listing_cost_keyboard('l_1')) == ['📅 预约看房', '📸 更多实拍', '💬 联系我们']
    pending = {**base, 'status': 'pending'}
    with patch('qiaolian_dual.listing.listing_context', return_value=pending):
        assert '房态：房态确认中' in listing_cost_text('l_1')
        assert '这套房正在确认最新房态' in listing_unavailable_text('pending', 'l_1')
        assert labels(listing_unavailable_keyboard('l_1')) == ['🏘 同区可约房源', '💬 联系我们', '🏠 房源详情']


def test_v2_more_photos_actions_use_current_detail_label():
    assert labels(_photo_action_keyboard('l_1', available=True)) == ['🏠 房源详情', '📅 预约看房', '💬 联系我们']
    assert labels(_photo_action_keyboard('l_1', available=False)) == ['🏠 房源详情', '💬 联系我们']


def test_v2_assurance_and_move_in_hubs():
    assurance = rental_home_text()
    assert '🛡 <b>侨联保障</b>' in assurance
    assert '签约前核对费用' in assurance
    assert '入住时把房屋、表计和物品状态留档' in assurance
    assert labels(rental_home_keyboard()) == [
        '💰 费用核对', '📋 入住交接', '🔐 押金与退租', '🚚 搬家协助', '💬 联系我们', '⬅️ 返回首页'
    ]
    assert labels(service_hub_keyboard()) == [
        '🔧 设备报修', '🏢 物业协调', '📦 生活服务', '💬 其他帮助', '⬅️ 返回首页'
    ]

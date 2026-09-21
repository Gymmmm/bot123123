from qiaolian_dual.admin_contract import _binding_contract_text, _contract_actions_keyboard
from qiaolian_dual.callback_rental import rental_home_keyboard, rental_home_text
from qiaolian_dual.keyboards_search import resident_service_keyboard, service_hub_keyboard, service_repair_keyboard


def _rows(markup):
    return [[button.text for button in row] for row in markup.inline_keyboard]


def _callbacks(markup):
    return [button.callback_data for row in markup.inline_keyboard for button in row if button.callback_data]


def test_p15_service_hub_keyboard_locked():
    assert _rows(service_hub_keyboard()) == [
        ['📋 我的租约', '🛡️ 入住服务'],
        ['🏠 安心租房', '💬 中文顾问'],
        ['⬅️ 返回首页'],
    ]


def test_p16_resident_service_keyboard_locked_and_coordination_prefix():
    markup = resident_service_keyboard()
    assert _rows(markup) == [
        ['🔧 房屋报修', '🏢 物业协调'],
        ['🔌 水电协助', '🚚 搬家协助'],
        ['🧹 保洁服务', '🌐 网络协助'],
        ['❓ 其他住房问题'],
        ['⬅️ 返回侨联服务'],
    ]
    callbacks = _callbacks(markup)
    assert 'coordination_start' in callbacks
    assert not any(value.startswith('property_') for value in callbacks)


def test_p20_repair_types_locked():
    assert _rows(service_repair_keyboard()) == [
        ['❄️ 空调', '🚿 热水 / 漏水'],
        ['💡 灯具 / 电路', '🔐 门锁 / 门禁'],
        ['🧺 洗衣机', '🧊 冰箱'],
        ['🌐 网络', '🪑 家具损坏'],
        ['❓ 其他问题'],
        ['⬅️ 退出报修'],
    ]


def test_p18_unbound_lease_copy_locked():
    assert _binding_contract_text(None) == (
        '📋 <b>我的租约</b>\n'
        '目前没有查到已绑定的租约。如果你已经通过侨联入住，但这里暂时没有显示，可以联系中文顾问帮你核对。'
    )


def test_p19_bound_lease_copy_and_buttons_locked():
    binding = {
        'property_name': '测试房源',
        'monthly_rent': 800,
        'deposit_months': 2,
        'rent_day': 5,
        'contract_end_date': '2027-01-31',
    }
    assert _binding_contract_text(binding) == (
        '📋 <b>租赁详情</b>\n'
        '🏠 测试房源\n'
        '月租｜$800/月\n'
        '押金｜2个月\n'
        '交租日｜每月5日\n'
        '到期日｜2027-01-31\n'
        '状态｜🟢 租约有效'
    )
    assert _rows(_contract_actions_keyboard()) == [
        ['🔄 申请续租', '💬 中文顾问'],
        ['⬅️ 返回侨联服务'],
    ]


def test_p24_assurance_copy_and_buttons_locked():
    assert rental_home_text() == (
        '🏠 <b>侨联地产｜金边中文租房</b>\n'
        '⭐ 金边本地6年经验\n'
        '📍 富力城｜炳发城｜BKK1｜钻石岛\n'
        '💬 专业中文顾问\n'
        '📸 真实房源｜实拍更新\n'
        '📹 实地看房 / 视频代看\n'
        '找房 · 看房 · 签约 · 入住 · 售后\n'
        '签约不是服务的结束。'
    )
    assert _rows(rental_home_keyboard()) == [
        ['🔍 开始找房'],
        ['💬 中文顾问'],
        ['⬅️ 返回首页'],
    ]
from qiaolian_dual import messages
from qiaolian_dual.keyboards_common import main_keyboard
from qiaolian_dual.keyboards_search import service_hub_keyboard


def _labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def test_integrated_home_copy_and_buttons():
    text = messages.home_text()
    assert '金边华人房产服务' in text
    assert '在金边找房，找自己人' in text
    labels = _labels(main_keyboard())
    assert '🔍 智能找房' in labels
    assert '📖 关于侨联' in labels
    assert '📅 预约看房' in labels
    assert '💬 联系顾问' in labels
    assert '🛡 入住服务' in labels
    assert '🧭 周边服务' in labels


def test_integrated_aftercare_scope():
    text = messages.service_hub_text()
    assert '入住才是侨联服务的开始' in text
    assert '续租 / 换房' in text
    assert '退租与押金核对' in text
    labels = _labels(service_hub_keyboard())
    assert '🔧 报修与维护' in labels
    assert '🏢 物业沟通' in labels
    assert '🔄 续租 / 换房' in labels
    assert '🚪 退租与押金' in labels
    assert '📋 我的租约' in labels


def test_integrated_copy_keeps_legacy_api():
    assert callable(messages.listing_detail)
    assert callable(messages.repair_progress_text)
    assert callable(messages.local_life_text)

from pathlib import Path

from qiaolian_dual.keyboards_common import appointment_success_keyboard
from qiaolian_dual.messages import advisor_response_notice_text, appoint_success_text
from qiaolian_dual.results_admin import _find_result_card_content, admin_lead_keyboard


def _labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def _callbacks(markup):
    return [button.callback_data for row in markup.inline_keyboard for button in row if button.callback_data]


def test_appointment_success_copy_and_buttons():
    text = appoint_success_text()
    assert '预约申请已提交' in text
    assert '无需重复提交' not in text
    assert '✅ <b>预约申请已提交</b>' in text
    assert _labels(appointment_success_keyboard()) == ['📅 我的预约', '💬 联系中文顾问', '🏠 继续看房']


def test_appointment_submit_page_uses_compact_date_time_and_no_repeat_copy():
    source = Path('qiaolian_dual/appointment_flow.py').read_text(encoding='utf-8')
    assert "date_compact = _appointment_date_compact" in source
    assert "time_compact = _appointment_time_compact" in source
    assert '预约申请已提交' in source
    assert '无需重复提交' not in source


def test_advisor_handoff_customer_copy_and_buttons():
    text = advisor_response_notice_text()
    assert '顾问已接手' in text
    assert '预约和房源信息已经一起发给顾问' in text
    source = Path('qiaolian_dual/callback_admin.py').read_text(encoding='utf-8')
    assert "InlineKeyboardButton('📅 查看我的预约'" in source
    assert "InlineKeyboardButton('🏠 继续看房'" in source
    assert "callback_data='hub:latest'" in source


def test_advisor_notification_hides_backend_words_and_uses_status():
    source = Path('qiaolian_dual/appointment_flow.py').read_text(encoding='utf-8')
    start = source.index("title=f'📅 新预约 #{appointment_id}'")
    end = source.index('subject_text =', start)
    block = source[start:end]
    assert 'appointment_hub' not in block
    assert '线索 #' not in block
    assert '待处理' not in block
    assert '<b>当前状态｜🟡 待联系</b>' in block
    assert 'advisor_listing' in block
    assert 'show_bell=False' in block


def test_advisor_buttons_use_new_copy_with_compatible_callbacks():
    kb = admin_lead_keyboard(lead_id=281, appointment_id=24, user_id=7)
    assert _labels(kb) == ['✅ 我来跟进', '📞 已联系客户', '🚫 结束跟进']
    assert _callbacks(kb) == [
        'adminlead:claim:281:24:7',
        'adminlead:contacted:281:24:7',
        'adminlead:invalid:281:24:7',
    ]
    assert '无效线索' not in ' '.join(_labels(kb))


def test_admin_status_replaces_current_status_instead_of_appending_old_copy():
    source = Path('qiaolian_dual/callback_admin.py').read_text(encoding='utf-8')
    assert '当前状态｜' in source
    assert '跟进顾问｜' in source
    assert 'status_pattern.sub(status_line' in source
    assert "status_line = f'\\n\\n<b>处理状态：</b>" not in source


def test_recommendation_card_navigation_and_more_photos_stay_locked(monkeypatch):
    import qiaolian_dual.results_admin as results_admin
    monkeypatch.setattr(results_admin, 'os', results_admin.os)
    monkeypatch.setattr(results_admin, 'listing_context', lambda *_: {}, raising=False)
    text, kb, _ = _find_result_card_content(
        {
            'listing_id': 'l_2',
            'project': '永旺1',
            'layout': '1房1办公2卫',
            'property_type': '公寓',
            'price': 1800,
            'status': 'active',
        },
        1,
        4,
    )
    labels = _labels(kb)
    callbacks = _callbacks(kb)
    assert '⬅️ 上一套' in labels
    assert '下一套 ➡️' in labels
    assert '📸 查看更多实拍' in labels
    assert 'findcard:0' in callbacks
    assert 'findcard:2' in callbacks
    assert 'listing:photos:l_2' in callbacks

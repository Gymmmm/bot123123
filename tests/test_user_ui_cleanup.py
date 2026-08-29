from qiaolian_dual.admin_contract import _contract_actions_keyboard
from qiaolian_dual.common import LEASE_REMINDER_DAYS
from qiaolian_dual.keyboards_common import main_keyboard, old_tenant_followup_keyboard
from qiaolian_dual.keyboards_search import service_hub_keyboard
from qiaolian_dual.listing import listing_entry_keyboard
from qiaolian_dual.results_admin import _format_match_line
from qiaolian_dual.utils_formatting import _display_layout


def _labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def _callbacks(markup):
    return [button.callback_data for row in markup.inline_keyboard for button in row if button.callback_data]


def test_customer_wording_is_unified():
    labels = _labels(main_keyboard())
    assert '🏠 可预约房源' in labels
    assert '💬 联系中文顾问' in labels
    joined = ' '.join(labels)
    assert '在架房源' not in joined
    assert '联系我们' not in joined
    assert '管理号' not in joined
    assert '待推荐' not in joined


def test_new_tenant_pages_do_not_generate_renew_or_change_buttons():
    callbacks = _callbacks(old_tenant_followup_keyboard())
    assert 'contract:renew' not in callbacks
    assert 'contract:change' not in callbacks
    assert 'service:renew_change' not in callbacks


def test_contract_page_does_not_generate_renew_or_change_buttons():
    callbacks = _callbacks(_contract_actions_keyboard(None))
    assert 'contract:renew' not in callbacks
    assert 'contract:change' not in callbacks
    assert 'contract:toggle_reminder' in callbacks


def test_service_hub_has_no_renew_change_entries_without_binding():
    callbacks = _callbacks(service_hub_keyboard(None))
    assert 'service:renew' not in callbacks
    assert 'service:change' not in callbacks
    assert 'service:renew_change' not in callbacks


def test_only_seven_day_lease_reminder_is_configured():
    assert LEASE_REMINDER_DAYS == (7,)


def test_residential_office_layout_is_display_only():
    assert _display_layout('1房1办公2卫', '公寓') == '1房＋书房｜2卫'
    assert _display_layout('1房1办公2卫', '办公室') == '1房1办公2卫'
    line = _format_match_line({'listing_id': 'l_2', 'area': '永旺1', 'layout': '1房1办公2卫', 'property_type': '公寓', 'price': 1800})
    assert '1房＋书房｜2卫' in line
    assert '1房1办公2卫' not in line


def test_listing_entry_keeps_photo_detail_appointment_links():
    callbacks = _callbacks(listing_entry_keyboard('l_2'))
    assert callbacks == [
        'listing:photos:l_2',
        'listing:detail:l_2',
        'listing:appoint:l_2',
        'home_smart_search',
    ]

from __future__ import annotations

from qiaolian_dual import admin_contract, callback_contract, callback_rental, keyboards_common, keyboards_search, texts


def _labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def _callbacks(markup):
    return [button.callback_data for row in markup.inline_keyboard for button in row if button.callback_data]


def test_home_matches_v1_information_architecture():
    labels = _labels(keyboards_common.main_keyboard())
    assert '🔍 开始找房' in labels
    assert '📅 我的预约' in labels
    assert '🛠 入住服务' in labels
    assert '🏠 关于侨联' in labels
    assert '💬 中文顾问' in labels
    assert all('侨联保障' not in label for label in labels)
    assert '侨联小管家' not in texts.welcome_text()


def test_about_qiaolian_is_a_real_home_destination():
    assert '关于侨联' in texts.about_text()
    assert '从找房、签约到入住后的租住服务' in texts.about_text()


def test_unbound_user_does_not_get_tenant_actions(monkeypatch):
    monkeypatch.setattr(keyboards_search.db, 'get_active_binding', lambda user_id: None)
    markup = keyboards_search.service_hub_keyboard(1001)
    labels = _labels(markup)
    assert labels == ['💬 联系中文顾问', '🏠 返回首页']
    assert '🔧 报修' not in labels
    assert '🏢 物业沟通' not in labels
    assert '📋 我的租约' not in labels
    assert '🔄 续租' not in labels
    assert '🚪 退租' not in labels


def test_bound_user_gets_full_tenant_service(monkeypatch):
    monkeypatch.setattr(keyboards_search.db, 'get_active_binding', lambda user_id: {'id': 7, 'property_name': 'BKK1 A-1203'})
    markup = keyboards_search.service_hub_keyboard(1001)
    labels = _labels(markup)
    assert '🔧 报修' in labels
    assert '🏢 物业沟通' in labels
    assert '📋 我的租约' in labels
    assert '📄 租赁服务指南' in labels
    assert '🔄 续租' in labels
    assert '🚪 退租' in labels
    assert all('换房' not in label for label in labels)


def test_rental_guide_previews_before_download():
    home_callbacks = _callbacks(callback_rental.rental_home_keyboard())
    assert 'hub:rental:handover' in home_callbacks
    assert 'hub:rental:deposit' in home_callbacks
    assert 'hub:rental:handover:pdf' not in home_callbacks
    assert 'hub:rental:deposit:pdf' not in home_callbacks

    handover_callbacks = _callbacks(callback_rental.handover_keyboard())
    deposit_callbacks = _callbacks(callback_rental.deposit_keyboard())
    assert 'hub:rental:handover:pdf' in handover_callbacks
    assert 'hub:rental:deposit:pdf' in deposit_callbacks


def test_contract_actions_are_renew_or_terminate_only(monkeypatch):
    monkeypatch.setattr(admin_contract.db, 'is_lease_reminder_enabled', lambda user_id: True)
    markup = admin_contract._contract_actions_keyboard(1001)
    labels = _labels(markup)
    callbacks = _callbacks(markup)
    assert '🔄 续租' in labels
    assert '🚪 退租' in labels
    assert all('换房' not in label for label in labels)
    assert 'contract:renew' in callbacks
    assert 'contract:terminate' in callbacks
    assert 'contract:change' not in callbacks


def test_contract_router_supports_termination():
    assert callback_contract.matches('contract:terminate')
    assert callback_contract.matches('contract:terminate_yes:7')

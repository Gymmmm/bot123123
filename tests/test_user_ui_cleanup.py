from pathlib import Path

import discussion_map_store
import publication_package
from qiaolian_dual import search
from qiaolian_dual.admin_contract import _contract_actions_keyboard
from qiaolian_dual.common import LEASE_REMINDER_DAYS
from qiaolian_dual.keyboards_common import main_keyboard, old_tenant_followup_keyboard
from qiaolian_dual.keyboards_search import service_hub_keyboard
from qiaolian_dual.listing import listing_entry_keyboard
from qiaolian_dual.results_admin import _format_match_line
from qiaolian_dual.utils_formatting import _display_floor, _display_layout


def _labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def _callbacks(markup):
    return [button.callback_data for row in markup.inline_keyboard for button in row if button.callback_data]


def test_customer_wording_is_unified():
    labels = _labels(main_keyboard())
    assert labels == ['🔍 帮我找房', '📅 我的预约', '🛡 侨联保障', '🛠 入住服务', '💬 联系我们']
    joined = ' '.join(labels)
    assert '可预约房源' not in joined
    assert '当前可预约' not in joined
    assert '在架房源' not in joined
    assert '联系中文顾问' not in joined
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


def test_floor_is_normalized_only_for_display():
    assert _display_floor('23') == '23楼'
    assert _display_floor('23楼') == '23楼'
    assert _display_floor('高层') == '高层'
    assert _display_floor('') == ''


def test_publication_caption_uses_display_layout_and_floor():
    item = {'area': '永旺1', 'layout': '1房1办公2卫', 'property_type': '公寓', 'size': '96.3㎡', 'floor': '23', 'price': 1800, 'deal_type': 'rent'}
    text = publication_package.format_button_post_text(item, 'l_2', [])
    assert '1房＋书房｜2卫' in text
    assert '1房1办公2卫' not in text
    assert '23楼' in text


def test_no_match_never_falls_back_to_unrelated_recent_listing(monkeypatch):
    monkeypatch.setattr(search, '_public_search_listings', lambda **kwargs: [])
    def _must_not_run(*args, **kwargs):
        raise AssertionError('strict public search must not silently return recent listings')
    monkeypatch.setattr(search.db, 'list_recent_listings', _must_not_run)
    matches, mode = search.search_listings_with_fallback(property_type='公寓', area='不存在区域', budget_min=12345, budget_max=13000, limit=3)
    assert matches == []
    assert mode == 'no_match'


def test_public_db_search_includes_reserved_status_source_rule():
    source = Path('qiaolian_dual/db.py').read_text(encoding='utf-8')
    assert "status IN ('active','reserved')" in source


def test_discussion_mapping_prefers_posts_and_only_uses_legacy_as_fallback(monkeypatch):
    monkeypatch.setattr(discussion_map_store, '_backend', lambda: 'auto')
    monkeypatch.setattr(discussion_map_store, '_load_posts_sqlite', lambda: {'3054': 5140})
    monkeypatch.setattr(discussion_map_store, '_load_legacy_sqlite', lambda: {'3054': 9999, '3055': 5143})
    monkeypatch.setattr(discussion_map_store, '_load_json', lambda: {'3055': 8888, '3000': 4000})
    assert discussion_map_store.load_discuss_map() == {'3054': 5140, '3055': 5143, '3000': 4000}


def test_historical_publication_package_fallback_is_preserved():
    source = Path('qiaolian_dual/listing.py').read_text(encoding='utf-8')
    assert 'publication_package_id' in source
    assert "media', 'publication_packages', package_id" in source
    assert "publish_status IN ('published','success','ok')" in source


def test_listing_entry_keeps_photo_detail_appointment_links():
    callbacks = _callbacks(listing_entry_keyboard('l_2'))
    assert callbacks == [
        'listing:detail:l_2',
        'listing:photos:l_2',
        'listing:appoint:l_2',
        'listing:consult:l_2',
        'home_smart_search',
    ]


def test_obsolete_rent_day_reminder_is_removed():
    import qiaolian_dual.jobs as jobs
    import qiaolian_dual.user_bot as user_bot
    assert not hasattr(jobs, 'rent_day_reminder_job')
    assert not hasattr(user_bot, 'rent_day_reminder_job')


def test_publication_package_has_no_duplicate_floor_import():
    source = Path('publication_package.py').read_text(encoding='utf-8')
    assert '_display_floor, _display_floor' not in source


def test_customer_routes_do_not_leak_internal_listing_wording():
    customer_route_files = ['qiaolian_dual/start_routes.py', 'qiaolian_dual/message_handlers.py', 'qiaolian_dual/listing.py']
    source = '\n'.join(Path(path).read_text(encoding='utf-8') for path in customer_route_files)
    assert '在架房源' not in source
    assert '请联系我们重新获取绑定码' not in source


def test_binding_copy_only_promises_service_context_and_seven_day_reminder():
    sources = '\n'.join(Path(path).read_text(encoding='utf-8') for path in ('qiaolian_dual/message_handlers.py', 'qiaolian_dual/admin_commands.py'))
    assert '到期前 7 天' in sources
    assert '续租和换房时无需重复填写' not in sources


def test_find_card_rechecks_listing_status_before_rendering():
    source = Path('qiaolian_dual/results_admin.py').read_text(encoding='utf-8')
    assert 'listing_is_available' in source
    assert 'valid_ids' in source
    assert '这批推荐的房态已经变化' in source


def test_find_card_keeps_navigation_and_more_photos_cta():
    source = Path('qiaolian_dual/results_admin.py').read_text(encoding='utf-8')
    assert "InlineKeyboardButton('⬅️ 上一套'" in source
    assert "InlineKeyboardButton('下一套 ➡️'" in source
    assert "InlineKeyboardButton('📸 更多实拍'" in source
    assert "callback_data=f'listing:photos:{listing_id}'" in source

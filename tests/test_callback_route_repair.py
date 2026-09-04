import re
from types import SimpleNamespace

import pytest
from telegram.constants import ParseMode

from qiaolian_dual import common
from qiaolian_dual.app import build_application
from qiaolian_dual.callbacks import handle_ui_callback
from qiaolian_dual.keyboards_common import main_keyboard
from qiaolian_dual.listing import listing_unavailable_keyboard, listing_unavailable_text
from qiaolian_dual.results_admin import _find_result_card_content


class DummyMessage:
    def __init__(self, *, photo=False):
        self.photo = [object()] if photo else []
        self.chat_id = 123
        self.message_id = 456
        self.deleted = 0
        self.edits = []
        self.replies = []

    async def delete(self):
        self.deleted += 1

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))
        return SimpleNamespace(chat_id=self.chat_id, message_id=999)


class DummyQuery:
    def __init__(self, data, *, photo=False):
        self.data = data
        self.message = DummyMessage(photo=photo)
        self.answers = 0
        self.text_edits = []
        self.caption_edits = []
        self.media_edits = []

    async def answer(self, *args, **kwargs):
        self.answers += 1

    async def edit_message_text(self, text, **kwargs):
        self.text_edits.append((text, kwargs))

    async def edit_message_caption(self, caption, **kwargs):
        self.caption_edits.append((caption, kwargs))

    async def edit_message_media(self, media, **kwargs):
        self.media_edits.append((media, kwargs))


class DummyBot:
    def __init__(self):
        self.sent = []

    async def send_message(self, **kwargs):
        self.sent.append(('message', kwargs))

    async def send_photo(self, **kwargs):
        self.sent.append(('photo', kwargs))

    async def send_media_group(self, **kwargs):
        self.sent.append(('media_group', kwargs))


class DummyContext:
    def __init__(self):
        self.user_data = {}
        self.bot = DummyBot()


def make_update(data, *, photo=False):
    query = DummyQuery(data, photo=photo)
    user = SimpleNamespace(id=7, username='tester', first_name='T', full_name='T')
    chat = SimpleNamespace(id=123)
    update = SimpleNamespace(callback_query=query, effective_user=user, effective_chat=chat, effective_message=query.message)
    return update, query


BASE_HOOKS = {'upsert_user_profile': lambda user: None}


def labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def callbacks(markup):
    return [button.callback_data for row in markup.inline_keyboard for button in row if button.callback_data]


def test_home_available_button_uses_unified_callback():
    kb = main_keyboard()
    mapping = {button.text: button.callback_data for row in kb.inline_keyboard for button in row}
    assert '🏠 可预约房源' not in mapping
    assert '当前可预约' not in ''.join(mapping)
    assert mapping['🔍 帮我找房'] == 'home_smart_search'
    assert mapping['📅 我的预约'] == 'hub:appointments'
    assert mapping['🛡 租房服务'] == 'hub:rental'
    assert mapping['🛠 入住服务'] == 'hub:service'
    assert mapping['💬 联系中文顾问'] == 'hub:advisor'


@pytest.mark.asyncio
async def test_home_available_callback_enters_real_recommendation_handler(monkeypatch):
    import qiaolian_dual.callback_navigation as nav
    import qiaolian_dual.results_admin as results
    monkeypatch.setattr(nav.db, 'list_recent_listings', lambda limit: [
        {'listing_id': 'l_1', 'status': 'active'},
        {'listing_id': 'l_2', 'status': 'reserved'},
        {'listing_id': 'l_3', 'status': 'rented'},
    ])
    seen = {}
    async def fake_cards(update, context, matches, mode):
        seen['ids'] = [item['listing_id'] for item in matches]
        seen['mode'] = mode
    monkeypatch.setattr(results, 'send_find_results_as_cards', fake_cards)
    update, query = make_update('hub:available')
    await handle_ui_callback(update, DummyContext(), hooks=BASE_HOOKS)
    assert query.answers == 1
    assert seen == {'ids': ['l_1', 'l_2'], 'mode': 'strict'}


@pytest.mark.asyncio
async def test_listing_detail_calls_real_detail_route(monkeypatch):
    import qiaolian_dual.callback_listing as listing_cb
    import qiaolian_dual.texts as texts
    import qiaolian_dual.listing as listing_mod
    import qiaolian_dual.search as search_mod
    monkeypatch.setattr(listing_cb.db, 'get_listing', lambda lid: {'listing_id': lid, 'status': 'active'})
    monkeypatch.setattr(listing_mod, 'listing_is_available', lambda lid: (True, 'active'))
    monkeypatch.setattr(listing_mod, 'listing_action_allowed', lambda lid, action: (True, 'active'))
    monkeypatch.setattr(search_mod, 'create_lead', lambda *a, **k: None)
    monkeypatch.setattr(listing_mod, 'listing_cost_text', lambda lid: '<b>DETAIL</b>')
    seen = {}
    async def fake_render(update, **kwargs):
        seen.update(kwargs)
    monkeypatch.setattr(texts, 'render_panel', fake_render)
    update, query = make_update('listing:detail:l_2')
    context = DummyContext()
    await handle_ui_callback(update, context, hooks=BASE_HOOKS)
    assert query.answers == 1
    assert seen['text'] == '<b>DETAIL</b>'
    assert seen['parse_mode'] == ParseMode.HTML
    assert context.user_data['contact_listing_id'] == 'l_2'
    assert '📅 预约看房' in labels(seen['reply_markup'])


@pytest.mark.asyncio
async def test_listing_appoint_enters_appointment_flow_with_listing_id(monkeypatch):
    import qiaolian_dual.flows as flows
    import qiaolian_dual.listing as listing_mod
    seen = {}
    async def fake_start(update, context, listing_id, **kwargs):
        seen['listing_id'] = listing_id
        seen.update(kwargs)
        return common.APPT_MODE
    monkeypatch.setattr(listing_mod, 'listing_is_available', lambda lid: (True, 'active'))
    monkeypatch.setattr(flows, 'start_appointment', fake_start)
    update, query = make_update('listing:appoint:l_2')
    state = await handle_ui_callback(update, DummyContext(), hooks=BASE_HOOKS)
    assert query.answers == 1
    assert state == common.APPT_MODE
    assert seen['listing_id'] == 'l_2'


@pytest.mark.asyncio
async def test_reserved_listing_is_allowed_by_real_appointment_flow(monkeypatch):
    import qiaolian_dual.listing as listing_mod
    import qiaolian_dual.texts as texts
    from qiaolian_dual.flows import start_appointment
    monkeypatch.setattr(listing_mod, 'listing_is_available', lambda lid: (True, 'reserved'))
    monkeypatch.setattr(listing_mod, 'listing_context', lambda lid: {
        'listing_id': lid, 'status': 'reserved', 'title': '钻石岛', 'layout': '2房', 'property_type': '公寓', 'price': 900,
    })
    seen = {}
    async def fake_render(update, **kwargs):
        seen.update(kwargs)
    monkeypatch.setattr(texts, 'render_panel', fake_render)
    update, _ = make_update('listing:appoint:l_2')
    context = DummyContext()
    state = await start_appointment(update, context, 'l_2')
    assert state == common.APPT_DATE
    assert context.user_data['appt']['listing_id'] == 'l_2'
    assert '预约看房' in seen['text']


@pytest.mark.asyncio
async def test_listing_photos_calls_complete_album_handler(monkeypatch):
    import qiaolian_dual.results_admin as results
    import qiaolian_dual.listing as listing_mod
    monkeypatch.setattr(listing_mod, 'listing_is_available', lambda lid: (True, 'active'))
    monkeypatch.setattr(listing_mod, 'listing_action_allowed', lambda lid, action: (True, 'active'))
    seen = {}
    async def fake_album(bot, chat_id, listing_id):
        seen['listing_id'] = listing_id
    monkeypatch.setattr(results, 'send_listing_photo_preview', fake_album)
    update, query = make_update('listing:photos:l_2')
    context = DummyContext()
    await handle_ui_callback(update, context, hooks=BASE_HOOKS)
    assert query.answers == 1
    assert seen['listing_id'] == 'l_2'
    assert context.user_data['contact_listing_id'] == 'l_2'


@pytest.mark.asyncio
async def test_listing_consult_preserves_listing_context(monkeypatch):
    import qiaolian_dual.flows as flows
    import qiaolian_dual.listing as listing_mod
    monkeypatch.setattr(listing_mod, 'listing_is_available', lambda lid: (True, 'active'))
    monkeypatch.setattr(listing_mod, 'listing_action_allowed', lambda lid, action: (True, 'active'))
    seen = {}
    async def fake_contact(update, context, **kwargs):
        seen.update(kwargs)
        return common.MAIN
    monkeypatch.setattr(flows, 'contact_management', fake_contact)
    update, query = make_update('listing:consult:l_2')
    context = DummyContext()
    await handle_ui_callback(update, context, hooks=BASE_HOOKS)
    assert query.answers == 1
    assert context.user_data['contact_listing_id'] == 'l_2'
    assert seen['from_listing'] == 'l_2'


def test_main_callback_pattern_matches_all_listing_routes_and_available_hub():
    callbacks_to_match = [
        'hub:available', 'hub:latest', 'listing:open:l_2', 'listing:detail:l_2',
        'listing:appoint:l_2', 'listing:photos:l_2', 'listing:consult:l_2',
    ]
    for callback in callbacks_to_match:
        assert re.match(common._MAIN_CB_PATTERN, callback), callback


@pytest.mark.asyncio
@pytest.mark.parametrize('data', ['listing:detail:l_2', 'listing:appoint:l_2', 'listing:photos:l_2', 'listing:consult:l_2'])
async def test_each_listing_callback_answers_query(monkeypatch, data):
    import qiaolian_dual.callbacks as dispatcher
    async def fake_listing(update, context, query, data, user):
        return common.MAIN
    monkeypatch.setattr(dispatcher, 'handle_listing_callback', fake_listing)
    update, query = make_update(data)
    await handle_ui_callback(update, DummyContext(), hooks=BASE_HOOKS)
    assert query.answers == 1


def test_unavailable_page_is_html_and_has_all_real_callbacks():
    text = listing_unavailable_text('pending')
    assert '🏠 <b>这套房暂时不能预约</b>' in text
    kb = listing_unavailable_keyboard('')
    assert labels(kb) == ['🔍 同区可预约房源', '💬 联系中文顾问', '📋 租赁详情']
    assert callbacks(kb) == ['unavail:more:any', 'listing:consult:', 'listing:detail:']


@pytest.mark.asyncio
async def test_unavailable_callback_renders_once_with_html(monkeypatch):
    import qiaolian_dual.listing as listing_mod
    import qiaolian_dual.texts as texts
    monkeypatch.setattr(listing_mod, 'listing_is_available', lambda lid: (False, 'pending'))
    monkeypatch.setattr(listing_mod, 'listing_action_allowed', lambda lid, action: (action != 'appoint', 'pending'))
    monkeypatch.setattr(listing_mod, 'listing_context', lambda lid: {'listing_id': lid, 'area': 'BKK1', 'status': 'pending'})
    seen = []
    async def fake_render(update, **kwargs):
        seen.append(kwargs)
    monkeypatch.setattr(texts, 'render_panel', fake_render)
    update, query = make_update('listing:appoint:l_2')
    await handle_ui_callback(update, DummyContext(), hooks=BASE_HOOKS)
    assert query.answers == 1
    assert len(seen) == 1
    assert seen[0]['parse_mode'] == ParseMode.HTML
    assert seen[0]['text'].count('这套房暂时不能预约') == 1


def test_multi_listing_navigation_names_area_layout_and_callbacks(monkeypatch):
    import qiaolian_dual.results_admin as results
    data = {
        'l_1': {'listing_id': 'l_1', 'area': 'BKK1', 'layout': '1房', 'property_type': '公寓', 'status': 'active', 'price': 600},
        'l_2': {'listing_id': 'l_2', 'area': '钻石岛', 'layout': '2房', 'property_type': '公寓', 'status': 'reserved', 'price': 900},
        'l_3': {'listing_id': 'l_3', 'area': '永旺1', 'layout': '1房', 'property_type': '公寓', 'status': 'active', 'price': 700},
    }
    monkeypatch.setattr(results, 'listing_context', lambda lid: data.get(lid, {}), raising=False)
    import qiaolian_dual.listing as listing_mod
    monkeypatch.setattr(listing_mod, 'listing_context', lambda lid: data.get(lid, {}))
    _, kb, _ = _find_result_card_content(data['l_2'], 1, 3, ['l_1', 'l_2', 'l_3'])
    nav = kb.inline_keyboard[0]
    assert nav[0].text == '⬅️ 上一套'
    assert nav[1].text == '下一套 ➡️'
    assert nav[0].callback_data == 'findcard:0:l_1'
    assert nav[1].callback_data == 'findcard:2:l_3'
    assert all(len(button.text) <= 28 for button in nav)


def test_single_listing_has_no_previous_next(monkeypatch):
    import qiaolian_dual.listing as listing_mod
    item = {'listing_id': 'l_2', 'area': '钻石岛', 'layout': '2房', 'property_type': '公寓', 'status': 'active', 'price': 900}
    monkeypatch.setattr(listing_mod, 'listing_context', lambda lid: item)
    _, kb, _ = _find_result_card_content(item, 0, 1, ['l_2'])
    assert not any('上一套' in label or '下一套' in label or '⬅️' in label and '返回' not in label for label in labels(kb))


def test_recommendation_card_detail_photo_appointment_consult_callbacks(monkeypatch):
    import qiaolian_dual.listing as listing_mod
    item = {'listing_id': 'l_2', 'area': '钻石岛', 'layout': '2房', 'property_type': '公寓', 'status': 'reserved', 'price': 900}
    monkeypatch.setattr(listing_mod, 'listing_context', lambda lid: item)
    _, kb, _ = _find_result_card_content(item, 0, 1, ['l_2'])
    cbs = callbacks(kb)
    assert 'listing:detail:l_2' in cbs
    assert 'listing:appoint:l_2' in cbs
    assert 'listing:photos:l_2' in cbs
    assert 'listing:consult:l_2' in cbs
    assert not any('similar' in cb for cb in cbs)


def test_build_application_constructs_with_test_token():
    app = build_application(token='123456:TESTTOKEN')
    assert app is not None

from pathlib import Path


def replace_once(path: str, old: str, new: str, label: str) -> None:
    p = Path(path)
    text = p.read_text(encoding='utf-8')
    if old not in text:
        raise SystemExit(f'missing patch target: {label}')
    p.write_text(text.replace(old, new, 1), encoding='utf-8')


# 1. Home available-listing entry gets its own stable callback; keep hub:latest as legacy alias.
replace_once(
    'qiaolian_dual/keyboards_common.py',
    "InlineKeyboardButton('🏠 可预约房源', callback_data='hub:latest')",
    "InlineKeyboardButton('🏠 可预约房源', callback_data='hub:available')",
    'home available callback',
)

# 2. Navigation routes both new and legacy callbacks into the same active/reserved recommendation renderer.
p = Path('qiaolian_dual/callback_navigation.py')
text = p.read_text(encoding='utf-8')
text = text.replace("or (data == 'hub:latest') or", "or (data == 'hub:latest') or (data == 'hub:available') or", 1)
old = """    if data == 'hub:latest':
            matches = db.list_recent_listings(10)
            if matches:
                await send_find_results_as_cards(update, context, matches, 'strict')
            else:
                await render_panel(update, text='暂时没有可以安排看房的房源。\\n可以换个条件，或让顾问继续帮你找。', parse_mode=ParseMode.HTML, reply_markup=no_match_followup_keyboard())
            return MAIN
"""
new = """    if data in {'hub:available', 'hub:latest'}:
            # hub:latest is retained only as a compatibility alias for historical buttons.
            # The customer-facing home entry is hub:available and always uses the same
            # public active/reserved list + single recommendation-card renderer.
            matches = db.list_recent_listings(10)
            matches = [item for item in matches if str(item.get('status') or '').strip().lower() in {'active', 'reserved'}]
            if matches:
                await send_find_results_as_cards(update, context, matches, 'strict')
            else:
                await render_panel(update, text='暂时没有可以安排看房的房源。\\n可以换个条件，或让顾问继续帮你找。', parse_mode=ParseMode.HTML, reply_markup=no_match_followup_keyboard(), context=context)
            return MAIN
"""
if old not in text:
    raise SystemExit('missing patch target: hub latest handler')
p.write_text(text.replace(old, new, 1), encoding='utf-8')

# 3. Listing copy/availability keyboard/detail copy.
p = Path('qiaolian_dual/listing.py')
text = p.read_text(encoding='utf-8')
start = text.index('def listing_cost_text(listing_id: str) -> str:')
end = text.index('\ndef listing_cost_keyboard', start)
new_cost = '''def listing_cost_text(listing_id: str) -> str:
    """租赁详情只展示已有事实；缺失项统一明确标注待确认。"""
    from .utils_formatting import _fmt_price
    item = listing_context(listing_id)
    normalized: dict = {}
    raw_normalized = item.get('normalized_data')
    if isinstance(raw_normalized, dict):
        normalized = raw_normalized
    elif raw_normalized:
        try:
            normalized = json.loads(str(raw_normalized))
        except (TypeError, ValueError, json.JSONDecodeError):
            normalized = {}

    def fact(*values) -> str:
        for value in values:
            value = str(value or '').strip()
            if value:
                return value
        return '待确认'

    price_raw = item.get('price')
    price = _fmt_price(price_raw) if price_raw not in (None, '', 0, '0') else '待确认'
    deposit = fact(item.get('deposit'), item.get('deposit_rule'), normalized.get('deposit_payment_terms'))
    contract = fact(normalized.get('contract_term_display'), normalized.get('contract_term'), item.get('contract_term'))
    management = fact(normalized.get('management_fee'))
    internet = fact(normalized.get('internet_fee'))
    water = fact(item.get('water_rate'), normalized.get('water_rate'))
    electric = fact(item.get('electric_rate'), normalized.get('electric_rate'))
    parking = fact(normalized.get('parking_fee'))

    amenities_raw = normalized.get('special_tags') or item.get('highlights') or []
    if isinstance(amenities_raw, str):
        amenities = [x.strip() for x in re.split(r'[、,，|｜·]', amenities_raw) if x.strip()]
    elif isinstance(amenities_raw, list):
        amenities = [str(x).strip() for x in amenities_raw if str(x).strip()]
    else:
        amenities = []
    amenities = list(dict.fromkeys(amenities))[:6]
    amenities_text = ' · '.join(amenities) if amenities else '待确认'

    lines = [
        '<b>📋 租赁详情</b>',
        '',
        '<b>💰 租赁</b>',
        f'月租｜{he(price)}',
        f'押付｜{he(deposit)}',
        f'租期｜{he(contract)}',
        '',
        '<b>🧾 费用</b>',
        f'管理费｜{he(management)}',
        f'网络｜{he(internet)}',
    ]
    known_optional = []
    missing_optional = []
    for label, value in (('水费', water), ('电费', electric), ('停车', parking)):
        if value == '待确认':
            missing_optional.append(label)
        else:
            known_optional.append(f'{label}｜{he(value)}')
    lines.extend(known_optional)
    if missing_optional:
        lines.append(f"待确认｜{' · '.join(missing_optional)}")
    lines.extend(['', '<b>🏊 配套</b>', he(amenities_text), ''])
    status = str(item.get('status') or '').strip().lower()
    if status == 'reserved':
        lines.append('<b>🟡 已有预约 · 仍可预约</b>')
    elif status == 'active':
        lines.append('<b>🟢 当前可预约</b>')
    else:
        lines.append('<b>房态待确认</b>')
    return '\\n'.join(lines)
'''
text = text[:start] + new_cost + text[end:]
old_unavailable = '''def listing_unavailable_text(reason: str='') -> str:
    """根据房态给用户准确提示，不把所有不可预约状态都说成已租出。"""
    status = str(reason or '').strip().lower()
    if status == 'rented':
        return '<b>🔴 这套房源已租出</b>\\n\\n目前不能继续预约。可以看看同区域、同预算的类似房源，或者让顾问直接帮你匹配。'
    if status == 'pending':
        return '<b>🔵 这套房源正在确认房态</b>\\n\\n为了避免白跑一趟，暂时不直接接受预约。顾问确认后可以继续安排。'
    if status in {'inactive', 'offline'}:
        return '<b>⚫ 这套房源目前已下架</b>\\n\\n可以看看同区域、同预算的类似房源。'
    return '<b>这套房暂时不能预约</b>\\n\\n房态正在确认。可以先看其他可预约房源，或者让中文顾问帮你确认。'

def listing_unavailable_keyboard(listing_id: str='') -> InlineKeyboardMarkup:
    area = str(listing_context(listing_id).get('area') or '').strip()
    rows: list[list[InlineKeyboardButton]] = [[InlineKeyboardButton('🔍 找相近房源', callback_data='findmode:guided')], [InlineKeyboardButton('💬 联系中文顾问', callback_data='appointment_menu:contact')]]
    if area and area != '不限':
        rows.append([InlineKeyboardButton('🏠 同区推荐', callback_data=f'unavail:more:{area}')])
    rows.append([InlineKeyboardButton('🏠 返回首页', callback_data='home')])
    return InlineKeyboardMarkup(rows)
'''
new_unavailable = '''def listing_unavailable_text(reason: str='') -> str:
    """统一不可预约页；具体内部状态不在客户页重复堆叠。"""
    return (
        '<b>🏠 这套房暂时不能预约</b>\\n\\n'
        '房态正在确认。\\n'
        '你可以先看附近可预约房源，\\n'
        '也可以让中文顾问帮你确认。'
    )

def listing_unavailable_keyboard(listing_id: str='') -> InlineKeyboardMarkup:
    area = str(listing_context(listing_id).get('area') or '').strip()
    area_token = area if area and area != '不限' else 'any'
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔍 找附近房源', callback_data='findmode:guided')],
        [InlineKeyboardButton('💬 联系中文顾问', callback_data='appointment_menu:contact')],
        [InlineKeyboardButton('🏠 同区推荐', callback_data=f'unavail:more:{area_token}')],
        [InlineKeyboardButton('⬅️ 返回上一页', callback_data='home')],
    ])
'''
if old_unavailable not in text:
    raise SystemExit('missing patch target: unavailable page')
text = text.replace(old_unavailable, new_unavailable, 1)
p.write_text(text, encoding='utf-8')

# 4. Result-card navigation labels, real detail callback, same-message switching.
p = Path('qiaolian_dual/results_admin.py')
text = p.read_text(encoding='utf-8')
old_send_results = '''    query = getattr(update, 'callback_query', None)
    if query is not None:
        try:
            await query.message.delete()
        except Exception:
            logger.debug('旧筛选面板无法删除，继续发送单卡推荐', exc_info=True)
    await send_find_result_card(update, context, 0, replace=False)
'''
new_send_results = '''    query = getattr(update, 'callback_query', None)
    # First render may need to replace a text-only panel with a photo card. In that
    # case Telegram cannot edit text -> media, so remove the old panel once and send
    # exactly one card. After that all previous/next navigation edits the same card.
    first_item = listing_context(ids[0]) if ids else {}
    first_media = first_item.get('media_files') if isinstance(first_item.get('media_files'), list) else []
    first_photo = next((path for path in first_media if isinstance(path, str) and os.path.exists(path)), '')
    if not first_photo:
        candidate = str(first_item.get('media_file_id') or '')
        first_photo = candidate if candidate and os.path.exists(candidate) else ''
    replace = bool(query is not None and (getattr(query.message, 'photo', None) or not first_photo))
    if query is not None and first_photo and not getattr(query.message, 'photo', None):
        try:
            await query.message.delete()
        except Exception:
            logger.debug('旧筛选面板无法删除，继续发送单张推荐卡', exc_info=True)
    await send_find_result_card(update, context, 0, replace=replace)
'''
if old_send_results not in text:
    raise SystemExit('missing patch target: send_find_results_as_cards panel switch')
text = text.replace(old_send_results, new_send_results, 1)
old_sig = "def _find_result_card_content(item: dict, index: int, total: int) -> tuple[str, InlineKeyboardMarkup, str]:"
new_sig = "def _find_result_card_content(item: dict, index: int, total: int, result_ids: list[str] | None=None) -> tuple[str, InlineKeyboardMarkup, str]:"
text = text.replace(old_sig, new_sig, 1)
old_nav = '''    nav = []
    if total > 1:
        nav = [InlineKeyboardButton('⬅️ 上一套', callback_data=f'findcard:{(index - 1) % total}'), InlineKeyboardButton('下一套 ➡️', callback_data=f'findcard:{(index + 1) % total}')]
    rows = []
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton('📋 租赁详情', callback_data=f'listing:open:{listing_id}'), InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}')])
'''
new_nav = '''    nav = []
    if total > 1:
        ids = list(result_ids or [])
        prev_index = (index - 1) % total
        next_index = (index + 1) % total
        prev_id = ids[prev_index] if len(ids) == total else ''
        next_id = ids[next_index] if len(ids) == total else ''

        def nav_label(target_id: str, fallback: str, *, left: bool) -> str:
            target = listing_context(target_id) if target_id else {}
            nav_area = clean_inline_text(str(target.get('project') or target.get('community') or target.get('area') or ''))
            nav_layout = _display_layout(clean_inline_text(str(target.get('layout') or target.get('property_type') or '')), target.get('property_type'))
            core = '｜'.join(value for value in (nav_area, nav_layout) if value) or fallback
            core = core[:22]
            return f'⬅️ {core}' if left else f'{core} ➡️'

        nav = [
            InlineKeyboardButton(nav_label(prev_id, '上一套', left=True), callback_data=f'findcard:{prev_index}:{prev_id or "unknown"}'),
            InlineKeyboardButton(nav_label(next_id, '下一套', left=False), callback_data=f'findcard:{next_index}:{next_id or "unknown"}'),
        ]
    rows = []
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton('📋 租赁详情', callback_data=f'listing:detail:{listing_id}'), InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}')])
'''
if old_nav not in text:
    raise SystemExit('missing patch target: result card nav/detail')
text = text.replace(old_nav, new_nav, 1)
text = text.replace("caption, keyboard, photo_path = _find_result_card_content(item, index, len(ids))", "caption, keyboard, photo_path = _find_result_card_content(item, index, len(ids), ids)", 1)
old_photos_kb = "rows = [[InlineKeyboardButton('📋 租赁详情', callback_data=f'listing:detail:{listing_id}'), InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}')], [InlineKeyboardButton('🤖 侨联找房助手', callback_data='home_smart_search')]]"
new_photos_kb = "rows = [[InlineKeyboardButton('📋 租赁详情', callback_data=f'listing:detail:{listing_id}'), InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}')], [InlineKeyboardButton('💬 联系中文顾问', callback_data=f'listing:consult:{listing_id}')], [InlineKeyboardButton('⬅️ 返回这套房', callback_data=f'listing:open:{listing_id}')]]"
if old_photos_kb not in text:
    raise SystemExit('missing patch target: photo keyboard')
text = text.replace(old_photos_kb, new_photos_kb, 1)
p.write_text(text, encoding='utf-8')

# 5. Listing callback parser + routes.
p = Path('qiaolian_dual/callback_listing.py')
text = p.read_text(encoding='utf-8')
old_findcard = '''    if data.startswith('findcard:'):
            value = data.split(':', 1)[1]
            if value == 'noop':
                return MAIN
            try:
                index = int(value)
            except (TypeError, ValueError):
                await query.answer('这批推荐已失效，请重新找房。', show_alert=True)
                return MAIN
            await send_find_result_card(update, context, index, replace=True)
            return MAIN
'''
new_findcard = '''    if data.startswith('findcard:'):
            parts = data.split(':', 2)
            value = parts[1] if len(parts) > 1 else ''
            if value == 'noop':
                return MAIN
            try:
                index = int(value)
            except (TypeError, ValueError):
                return MAIN
            # New callbacks carry both real index and target listing_id; old
            # findcard:{index} callbacks remain compatible.
            if len(parts) == 3 and parts[2] not in {'', 'unknown'}:
                ids = list(context.user_data.get('find_card_listing_ids') or [])
                if 0 <= index < len(ids) and ids[index] != parts[2]:
                    return MAIN
            await send_find_result_card(update, context, index, replace=True)
            return MAIN
'''
if old_findcard not in text:
    raise SystemExit('missing patch target: findcard parser')
text = text.replace(old_findcard, new_findcard, 1)
old_photos = '''    if data.startswith('listing:photos:'):
            lid = data.split(':', 2)[2]
            await send_listing_photo_preview(context.bot, update.effective_chat.id, lid)
            return MAIN
'''
new_photos = '''    if data.startswith('listing:photos:'):
            lid = data.split(':', 2)[2]
            context.user_data['contact_listing_id'] = lid
            try:
                await send_listing_photo_preview(context.bot, update.effective_chat.id, lid)
            except Exception:
                logger.exception('完整相册发送失败: listing_id=%s', lid)
                await render_panel(
                    update,
                    text='<b>📸 完整实拍</b>\\n\\n这套房的实拍暂时无法加载。你可以稍后再试，或联系中文顾问补充图片。',
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton('💬 联系中文顾问', callback_data=f'listing:consult:{lid}')],
                        [InlineKeyboardButton('⬅️ 返回这套房', callback_data=f'listing:open:{lid}')],
                    ]),
                    context=context,
                )
            return MAIN
'''
if old_photos not in text:
    raise SystemExit('missing patch target: photos handler')
text = text.replace(old_photos, new_photos, 1)
# Remove availability gate from lease detail: details remain readable even when booking is unavailable.
old_detail = '''    if data.startswith('listing:detail:'):
            lid = data.split(':', 2)[2]
            is_available, availability_reason = listing_is_available(lid)
            if not is_available:
                await render_panel(update, text=listing_unavailable_text(availability_reason), reply_markup=listing_unavailable_keyboard(lid), context=context)
                return MAIN
            item = db.get_listing(lid) if lid else None
            if not item:
                await render_panel(update, text='未找到该房源详情，可能已下架。', reply_markup=main_keyboard())
                return MAIN
            create_lead(user, action='listing_detail_view', source='listing_landing', listing_id=lid)
            detail_rows = [[InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{lid}'), InlineKeyboardButton('💬 联系顾问', callback_data=f'listing:consult:{lid}')]]
            channel_url = _listing_channel_url(lid)
            if channel_url:
                detail_rows.append([InlineKeyboardButton('📸 全部实拍与留言区', url=channel_url)])
            detail_rows.append([InlineKeyboardButton('⬅️ 返回', callback_data='home')])
            detail_kb = InlineKeyboardMarkup(detail_rows)
            await render_panel(update, text=listing_detail_text(item), parse_mode=ParseMode.HTML, reply_markup=detail_kb)
            return MAIN
'''
new_detail = '''    if data.startswith('listing:detail:'):
            lid = data.split(':', 2)[2]
            item = db.get_listing(lid) if lid else None
            if not item:
                await render_panel(update, text='未找到该房源详情，可能已下架。', reply_markup=main_keyboard(), context=context)
                return MAIN
            context.user_data['contact_listing_id'] = lid
            create_lead(user, action='listing_detail_view', source='listing_landing', listing_id=lid)
            detail_kb = InlineKeyboardMarkup([
                [InlineKeyboardButton('📅 预约这套', callback_data=f'listing:appoint:{lid}')],
                [InlineKeyboardButton('📸 查看更多实拍', callback_data=f'listing:photos:{lid}')],
                [InlineKeyboardButton('💬 联系中文顾问', callback_data=f'listing:consult:{lid}')],
                [InlineKeyboardButton('⬅️ 返回这套房', callback_data=f'listing:open:{lid}')],
            ])
            await render_panel(update, text=listing_cost_text(lid), parse_mode=ParseMode.HTML, reply_markup=detail_kb, context=context)
            return MAIN
'''
if old_detail not in text:
    raise SystemExit('missing patch target: detail handler')
text = text.replace(old_detail, new_detail, 1)
# All unavailable callback panels must render HTML instead of exposing <b> tags.
text = text.replace("await render_panel(update, text=listing_unavailable_text(availability_reason), reply_markup=listing_unavailable_keyboard(lid), context=context)", "await render_panel(update, text=listing_unavailable_text(availability_reason), parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard(lid), context=context)")
p.write_text(text, encoding='utf-8')

# 6. Appointment flow unavailable panel also uses HTML and edits the current callback panel once.
p = Path('qiaolian_dual/flows.py')
text = p.read_text(encoding='utf-8')
text = text.replace("await render_panel(update, text=listing_unavailable_text(availability_reason), reply_markup=listing_unavailable_keyboard(listing_id), context=context)", "await render_panel(update, text=listing_unavailable_text(availability_reason), parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard(listing_id), context=context)")
p.write_text(text, encoding='utf-8')

# 7. Deep-link unavailable sends explicitly use HTML. These are message sends (not callbacks), so no duplicate edit path exists.
p = Path('qiaolian_dual/start_routes.py')
text = p.read_text(encoding='utf-8')
text = text.replace("await message.reply_text(listing_unavailable_text(availability_reason), reply_markup=listing_unavailable_keyboard(listing_id))", "await message.reply_text(listing_unavailable_text(availability_reason), parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard(listing_id))")
text = text.replace("listing_unavailable_text(availability_reason),\n                reply_markup=listing_unavailable_keyboard(listing_id),", "listing_unavailable_text(availability_reason),\n                parse_mode=ParseMode.HTML,\n                reply_markup=listing_unavailable_keyboard(listing_id),")
p.write_text(text, encoding='utf-8')

# 8. Search same-area fallback: only public active/reserved and no fallback_recent.
p = Path('qiaolian_dual/callback_search.py')
text = p.read_text(encoding='utf-8')
old_more = "matches = db.search_listings(areas=[area] if area and area != '不限' else None, limit=3)"
new_more = "matches = db.search_listings(areas=[area] if area and area not in {'不限', 'any'} else None, limit=5)"
if old_more not in text:
    raise SystemExit('missing patch target: unavailable same-area search')
text = text.replace(old_more, new_more, 1)
p.write_text(text, encoding='utf-8')

# 9. Route regression tests. They call the real central dispatcher and domain handlers;
# side-effect leaves are monkeypatched only to avoid Telegram/network/DB writes.
Path('tests/test_callback_route_repair.py').write_text(r'''import re
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
    assert mapping['🏠 可预约房源'] == 'hub:available'


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
    assert '📅 预约这套' in labels(seen['reply_markup'])


@pytest.mark.asyncio
async def test_listing_appoint_enters_appointment_flow_with_listing_id(monkeypatch):
    import qiaolian_dual.flows as flows
    seen = {}
    async def fake_start(update, context, listing_id, **kwargs):
        seen['listing_id'] = listing_id
        seen.update(kwargs)
        return common.APPT_MODE
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
    assert state == common.APPT_MODE
    assert context.user_data['appt']['listing_id'] == 'l_2'
    assert '预约看房' in seen['text']


@pytest.mark.asyncio
async def test_listing_photos_calls_complete_album_handler(monkeypatch):
    import qiaolian_dual.results_admin as results
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
    assert '<b>🏠 这套房暂时不能预约</b>' in text
    kb = listing_unavailable_keyboard('')
    assert labels(kb) == ['🔍 找附近房源', '💬 联系中文顾问', '🏠 同区推荐', '⬅️ 返回上一页']
    assert callbacks(kb) == ['findmode:guided', 'appointment_menu:contact', 'unavail:more:any', 'home']


@pytest.mark.asyncio
async def test_unavailable_callback_renders_once_with_html(monkeypatch):
    import qiaolian_dual.listing as listing_mod
    import qiaolian_dual.texts as texts
    monkeypatch.setattr(listing_mod, 'listing_is_available', lambda lid: (False, 'pending'))
    monkeypatch.setattr(listing_mod, 'listing_context', lambda lid: {'listing_id': lid, 'area': 'BKK1', 'status': 'pending'})
    seen = []
    async def fake_render(update, **kwargs):
        seen.append(kwargs)
    monkeypatch.setattr(texts, 'render_panel', fake_render)
    update, query = make_update('listing:open:l_2')
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
    # _find_result_card_content imports listing_context locally; patch source module too.
    import qiaolian_dual.listing as listing_mod
    monkeypatch.setattr(listing_mod, 'listing_context', lambda lid: data.get(lid, {}))
    _, kb, _ = _find_result_card_content(data['l_2'], 1, 3, ['l_1', 'l_2', 'l_3'])
    nav = kb.inline_keyboard[0]
    assert 'BKK1' in nav[0].text and '1房' in nav[0].text
    assert '永旺1' in nav[1].text and '1房' in nav[1].text
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
''', encoding='utf-8')

# 10. Static safety checks guard against the production regressions described in this task.
Path('tests/test_callback_route_static_guards.py').write_text(r'''from pathlib import Path


def test_no_detail_to_appointment_alias_and_no_photo_similar_fallback():
    source = Path('qiaolian_dual/callback_listing.py').read_text(encoding='utf-8')
    detail = source[source.index("if data.startswith('listing:detail:')"):source.index("if data.startswith('listing:similar:')")]
    assert "return await start_appointment" not in detail
    photos = source[source.index("if data.startswith('listing:photos:')"):source.index("if data == 'find:show_more'")]
    assert 'listing:similar:' not in photos


def test_reserved_is_explicitly_available():
    source = Path('qiaolian_dual/listing.py').read_text(encoding='utf-8')
    block = source[source.index('def listing_is_available'):source.index('def listing_unavailable_text')]
    assert "{'active', 'reserved'}" in block


def test_unavailable_callback_paths_use_html_parse_mode():
    for filename in ('qiaolian_dual/callback_listing.py', 'qiaolian_dual/flows.py', 'qiaolian_dual/start_routes.py'):
        source = Path(filename).read_text(encoding='utf-8')
        if 'listing_unavailable_text' in source:
            assert 'parse_mode=ParseMode.HTML' in source


def test_recommendation_card_no_longer_routes_detail_to_open():
    source = Path('qiaolian_dual/results_admin.py').read_text(encoding='utf-8')
    assert "InlineKeyboardButton('📋 租赁详情', callback_data=f'listing:detail:{listing_id}')" in source
''', encoding='utf-8')

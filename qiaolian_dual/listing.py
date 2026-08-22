"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

def listing_context(listing_id: str) -> dict:
    from .session_deeplink import _extract_caption_variant, _latest_draft_context
    listing_id = str(listing_id or '').strip()
    if not listing_id:
        return {}
    merged: dict = {}
    try:
        listing = db.get_listing(listing_id)
        if listing:
            merged.update(dict(listing))
    except Exception:
        logger.debug('用户 Bot 读取 listings 失败: %s', listing_id, exc_info=True)
    draft_ctx = _latest_draft_context(listing_id)
    if draft_ctx:
        for key in ('listing_id', 'area', 'layout', 'property_type', 'price', 'floor', 'size', 'title', 'project', 'deposit', 'available_date', 'cost_notes', 'normalized_data', 'water_rate', 'electric_rate'):
            if key not in merged or merged.get(key) in (None, '', 0, '0'):
                merged[key] = draft_ctx.get(key, merged.get(key))
        merged['caption_variant'] = _extract_caption_variant(draft_ctx.get('review_note'))
    if 'caption_variant' not in merged:
        merged['caption_variant'] = 'a'
    if not merged:
        return {'listing_id': listing_id, 'caption_variant': 'a'}
    merged.setdefault('listing_id', listing_id)
    return merged

def listing_cost_text(listing_id: str) -> str:
    """费用页只展示数据库已有事实，不把缺失字段编成确定价格。"""
    from .utils_formatting import _fmt_price
    item = listing_context(listing_id)
    title = str(item.get('project') or item.get('community') or item.get('title') or '这套房').strip()
    layout = str(item.get('layout') or item.get('property_type') or '').strip()
    price_value = item.get('price')
    price_text = _fmt_price(price_value)
    deposit = str(item.get('deposit') or item.get('deposit_rule') or '').strip()
    normalized: dict = {}
    raw_normalized = item.get('normalized_data')
    if isinstance(raw_normalized, dict):
        normalized = raw_normalized
    elif raw_normalized:
        try:
            normalized = json.loads(str(raw_normalized))
        except (TypeError, ValueError, json.JSONDecodeError):
            normalized = {}
    contract = str(normalized.get('contract_term') or item.get('contract_term') or '').strip()
    electric = str(item.get('electric_rate') or normalized.get('electric_rate') or '').strip()
    water = str(item.get('water_rate') or normalized.get('water_rate') or '').strip()
    management = str(normalized.get('management_fee') or '').strip()
    parking = str(normalized.get('parking_fee') or '').strip()
    lines = [f"💰 <b>{he(title)}{(f' · {he(layout)}' if layout else '')} · 费用明细</b>", '', f'月租：<b>{he(price_text)}</b>']
    if deposit:
        upfront = ''
        if re.search('押\\s*1\\s*付\\s*1|押一付一', deposit) and isinstance(price_value, (int, float)) and (price_value > 0):
            upfront = f'（预计首付 ${int(price_value) * 2:,}）'
        lines.append(f'押付：{he(deposit)}{upfront}')
    if contract:
        lines.append(f'合同：{he(contract)}')
    if electric:
        lines.append(f'电费：{he(electric)}')
    if water:
        lines.append(f'水费：{he(water)}')
    if management:
        lines.append(f'物业费：{he(management)}')
    if parking:
        lines.append(f'停车费：{he(parking)}')
    else:
        lines.append('停车费：如有需要，看房时可确认')
    advisor = str(ADVISOR_TG or '@pengqingw').strip()
    if advisor and (not advisor.startswith('@')) and ('t.me/' not in advisor):
        advisor = f'@{advisor}'
    lines.extend(['', f'还有不清楚的，直接回复问我，或联系顾问 {he(advisor)}'])
    return '\n'.join(lines)

def listing_cost_keyboard(listing_id: str) -> InlineKeyboardMarkup:
    from .keyboards_common import _advisor_listing_url
    return InlineKeyboardMarkup([[InlineKeyboardButton('📅 预约这套', callback_data=f'listing:appoint:{listing_id}'), InlineKeyboardButton('💬 联系顾问', url=_advisor_listing_url(listing_id))]])

def listing_is_available(listing_id: str) -> tuple[bool, str]:
    from .session_deeplink import _latest_draft_review_status
    listing_id = str(listing_id or '').strip()
    if not listing_id:
        return (False, 'missing')
    listing = db.get_listing(listing_id)
    if listing:
        status = str(listing.get('status') or 'active').strip().lower()
        if status != 'active':
            return (False, status or 'inactive')
        if not db.is_listing_public(listing_id):
            return (False, 'unpublished')
    draft_status = _latest_draft_review_status(listing_id)
    if draft_status in {'ready', 'published'}:
        return (True, draft_status)
    if draft_status:
        return (False, draft_status)
    return (False, 'missing')

def listing_unavailable_text(reason: str='') -> str:
    """根据房态给用户准确提示，不把所有不可预约状态都说成已租出。"""
    status = str(reason or '').strip().lower()
    if status == 'rented':
        return '<b>🔴 这套房源已租出</b>\n\n目前不能继续预约。可以看看同区域、同预算的类似房源，或者让顾问直接帮你匹配。'
    if status == 'reserved':
        return '<b>🔵 这套房源目前已预订</b>\n\n暂时不开放新的预约。可以先看类似房源，也可以联系顾问确认是否有候补机会。'
    if status == 'pending':
        return '<b>🟡 这套房源正在重新确认房态</b>\n\n为了避免白跑一趟，暂时不直接接受预约。顾问确认后可以继续安排。'
    if status in {'inactive', 'offline'}:
        return '<b>⚫ 这套房源目前已下架</b>\n\n可以看看同区域、同预算的类似房源。'
    return '<b>这套房源目前暂不可预约</b>\n\n可能正在更新或房态需要确认。可以先看类似房源，或者联系顾问帮你确认。'

def listing_unavailable_keyboard(listing_id: str='') -> InlineKeyboardMarkup:
    area = str(listing_context(listing_id).get('area') or '').strip()
    rows: list[list[InlineKeyboardButton]] = [[InlineKeyboardButton('🔍 继续找房', callback_data='findmode:guided')], [InlineKeyboardButton('💬 联系顾问', callback_data='appointment_menu:contact')]]
    if area and area != '不限':
        rows.append([InlineKeyboardButton('🏠 同区推荐', callback_data=f'unavail:more:{area}')])
    rows.append([InlineKeyboardButton('🏠 返回首页', callback_data='home')])
    return InlineKeyboardMarkup(rows)

def _store_active_entry(context: ContextTypes.DEFAULT_TYPE, *, arg: str, action: str, listing_id: str='', touch_payload: dict | None=None) -> None:
    from .session_deeplink import now_ts
    context.user_data['active_entry'] = {'arg': arg, 'action': action, 'listing_id': str(listing_id or '').strip(), 'touch_payload': dict(touch_payload or {}), 'saved_at': now_ts()}

def _active_entry_resume_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('▶️ 继续当前流程', callback_data='resume:continue')], [InlineKeyboardButton('🔄 重新开始本次入口', callback_data='resume:restart')], [InlineKeyboardButton('🏠 返回首页', callback_data='home')]])

def channel_topic_welcome_text(topic: str) -> str:
    topic = str(topic or '').strip().lower()
    topic_map = {'district_guide': '📍 已收到区域导流。\n\n想看这个区域的实拍房源，直接发预算和户型偏好，或点下方按钮开始找房。', 'service': '🧰 已进入侨联服务入口。\n\n想咨询代看、合同、入住协助或租后问题，直接点按钮就能接上顾问。', 'video_tour': '🎥 已进入视频代看入口。\n\n先发你要找的区域、预算、户型，我先推两套，再接顾问视频代看。'}
    return topic_map.get(topic, '已收到频道入口。\n\n告诉我你的需求，我马上帮你接上顾问或开始找房。')

def _resolve_area_from_target(target: str) -> tuple[str, str]:
    from .search import detect_area
    raw_target = str(target or '').strip()
    if not raw_target:
        return ('', '')
    listing_id = raw_target if raw_target.startswith('l_') else ''
    area = detect_area(raw_target)
    if area == raw_target[:40]:
        area = ''
    if listing_id:
        area = str(listing_context(listing_id).get('area') or area).strip()
    return (area, listing_id)

def _daily_listing_line(item: dict) -> str:
    from .utils_formatting import _fmt_price
    area = str(item.get('area') or '金边').strip() or '金边'
    layout = str(item.get('layout') or item.get('property_type') or '房源').strip() or '房源'
    return f"{he(area)}｜{he(layout)}｜{he(_fmt_price(item.get('price')))}"

def _latest_listing_text(limit: int=5) -> str:
    matches = db.list_recent_listings(limit)
    if not matches:
        return '🏠 <b>今日可看房源更新</b>\n\n暂时还没有可展示的最新房源，你可以先发区域和预算，我马上筛一轮。'
    lines = ['🏠 <b>今日可看房源更新</b>', '', '今日新增：', '']
    for item in matches[:limit]:
        lines.append(_daily_listing_line(item))
    lines.extend(['', '房源变动很快，', '看到合适的建议先咨询是否还在。', '', '侨联可以先帮你确认：', '• 是否可入住', '• 押金怎么收', '• 费用怎么算', '• 能不能视频代看'])
    return '\n'.join(lines)

def _resolve_video_pref_snapshot(context: ContextTypes.DEFAULT_TYPE) -> dict[str, object]:
    from .admin_contract import _budget_text

    def _int_or_none(v: object) -> int | None:
        if v in (None, ''):
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None
    snap = context.user_data.get('video_pref')
    snap = dict(snap) if isinstance(snap, dict) else {}
    area = str(snap.get('area') or '').strip()
    budget_min = _int_or_none(snap.get('budget_min'))
    budget_max = _int_or_none(snap.get('budget_max'))
    layout = str(snap.get('layout') or '').strip()
    pref = context.user_data.get('search_pref')
    if isinstance(pref, dict):
        if not area:
            area = str(pref.get('area') or '').strip()
        if not layout:
            goal = str(pref.get('goal') or '').strip()
            if goal and goal not in {'any', '住宅'}:
                layout = goal
    listing_id = str(context.user_data.get('contact_listing_id') or '').strip()
    if listing_id:
        info = listing_context(listing_id)
        if not area:
            area = str(info.get('area') or '').strip()
        if not layout:
            layout = str(info.get('layout') or info.get('property_type') or '').strip()
    area_display = area or '未填写'
    budget_display = _budget_text(budget_min, budget_max)
    if budget_display == '-':
        budget_display = '未填写'
    layout_display = layout or '未填写'
    return {'area': area, 'area_display': area_display, 'budget_min': budget_min, 'budget_max': budget_max, 'budget_display': budget_display, 'layout': layout, 'layout_display': layout_display}

def _video_tour_intro_text(*, area: str, budget: str, layout: str) -> str:
    return f'🎥 可以，侨联可以先帮你视频代看。\n\n适合这些情况：\n\n✔ 人还没到金边\n✔ 没时间一套套跑\n✔ 想先确认房子真实情况\n✔ 想看周边环境\n✔ 想提前看家具家电状态\n稍等，侨联找房助手正在为你从房源库检索...\n\n按你的需求：\n区域：{he(area)}\n预算：{he(budget)}\n户型：{he(layout)}'

def _video_tour_match_text(matches: list[dict], *, match_mode: str='strict') -> str:
    from .utils_formatting import _display_listing_id, _fmt_price
    if not matches:
        return '暂时没有完全匹配的在架房源，我先把你接给顾问优先人工匹配。'
    lines = ['已为你先匹配 2 套：', '']
    for idx, item in enumerate(matches[:2], start=1):
        area = str(item.get('area') or '金边').strip() or '金边'
        layout = str(item.get('layout') or item.get('property_type') or '房源').strip() or '房源'
        listing_id = str(item.get('listing_id') or '-').strip() or '-'
        lines.append(f"{idx}. {he(area)}｜{he(layout)}｜{he(_fmt_price(item.get('price')))}")
        lines.append(f'房源编号：<code>{he(_display_listing_id(listing_id))}</code>')
    if match_mode in {'no_type', 'no_area', 'budget_only', 'fuzzy', 'fallback_recent'}:
        lines.append('\n已自动放宽条件先给你匹配，顾问会继续人工精筛。')
    return '\n'.join(lines)

def _video_match_keyboard(matches: list[dict]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for item in matches[:2]:
        listing_id = str(item.get('listing_id') or '').strip()
        if listing_id:
            rows.append([InlineKeyboardButton(f'💬 咨询 {listing_id}', callback_data=f'appointment_menu:contact:listing:{listing_id}')])
    rows.append([InlineKeyboardButton('📅 安排视频代看', callback_data='appointment_menu:video')])
    rows.append([InlineKeyboardButton('🏠 查看更多房源', callback_data='hub:latest')])
    return InlineKeyboardMarkup(rows)

async def start_video_tour_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, *, source: str, area: str='', budget_min: int | None=None, budget_max: int | None=None, layout: str='') -> int:
    from .admin_contract import _user_contact_text, _user_mention_html
    from .keyboards_common import keyword_followup_keyboard, main_keyboard
    from .results_admin import _allow_admin_notify, _notify_admins
    from .search import create_lead, detect_property_type, search_listings_with_fallback
    from .session_deeplink import _remember_video_pref
    from .texts import render_panel
    _remember_video_pref(context, area=area or None, budget_min=budget_min, budget_max=budget_max, layout=layout or None)
    pref = _resolve_video_pref_snapshot(context)
    area_value = str(pref.get('area') or '')
    budget_lo = pref.get('budget_min')
    budget_hi = pref.get('budget_max')
    layout_value = str(pref.get('layout') or '')
    create_lead(update.effective_user, action='video_tour_click', source=source, area=area_value if area_value and area_value != '不限' else '', budget_min=budget_lo if isinstance(budget_lo, int) else None, budget_max=budget_hi if isinstance(budget_hi, int) else None, payload={'preferred_mode': 'video', 'layout': layout_value, 'area': area_value})
    notify_key = f'video_tour_click:{source}'
    if _allow_admin_notify(context, key=notify_key, cooldown_seconds=120):
        await _notify_admins(context, title='视频代看入口点击', lines=[f'用户：{_user_mention_html(update.effective_user)}', f'联系方式：{he(_user_contact_text(update.effective_user))}', f'来源：{he(source)}', f"区域：{he(str(pref.get('area_display') or '未填写'))}", f"预算：{he(str(pref.get('budget_display') or '未填写'))}", f"户型：{he(str(pref.get('layout_display') or '未填写'))}"])
    await render_panel(update, text=_video_tour_intro_text(area=str(pref.get('area_display') or '未填写'), budget=str(pref.get('budget_display') or '未填写'), layout=str(pref.get('layout_display') or '未填写')), parse_mode=ParseMode.HTML, reply_markup=main_keyboard(), context=context)
    has_condition = bool(area_value or layout_value or isinstance(budget_lo, int) or isinstance(budget_hi, int))
    if not has_condition:
        await update.effective_message.reply_text('请先发找房条件，我再推 2 套：例如 <code>BKK1 500-800 一房</code>。', parse_mode=ParseMode.HTML, reply_markup=keyword_followup_keyboard())
        return MAIN
    property_type = detect_property_type(layout_value)
    matches, match_mode = search_listings_with_fallback(property_type=property_type or None, area=area_value if area_value and area_value != '不限' else '', budget_min=budget_lo if isinstance(budget_lo, int) else None, budget_max=budget_hi if isinstance(budget_hi, int) else None, text_fragment=f"{area_value} {layout_value} {pref.get('budget_display')}", limit=2)
    await update.effective_message.reply_text(_video_tour_match_text(matches, match_mode=match_mode), parse_mode=ParseMode.HTML, reply_markup=_video_match_keyboard(matches))
    return MAIN

def _keyword_intro_text(*, area: str='', room_type: str='', budget_min: int | None=None, budget_max: int | None=None) -> str:
    from .admin_contract import _budget_text
    parts: list[str] = []
    if area:
        parts.append(f'区域：<b>{he(area)}</b>')
    if room_type:
        parts.append(f'户型：<b>{he(room_type)}</b>')
    if budget_min is not None or budget_max is not None:
        parts.append(f'预算：<b>{he(_budget_text(budget_min, budget_max))}</b>')
    if not parts:
        return '我先按你刚才的需求找一轮，你也可以继续补区域、预算或户型。'
    return '已按你的需求接上：' + ' ｜ '.join(parts)

def listing_landing_text(listing_id: str) -> str:
    """深链直达 - 删除中间页，直接跳转到目标动作"""
    return ''

def listing_landing_keyboard(listing_id: str, area: str='') -> InlineKeyboardMarkup:
    """深链进入按钮 - 2x2 布局"""
    return InlineKeyboardMarkup([[InlineKeyboardButton('📋 查看详情', callback_data=f'listing:detail:{listing_id}'), InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}')], [InlineKeyboardButton('💬 联系顾问', callback_data=f'listing:consult:{listing_id}'), InlineKeyboardButton('🏠 看相似房源', callback_data=f'listing:similar:{listing_id}')]])

"""User Bot 房源读取、详情、房态与兼容入口。"""
from __future__ import annotations

from .common import *


def listing_context(listing_id: str) -> dict:
    """读取当前房源，并补齐冻结发布包中的媒体。"""
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
        for key in (
            'listing_id', 'area', 'layout', 'property_type', 'price', 'floor', 'size',
            'title', 'project', 'deposit', 'available_date', 'cost_notes', 'normalized_data',
            'water_rate', 'electric_rate', 'contract_term', 'community',
        ):
            if key not in merged or merged.get(key) in (None, '', 0, '0'):
                merged[key] = draft_ctx.get(key, merged.get(key))
        merged['caption_variant'] = _extract_caption_variant(draft_ctx.get('review_note'))
    if 'caption_variant' not in merged:
        merged['caption_variant'] = 'a'
    if not merged:
        return {'listing_id': listing_id, 'caption_variant': 'a'}
    merged.setdefault('listing_id', listing_id)
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT main_images_json, discussion_images_json FROM publication_packages WHERE property_id=? AND status IN ('published','approved','package_ready') ORDER BY id DESC LIMIT 1",
                (listing_id,),
            ).fetchone()
        if row:
            images: list[str] = []
            for raw_images in row:
                parsed = json.loads(raw_images) if isinstance(raw_images, str) and raw_images else raw_images
                if isinstance(parsed, list):
                    images.extend(str(path) for path in parsed if isinstance(path, str) and path.strip())
            merged['media_files'] = list(dict.fromkeys(images))
        else:
            with sqlite3.connect(DB_PATH) as conn:
                post_row = conn.execute(
                    "SELECT publication_package_id FROM posts WHERE listing_id=? AND platform='telegram' AND publish_status IN ('published','success','ok') AND COALESCE(publication_package_id,'')<>'' ORDER BY id DESC LIMIT 1",
                    (listing_id,),
                ).fetchone()
            package_id = str(post_row[0] or '').strip() if post_row else ''
            package_root = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(DB_PATH))), 'media', 'publication_packages', package_id)
            if package_id and os.path.isdir(package_root):
                names = sorted(name for name in os.listdir(package_root) if re.fullmatch(r'image_\d+\.(?:jpg|jpeg|png|webp)', name, flags=re.I))
                merged['media_files'] = [os.path.join(package_root, name) for name in names]
    except Exception:
        logger.debug('读取房源多图包失败: %s', listing_id, exc_info=True)
    return merged


def _normalized_facts(item: dict) -> dict:
    raw = item.get('normalized_data')
    if isinstance(raw, dict):
        return raw
    if raw:
        try:
            parsed = json.loads(str(raw))
            return parsed if isinstance(parsed, dict) else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
    return {}


def _known_value(*values) -> str:
    for value in values:
        text = str(value or '').strip()
        if text and text not in {'待确认', '暂无', '[暂无]', '未知', '--', '-'}:
            return text
    return ''


def _critical_fee(value: str) -> str:
    return value if value else '待确认'


def listing_cost_text(listing_id: str) -> str:
    """房源决策页：基础信息 → 侨联说 → 房态。"""
    from .talk_engine import generate_talk
    from .utils_formatting import _display_floor, _display_layout, _fmt_price

    item = listing_context(listing_id)
    normalized = _normalized_facts(item)
    project = _known_value(item.get('project'), item.get('community'), item.get('area'))
    layout = _known_value(_display_layout(item.get('layout') or item.get('property_type'), item.get('property_type')))
    title = '｜'.join(value for value in (project, layout) if value) or '这套房'
    price = _fmt_price(item.get('price')) if item.get('price') not in (None, '', 0, '0') else '待确认'
    size = _known_value(item.get('size_sqm'), item.get('size'))
    if size and '㎡' not in size:
        size += '㎡'
    floor = _display_floor(item.get('floor'))
    deposit = _known_value(item.get('deposit'), item.get('deposit_rule'), normalized.get('deposit_payment_terms'))
    contract = _known_value(normalized.get('contract_term_display'), normalized.get('contract_term'), item.get('contract_term'))

    lines = ['🏠 <b>房源详情</b>', '', f'<b>{he(title)}</b>', f'💰 租金：<b>{he(price)}</b>']
    if size:
        lines.append(f'📐 面积：{he(size)}')
    if floor:
        lines.append(f'🏢 楼层：{he(floor)}')
    if contract:
        lines.append(f'📅 租期：{he(contract)}')
    if deposit:
        lines.append(f'🔐 押金：{he(deposit)}')

    talk = generate_talk(item, max_points=2, allow_empty=True).strip()
    if talk:
        safe_talk = '\n'.join(he(line) for line in talk.splitlines() if line.strip())
        lines.extend(['', '💬 <b>侨联说</b>', safe_talk])

    status = str(item.get('status') or 'pending').strip().lower()
    status_text = {
        'active': '🟢 当前可预约',
        'reserved': '🟡 已有预约 · 仍可预约',
        'pending': '🔵 房态确认中',
        'rented': '🔴 已租出',
        'inactive': '⚫ 已下架',
        'offline': '⚫ 已下架',
    }.get(status, '🔵 房态确认中')
    lines.extend(['', '<b>房态</b>', status_text])
    return '\n'.join(lines)


def listing_cost_keyboard(listing_id: str) -> InlineKeyboardMarkup:
    status = str(listing_context(listing_id).get('status') or 'pending').strip().lower()
    if status in {'active', 'reserved'}:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}'), InlineKeyboardButton('📸 更多实拍', callback_data=f'listing:photos:{listing_id}')],
            [InlineKeyboardButton('💬 联系我们', callback_data=f'listing:consult:{listing_id}')],
        ])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📸 更多实拍', callback_data=f'listing:photos:{listing_id}'), InlineKeyboardButton('💬 联系我们', callback_data=f'listing:consult:{listing_id}')],
        [InlineKeyboardButton('🏘 看相近房源', callback_data=f'unavail:more:{listing_id}')],
    ])


def listing_entry_text(listing_id: str) -> str:
    from .utils_formatting import _display_layout, _display_listing_id, _fmt_price
    info = listing_context(listing_id)
    qc = _display_listing_id(listing_id)
    project = str(info.get('project') or info.get('community') or info.get('area') or '这套房').strip()
    layout = _display_layout(info.get('layout') or info.get('property_type'), info.get('property_type'))
    subject = '｜'.join(v for v in (project, layout) if v)
    return f'🏠 <b>{he(subject)}</b>\n💰 <b>{he(_fmt_price(info.get("price")))}</b>\n🆔 {he(qc)}'


def listing_entry_keyboard(listing_id: str) -> InlineKeyboardMarkup:
    status = str(listing_context(listing_id).get('status') or 'active').strip().lower()
    rows = [[
        InlineKeyboardButton('📋 租赁详情', callback_data=f'listing:detail:{listing_id}'),
        InlineKeyboardButton('📸 更多实拍', callback_data=f'listing:photos:{listing_id}'),
    ]]
    if status in {'active', 'reserved'}:
        rows.append([InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}')])
    rows.append([InlineKeyboardButton('💬 联系我们', callback_data=f'listing:consult:{listing_id}')])
    rows.append([InlineKeyboardButton('🔍 继续找房', callback_data='home_smart_search')])
    return InlineKeyboardMarkup(rows)


def listing_is_available(listing_id: str) -> tuple[bool, str]:
    listing_id = str(listing_id or '').strip()
    if not listing_id:
        return (False, 'missing')
    listing = db.get_listing(listing_id)
    if not listing:
        return (False, 'missing')
    status = str(listing.get('status') or 'pending').strip().lower()
    if status in {'active', 'reserved'}:
        return ((True, status) if db.is_listing_public(listing_id) else (False, 'unpublished'))
    if status == 'pending':
        return (False, 'pending')
    if status == 'rented':
        return (False, 'rented')
    if status in {'offline', 'inactive'}:
        return (False, 'offline')
    return (False, status or 'pending')


def listing_action_allowed(listing_id: str, action: str) -> tuple[bool, str]:
    """不可预约不等于不可查看：详情、实拍和联系我们仍可访问。"""
    allowed, reason = listing_is_available(listing_id)
    if allowed:
        return (True, reason)
    normalized_action = str(action or '').strip().lower()
    if reason in {'pending', 'rented', 'offline'} and normalized_action in {'detail', 'photos', 'consult'}:
        return (True, reason)
    return (False, reason)


def listing_unavailable_text(reason: str='', listing_id: str='') -> str:
    normalized = str(reason or '').strip().lower()
    if normalized == 'rented':
        return '🔴 <b>这套房已经租出</b>\n\n类似的房子可能还有。\n\n我们可以继续帮您找同区域、相近预算和户型的房源。'
    if normalized == 'offline':
        return '⚫ <b>这套房目前已下架</b>\n\n可能是房源暂停出租，或信息已经更新。'
    return '🔵 <b>这套房正在确认最新房态</b>\n\n暂时无法提交预约。\n\n您可以先看看同区域其他可约房源，我们也可以帮您确认这套房。'


def listing_unavailable_keyboard(listing_id: str='') -> InlineKeyboardMarkup:
    info = listing_context(listing_id) if listing_id else {}
    area = str(info.get('area') or '').strip()
    area_token = area if area and area != '不限' else 'any'
    status = str(info.get('status') or 'pending').strip().lower()
    if status == 'rented':
        return InlineKeyboardMarkup([
            [InlineKeyboardButton('🏘 看相近房源', callback_data=f'unavail:more:{area_token}')],
            [InlineKeyboardButton('💬 联系我们', callback_data=f'listing:consult:{listing_id}')],
        ])
    if status in {'offline', 'inactive'}:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton('🔍 继续找房', callback_data='home_smart_search')],
            [InlineKeyboardButton('💬 联系我们', callback_data=f'listing:consult:{listing_id}')],
        ])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🏘 同区可约房源', callback_data=f'unavail:more:{area_token}')],
        [InlineKeyboardButton('💬 联系我们', callback_data=f'listing:consult:{listing_id}')],
        [InlineKeyboardButton('📋 租赁详情', callback_data=f'listing:detail:{listing_id}')],
    ])


def _store_active_entry(context: ContextTypes.DEFAULT_TYPE, *, arg: str, action: str, listing_id: str='', touch_payload: dict | None=None) -> None:
    from .session_deeplink import now_ts
    context.user_data['active_entry'] = {'arg': arg, 'action': action, 'listing_id': str(listing_id or '').strip(), 'touch_payload': dict(touch_payload or {}), 'saved_at': now_ts()}


def _active_entry_resume_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('▶️ 继续当前流程', callback_data='resume:continue')],
        [InlineKeyboardButton('🔄 重新开始本次入口', callback_data='resume:restart')],
        [InlineKeyboardButton('🏠 返回首页', callback_data='home')],
    ])


def channel_topic_welcome_text(topic: str) -> str:
    topic = str(topic or '').strip().lower()
    topic_map = {
        'district_guide': '📍 想住在哪个区域？',
        'service': '🛡 已进入侨联保障。',
        'video_tour': '🎥 可以安排视频看房。\n\n先选择具体房源，再从预约页切换为视频看房。',
    }
    return topic_map.get(topic, '告诉我您的需求就可以。')


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
    from .utils_formatting import _display_layout, _fmt_price
    area = str(item.get('area') or '金边').strip() or '金边'
    layout = _display_layout(item.get('layout') or item.get('property_type') or '房源', item.get('property_type')) or '房源'
    return f"{he(area)}｜{he(layout)}｜{he(_fmt_price(item.get('price')))}"


def _latest_listing_text(limit: int=5) -> str:
    matches = [item for item in db.list_recent_listings(limit) if str(item.get('status') or '').strip().lower() in {'active', 'reserved'}]
    if not matches:
        return '暂时没有可以安排看房的房源。'
    return '🏘 <b>最近可预约房源</b>\n\n下面这些房源目前可以申请看房。\n\n' + '\n'.join(_daily_listing_line(item) for item in matches[:limit])


def _resolve_video_pref_snapshot(context: ContextTypes.DEFAULT_TYPE) -> dict[str, object]:
    from .admin_contract import _budget_text
    snap = context.user_data.get('video_pref')
    snap = dict(snap) if isinstance(snap, dict) else {}
    area = str(snap.get('area') or '').strip()
    layout = str(snap.get('layout') or '').strip()
    try:
        budget_min = int(snap.get('budget_min')) if snap.get('budget_min') not in (None, '') else None
    except (TypeError, ValueError):
        budget_min = None
    try:
        budget_max = int(snap.get('budget_max')) if snap.get('budget_max') not in (None, '') else None
    except (TypeError, ValueError):
        budget_max = None
    budget_display = _budget_text(budget_min, budget_max)
    return {
        'area': area, 'area_display': area or '未填写',
        'budget_min': budget_min, 'budget_max': budget_max,
        'budget_display': budget_display if budget_display != '-' else '未填写',
        'layout': layout, 'layout_display': layout or '未填写',
    }


def _video_tour_intro_text(*, area: str, budget: str, layout: str) -> str:
    return f'🎥 <b>视频看房</b>\n\n区域：{he(area)}\n预算：{he(budget)}\n户型：{he(layout)}\n\n先选具体房源，再选择视频看房时间。'


def _video_tour_match_text(matches: list[dict], *, match_mode: str='strict') -> str:
    if not matches:
        return '暂时没有完全符合条件、可以预约的视频看房房源。'
    return '先从下面房源中选一套，再进入「预约看房」切换为视频看房。'


def _video_match_keyboard(matches: list[dict]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for item in matches[:2]:
        listing_id = str(item.get('listing_id') or '').strip()
        if listing_id:
            rows.append([InlineKeyboardButton('📋 租赁详情', callback_data=f'listing:detail:{listing_id}')])
    rows.append([InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')])
    rows.append([InlineKeyboardButton('🔍 继续找房', callback_data='home_smart_search')])
    return InlineKeyboardMarkup(rows)


async def start_video_tour_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, *, source: str, area: str='', budget_min: int | None=None, budget_max: int | None=None, layout: str='') -> int:
    from .results_admin import send_find_results_as_cards
    from .search import detect_property_type, search_listings_with_fallback
    from .session_deeplink import _remember_video_pref
    _remember_video_pref(context, area=area or None, budget_min=budget_min, budget_max=budget_max, layout=layout or None)
    property_type = detect_property_type(layout)
    matches, match_mode = search_listings_with_fallback(property_type=property_type or None, area=area or None, budget_min=budget_min, budget_max=budget_max, text_fragment='', limit=5)
    await send_find_results_as_cards(update, context, matches, match_mode)
    return MAIN


def _keyword_intro_text(*, area: str='', room_type: str='', budget_min: int | None=None, budget_max: int | None=None) -> str:
    from .admin_contract import _budget_text
    parts: list[str] = []
    if area:
        parts.append(area)
    if room_type:
        parts.append(room_type)
    if budget_min is not None or budget_max is not None:
        parts.append(_budget_text(budget_min, budget_max))
    return '｜'.join(parts)


def listing_landing_text(listing_id: str) -> str:
    return listing_entry_text(listing_id)


def listing_landing_keyboard(listing_id: str, area: str='') -> InlineKeyboardMarkup:
    return listing_entry_keyboard(listing_id)

"""User Bot 房源卡、实拍发送与管理号通知。"""
from __future__ import annotations

from .common import *
from .utils_formatting import _display_layout


def _listing_card_keyboard(listing_id: str, *, available: bool = True, nav: list[InlineKeyboardButton] | None = None) -> InlineKeyboardMarkup:
    from .media_flipper import search_card_keyboard

    return search_card_keyboard(listing_id, available=available, listing_nav=nav)


def _rent_usd(value: object) -> int | None:
    if isinstance(value, (int, float)) and value > 0:
        return int(value)
    raw = str(value or '').strip()
    if not raw:
        return None
    match = re.search(r'(\d[\d,]*)', raw.replace('，', ','))
    if not match:
        return None
    amount = int(match.group(1).replace(',', ''))
    return amount if amount > 0 else None


def _estimated_first_payment(item: dict, rent: int | None) -> int | None:
    """押+付月数 × 月租；优先 canonical 月数，其次解析「押X付Y」。"""
    if rent is None or rent <= 0:
        return None
    facts = item.get('normalized_data')
    if not isinstance(facts, dict):
        facts = {}
    months = 0
    try:
        deposit = int(facts.get('deposit_months') or 0)
        prepay = int(facts.get('prepay_months') or 0)
        months = deposit + prepay
    except (TypeError, ValueError):
        months = 0
    if months <= 0:
        payment = str(
            item.get('deposit')
            or item.get('deposit_rule')
            or item.get('payment_terms')
            or facts.get('deposit_payment_terms')
            or ''
        )
        match = re.search(r'押\s*(\d+)\s*付\s*(\d+)', payment)
        if match:
            months = int(match.group(1)) + int(match.group(2))
    return rent * months if months > 0 else None


async def send_listing_card(bot, chat_id: int, listing: dict, index: int=0, total: int=1) -> None:
    listing_id = str(listing.get('listing_id') or '').strip()
    caption, keyboard, photo_path = _find_result_card_content(listing, index, total, [listing_id] if listing_id else None)
    if photo_path:
        with open(photo_path, 'rb') as photo:
            await bot.send_photo(chat_id=chat_id, photo=photo, caption=caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    else:
        await bot.send_message(chat_id=chat_id, text=caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def send_find_results_as_cards(update: Update, context: ContextTypes.DEFAULT_TYPE, matches: list[dict], match_mode: str='strict') -> None:
    from .keyboards_common import no_match_followup_keyboard
    if not matches:
        await update.effective_message.reply_text(
            '🔍 <b>暂时没有完全符合条件的房源</b>\n\n不用重新开始。\n\n您可以稍微调整条件，或者先看看最接近需求的房源。',
            reply_markup=no_match_followup_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        return
    ids = [str(item.get('listing_id') or '').strip() for item in matches if item.get('listing_id')]
    context.user_data['find_card_listing_ids'] = ids
    context.user_data['find_card_match_mode'] = str(match_mode or 'strict')
    context.user_data['find_card_index'] = 0
    query = getattr(update, 'callback_query', None)
    from .listing import listing_context
    from .media_flipper import listing_media_paths
    first_item = listing_context(ids[0]) if ids else {}
    paths = listing_media_paths(first_item if isinstance(first_item, dict) else {})
    first_photo = paths[0] if paths else ''
    replace = bool(query is not None and (getattr(query.message, 'photo', None) or not first_photo))
    await send_find_result_card(update, context, 0, replace=replace)


def _find_result_card_content(item: dict, index: int, total: int, result_ids: list[str] | None=None) -> tuple[str, InlineKeyboardMarkup, str]:
    from .listing import listing_context
    from .text_utils import clean_inline_text
    from .utils_formatting import _display_floor, _display_layout, _fmt_price

    listing_id = str(item.get('listing_id') or '').strip()
    full = listing_context(listing_id)
    if full:
        item = {**item, **{k: v for k, v in full.items() if v not in (None, '', [], {})}}
    project = clean_inline_text(str(item.get('project') or item.get('community') or item.get('area') or '金边'))
    property_type = clean_inline_text(str(item.get('property_type') or ''))
    layout = _display_layout(clean_inline_text(str(item.get('layout') or item.get('property_type') or '房源')), property_type)
    rent_value = _rent_usd(item.get('price'))
    price = _fmt_price(item.get('price'))
    deposit = clean_inline_text(
        str(
            item.get('deposit')
            or item.get('deposit_rule')
            or item.get('payment_terms')
            or ''
        )
    )
    first_payment = _estimated_first_payment(item, rent_value)
    size = clean_inline_text(str(item.get('size_sqm') or item.get('size') or ''))
    floor = _display_floor(item.get('floor'))
    lease = clean_inline_text(str(item.get('contract_term') or ''))
    status = str(item.get('status') or 'active').strip().lower()
    status_text = '🟡 <b>已有预约 · 仍可预约</b>' if status == 'reserved' else '🟢 <b>当前可预约</b>'
    public_id = clean_inline_text(str(item.get('public_listing_id') or item.get('listing_id') or ''))

    title_bits = [value for value in (public_id, project, layout, price) if value]
    lines = [f'💰 <b>{he(" · ".join(title_bits) if title_bits else price or "房源")}</b>']
    if deposit and first_payment:
        lines.extend(['', f'{he(deposit)}｜预计首付 <b>${first_payment:,}</b>'])
    elif deposit:
        lines.extend(['', he(deposit)])
    elif first_payment:
        lines.extend(['', f'预计首付 <b>${first_payment:,}</b>'])
    property_bits = []
    if size:
        property_bits.append(f"{size}{'㎡' if '㎡' not in size else ''}")
    if floor:
        property_bits.append(floor)
    if lease:
        property_bits.append(lease)
    if property_bits:
        lines.append(f"📐 {he('｜'.join(property_bits))}")
    if item.get('area'):
        lines.append(f'📍 {he(str(item.get("area")))}')
    lines.extend(['', status_text, f'{index + 1}/{total}'])

    nav: list[InlineKeyboardButton] = []
    if total > 1:
        ids = list(result_ids or [])
        prev_index = (index - 1) % total
        next_index = (index + 1) % total
        prev_id = ids[prev_index] if len(ids) == total else ''
        next_id = ids[next_index] if len(ids) == total else ''
        nav = [
            InlineKeyboardButton('⬅️ 上一套', callback_data=f'findcard:{prev_index}:{prev_id}' if prev_id else f'findcard:{prev_index}'),
            InlineKeyboardButton('下一套 ➡️', callback_data=f'findcard:{next_index}:{next_id}' if next_id else f'findcard:{next_index}'),
        ]

    media_files = item.get('media_files') if isinstance(item.get('media_files'), list) else []
    from .media_flipper import listing_media_paths
    paths = listing_media_paths(item)
    photo_path = paths[0] if paths else ''
    if not photo_path:
        candidate = str(item.get('media_file_id') or '')
        photo_path = candidate if candidate and os.path.exists(candidate) else ''
    return ('\n'.join(lines), _listing_card_keyboard(listing_id, available=status in {'active', 'reserved'}, nav=nav), photo_path)


async def send_find_result_card(update: Update, context: ContextTypes.DEFAULT_TYPE, index: int, *, replace: bool=True) -> None:
    from .listing import listing_context, listing_is_available
    ids = list(context.user_data.get('find_card_listing_ids') or [])
    if not ids:
        await update.effective_message.reply_text('这批推荐已经失效，请重新选择找房条件。', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔍 帮我找房', callback_data='home_smart_search')]]))
        return
    valid_ids = [lid for lid in ids if listing_is_available(lid)[0]]
    if not valid_ids:
        context.user_data['find_card_listing_ids'] = []
        from .keyboards_common import no_match_followup_keyboard
        await update.effective_message.reply_text('这批推荐的房态已经变化。\n可以调整条件继续找。', reply_markup=no_match_followup_keyboard())
        return
    requested_id = ids[int(index) % len(ids)]
    context.user_data['find_card_listing_ids'] = valid_ids
    index = valid_ids.index(requested_id) if requested_id in valid_ids else min(int(index), len(valid_ids) - 1)
    item = listing_context(valid_ids[index])
    context.user_data['find_card_index'] = index
    caption, keyboard, photo_path = _find_result_card_content(item, index, len(valid_ids), valid_ids)
    query = getattr(update, 'callback_query', None)

    if replace and query is not None and getattr(query.message, 'photo', None) and photo_path:
        from telegram import InputMediaPhoto
        with open(photo_path, 'rb') as photo:
            await query.edit_message_media(media=InputMediaPhoto(media=photo.read(), caption=caption, parse_mode=ParseMode.HTML), reply_markup=keyboard)
        return
    if replace and query is not None and not getattr(query.message, 'photo', None) and not photo_path:
        await query.edit_message_text(caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return
    if replace and query is not None and getattr(query.message, 'photo', None) and not photo_path:
        await query.edit_message_caption(caption=caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return
    if photo_path:
        with open(photo_path, 'rb') as photo:
            sent = await context.bot.send_photo(chat_id=update.effective_chat.id, photo=photo, caption=caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    else:
        sent = await context.bot.send_message(chat_id=update.effective_chat.id, text=caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    context.user_data['find_card_anchor'] = {'chat_id': int(sent.chat_id), 'message_id': int(sent.message_id)}


def _format_match_line(item: dict) -> str:
    from .utils_formatting import _display_layout, _display_listing_id, _fmt_price
    listing_id = _display_listing_id(str(item.get('listing_id') or ''))
    project = str(item.get('project') or item.get('community') or item.get('area') or '金边')
    layout = _display_layout(item.get('layout') or item.get('property_type'), item.get('property_type'))
    return f"• <b>{he(listing_id)}</b>｜{he(project)}｜{he(layout)}｜{he(_fmt_price(item.get('price')))}"


def _format_listing_choice_lines(matches: list[dict]) -> str:
    return '\n'.join(_format_match_line(item) for item in matches[:3])


def search_results_keyboard(matches: list[dict]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for item in matches[:3]:
        listing_id = str(item.get('listing_id') or '').strip()
        if listing_id:
            rows.append([InlineKeyboardButton('📷 房源详情', callback_data=f'listing:photos:{listing_id}')])
    rows.extend([
        [InlineKeyboardButton('✏️ 调整条件', callback_data='home_smart_search')],
        [InlineKeyboardButton('💬 联系中文顾问', callback_data='hub:advisor')],
        [InlineKeyboardButton('🏠 返回首页', callback_data='home')],
    ])
    return InlineKeyboardMarkup(rows)


async def _notify_admins(context: ContextTypes.DEFAULT_TYPE, *, title: str, lines: list[str], reply_markup: InlineKeyboardMarkup | None=None, show_bell: bool=True) -> None:
    from .admin_contract import _all_user_admin_ids
    admin_ids = _all_user_admin_ids()
    if not admin_ids:
        return
    body = '\n'.join([line for line in lines if str(line or '').strip()])
    prefix = '🔔 ' if show_bell else ''
    text = f'{prefix}<b>{he(title)}</b>\n\n{body}'.strip()
    for admin_id in sorted(admin_ids):
        try:
            await context.bot.send_message(chat_id=admin_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=reply_markup)
        except Exception:
            logger.exception('发送管理号消息失败: admin_id=%s title=%s', admin_id, title)


def admin_lead_keyboard(*, lead_id: int | None, appointment_id: int, user_id: int) -> InlineKeyboardMarkup:
    suffix = f'{lead_id or 0}:{appointment_id}:{user_id}'
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('✅ 我来跟进', callback_data=f'adminlead:claim:{suffix}')],
        [InlineKeyboardButton('📞 已联系客户', callback_data=f'adminlead:contacted:{suffix}'), InlineKeyboardButton('🚫 结束跟进', callback_data=f'adminlead:invalid:{suffix}')],
    ])


def admin_repair_keyboard(ticket_id: int) -> InlineKeyboardMarkup:
    ticket = int(ticket_id or 0)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('✅ 已接手', callback_data=f'adminrepair:accepted:{ticket}'), InlineKeyboardButton('📅 已安排', callback_data=f'adminrepair:scheduled:{ticket}')],
        [InlineKeyboardButton('🔧 处理中', callback_data=f'adminrepair:in_progress:{ticket}'), InlineKeyboardButton('✅ 已完成', callback_data=f'adminrepair:done:{ticket}')],
        [InlineKeyboardButton('💬 需要客户补充', callback_data=f'adminrepair:need_info:{ticket}')],
    ])


def _allow_admin_notify(context: ContextTypes.DEFAULT_TYPE, *, key: str, cooldown_seconds: int=180) -> bool:
    box = context.user_data.setdefault('_notify_throttle', {})
    if not isinstance(box, dict):
        box = {}
    now_value = datetime.now().timestamp()
    last_ts = float(box.get(key) or 0)
    if now_value - last_ts < max(1, int(cooldown_seconds)):
        return False
    box[key] = now_value
    context.user_data['_notify_throttle'] = box
    return True



def _photo_action_keyboard(listing_id: str, *, available: bool, photo_index: int = 0, photo_total: int = 0, return_to_results: bool = False) -> InlineKeyboardMarkup:
    from .media_flipper import photo_flipper_keyboard

    return photo_flipper_keyboard(
        listing_id,
        available=available,
        photo_index=photo_index,
        photo_total=photo_total,
        return_to_results=return_to_results,
    )


async def send_listing_photo_preview(
    bot,
    chat_id: int,
    listing_id: str,
    *,
    query=None,
    photo_index: int = 0,
    return_to_results: bool = False,
    send_detail: bool = False,
) -> None:
    from .listing import listing_context, listing_cost_text
    from .media_flipper import send_or_edit_photo_flipper

    listing_id = str(listing_id or '').strip()
    info = listing_context(listing_id) if listing_id else {}
    await send_or_edit_photo_flipper(
        bot=bot,
        chat_id=chat_id,
        listing_id=listing_id,
        item=info if isinstance(info, dict) else {},
        photo_index=photo_index,
        query=query,
        return_to_results=return_to_results,
    )
    if send_detail:
        detail = listing_cost_text(listing_id)
        if detail:
            try:
                await bot.send_message(chat_id=chat_id, text=detail, parse_mode=ParseMode.HTML)
            except Exception:
                logger.debug('发送房源详情文本失败: %s', listing_id, exc_info=True)

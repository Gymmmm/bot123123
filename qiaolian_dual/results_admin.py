"""User Bot 房源卡、实拍发送与管理号通知。"""
from __future__ import annotations

from .common import *
from .utils_formatting import _display_layout


def _listing_card_keyboard(listing_id: str, *, available: bool = True, nav: list[InlineKeyboardButton] | None = None) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if nav:
        rows.append(nav)
    rows.append([
        InlineKeyboardButton('📋 租赁详情', callback_data=f'listing:detail:{listing_id}'),
        InlineKeyboardButton('📸 更多实拍', callback_data=f'listing:photos:{listing_id}'),
    ])
    if available:
        rows.append([InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}')])
    rows.append([InlineKeyboardButton('💬 联系中文顾问', callback_data=f'listing:consult:{listing_id}')])
    rows.append([InlineKeyboardButton('✏️ 换条件', callback_data='home_smart_search')])
    return InlineKeyboardMarkup(rows)


async def send_listing_card(bot, chat_id: int, listing: dict, index: int=0, total: int=1) -> None:
    """兼容旧调用：仍使用统一客户房源卡 renderer。"""
    listing_id = str(listing.get('listing_id') or '').strip()
    caption, keyboard, photo_path = _find_result_card_content(listing, index, total, [listing_id] if listing_id else None)
    if photo_path:
        with open(photo_path, 'rb') as photo:
            await bot.send_photo(chat_id=chat_id, photo=photo, caption=caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    else:
        await bot.send_message(chat_id=chat_id, text=caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def send_find_results_as_cards(update: Update, context: ContextTypes.DEFAULT_TYPE, matches: list[dict], match_mode: str='strict') -> None:
    """所有找房来源只发一张可切换卡片。"""
    from .keyboards_common import no_match_followup_keyboard
    if not matches:
        await update.effective_message.reply_text(
            '🔎 <b>暂时没有完全符合条件的房源</b>\n\n你可以调整一个条件继续找，\n也可以让中文顾问按这个需求继续留意。',
            reply_markup=no_match_followup_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        return
    ids = [str(item.get('listing_id') or '').strip() for item in matches if item.get('listing_id')]
    context.user_data['find_card_listing_ids'] = ids
    context.user_data['find_card_match_mode'] = str(match_mode or 'strict')
    query = getattr(update, 'callback_query', None)
    from .listing import listing_context
    first_item = listing_context(ids[0]) if ids else {}
    first_media = first_item.get('media_files') if isinstance(first_item.get('media_files'), list) else []
    first_photo = next((path for path in first_media if isinstance(path, str) and os.path.exists(path)), '')
    if not first_photo:
        candidate = str(first_item.get('media_file_id') or '')
        first_photo = candidate if candidate and os.path.exists(candidate) else ''
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
    price = _fmt_price(item.get('price'))
    size = clean_inline_text(str(item.get('size_sqm') or item.get('size') or ''))
    floor = _display_floor(item.get('floor'))
    deposit = clean_inline_text(str(item.get('deposit_rule') or item.get('deposit') or ''))
    contract = clean_inline_text(str(item.get('contract_term') or ''))
    status = str(item.get('status') or 'active').strip().lower()
    status_text = '🟡 <b>已有预约 · 仍可预约</b>' if status == 'reserved' else '🟢 <b>当前可预约</b>'

    lines = [f'🏠 <b>{he(project)}｜{he(layout)}</b>', f'💰 <b>{he(price)}</b>', '']
    house_bits = [value for value in (size + ('㎡' if size and '㎡' not in size else ''), floor) if value]
    if house_bits:
        lines.append(f"📐 {he('｜'.join(house_bits))}")
    rent_bits = [value for value in (deposit, contract) if value]
    if rent_bits:
        lines.append(f"🔑 {he('｜'.join(rent_bits))}")
    lines.extend(['', status_text, f'第 {index + 1}/{total} 套'])

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
    photo_path = next((p for p in media_files if isinstance(p, str) and os.path.exists(p)), '')
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
            rows.append([InlineKeyboardButton('📋 租赁详情', callback_data=f'listing:detail:{listing_id}')])
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


def _photo_action_keyboard(listing_id: str, *, available: bool) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton('📋 租赁详情', callback_data=f'listing:detail:{listing_id}')]]
    if available:
        rows[0].append(InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}'))
    rows.append([InlineKeyboardButton('💬 联系中文顾问', callback_data=f'listing:consult:{listing_id}')])
    return InlineKeyboardMarkup(rows)


async def send_listing_photo_preview(bot, chat_id: int, listing_id: str) -> None:
    """直接发送当前 QC 对应实拍；照片结束后只补一次操作消息。"""
    from .listing import listing_context
    from .utils_formatting import _display_listing_id
    from telegram import InputMediaPhoto

    info = listing_context(str(listing_id or '').strip())
    media_files = info.get('media_files', []) if isinstance(info, dict) else []
    photos = list(dict.fromkeys(
        p for p in media_files
        if isinstance(p, str) and os.path.exists(p)
        and os.path.basename(p).lower() not in {'cover.jpg', 'cover.jpeg', 'cover.png'}
    ))
    status = str(info.get('status') or 'active').strip().lower()
    keyboard = _photo_action_keyboard(listing_id, available=status in {'active', 'reserved'})
    qc = _display_listing_id(listing_id)

    if not photos:
        await bot.send_message(
            chat_id=chat_id,
            text=(
                f'📸 <b>更多实拍｜{he(qc)}</b>\n\n'
                '这套房的实拍暂时没有加载出来。\n\n'
                '可以稍后再试，\n或者让中文顾问直接补充给你。'
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )
        return

    for offset in range(0, len(photos), 10):
        chunk = photos[offset:offset + 10]
        media = []
        for path in chunk:
            with open(path, 'rb') as photo:
                media.append(InputMediaPhoto(media=photo.read()))
        if len(media) == 1:
            await bot.send_photo(chat_id=chat_id, photo=media[0].media)
        else:
            await bot.send_media_group(chat_id=chat_id, media=media)
    await bot.send_message(
        chat_id=chat_id,
        text=f'📸 <b>更多实拍｜{he(qc)}</b>\n\n以上是这套房目前保存的现场实拍。',
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )

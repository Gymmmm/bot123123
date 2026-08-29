"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *
from .utils_formatting import _display_layout

async def send_listing_card(bot, chat_id: int, listing: dict, index: int=0, total: int=1) -> None:
    """发送单个房源卡片（带图片）"""
    from .keyboards_common import _advisor_listing_url, _listing_channel_url
    from .text_utils import clean_inline_text, clean_highlights_for_card, remove_test_markers, fix_duplicate_words
    from .location_mapping import get_display_location
    from .utils_formatting import _display_layout, _display_listing_id
    listing_id = listing.get('listing_id', '')
    title = listing.get('title', '房源')
    area_raw = listing.get('area', '金边')
    community = listing.get('community', '')
    price = listing.get('price', 0)
    layout = listing.get('layout', '')
    size_sqm = listing.get('size_sqm', '')
    available_date = listing.get('available_date', '')
    deposit_rule = listing.get('deposit_rule', '')
    property_type = listing.get('property_type', '')
    highlights_raw = listing.get('highlights', '')
    media_file = listing.get('media_file_id', '')
    from .listing import listing_context
    full_listing = listing_context(listing_id) if listing_id else {}
    media_files = full_listing.get('media_files', []) if isinstance(full_listing, dict) else []
    logger.info('route=listing_card listing_id=%s send_mode=%s media_count=%s', listing_id or '-', 'media_group' if len(media_files) > 1 else ('photo' if media_file and os.path.exists(media_file) else 'text'), len(media_files) if isinstance(media_files, list) else 0)
    area = get_display_location(area_raw)
    title = remove_test_markers(clean_inline_text(title))
    area = remove_test_markers(clean_inline_text(area))
    community = remove_test_markers(clean_inline_text(community))
    layout = _display_layout(clean_inline_text(layout), property_type)
    available_date = fix_duplicate_words(clean_inline_text(available_date))
    deposit_rule = clean_inline_text(deposit_rule)
    property_type = clean_inline_text(property_type)
    highlights_clean = clean_highlights_for_card(highlights_raw)
    title_parts = []
    for value in (area, layout):
        if value and value not in title_parts:
            title_parts.append(value)
    public_id = _display_listing_id(listing_id) if listing_id else '房源'
    caption_parts = [f"<b>{he(public_id.upper())} · {'｜'.join((he(x) for x in title_parts)) or '金边房源'}</b>"]
    price_line = f'<b>${price}/月</b>'
    if size_sqm:
        price_line += f' · {he(size_sqm)}㎡'
    caption_parts.append(price_line)
    short_benefits = []
    if property_type:
        short_benefits.append(property_type)
    if deposit_rule:
        short_benefits.append(deposit_rule)
    if highlights_clean:
        short_benefits.append(highlights_clean.split(' · ')[0])
    if available_date:
        short_benefits.append(available_date)
    if short_benefits:
        caption_parts.extend(['', '｜'.join((he(x) for x in short_benefits[:2]))])
    caption = '\n'.join(caption_parts)
    keyboard_rows = [
        [InlineKeyboardButton('🏠 查看这套', callback_data=f'listing:open:{listing_id}')],
        [InlineKeyboardButton('💬 联系中文顾问', url=_advisor_listing_url(listing_id)), InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}')],
    ]
    channel_url = _listing_channel_url(listing_id)
    if channel_url:
        keyboard_rows.append([InlineKeyboardButton('📸 更多实拍', url=channel_url)])
    keyboard = InlineKeyboardMarkup(keyboard_rows)
    if media_file and listing_id not in media_file:
        logger.warning(f'图文不符！listing_id={listing_id}, media_file={media_file}')
        media_file = None
    available_photos = [p for p in media_files if isinstance(p, str) and os.path.exists(p)] if isinstance(media_files, list) else []
    media_group_sent = False
    if len(available_photos) > 1:
        try:
            from telegram import InputMediaPhoto
            media_group = []
            for idx, photo_path in enumerate(available_photos[:10]):
                with open(photo_path, 'rb') as photo:
                    media_group.append(InputMediaPhoto(media=photo.read(), caption=caption if idx == 0 else None, parse_mode=ParseMode.HTML if idx == 0 else None))
            await bot.send_media_group(chat_id=chat_id, media=media_group)
            await bot.send_message(chat_id=chat_id, text=f'🏠 {public_id.upper()}｜请选择这套房的下一步：', reply_markup=keyboard)
            media_group_sent = True
            logger.info('route=listing_card listing_id=%s result=media_group_success count=%s', listing_id or '-', len(media_group))
        except Exception as e:
            logger.warning('route=listing_card listing_id=%s result=media_group_failed fallback=single exception=%s', listing_id or '-', type(e).__name__)
    if not media_group_sent:
        if media_file and os.path.exists(media_file):
            try:
                with open(media_file, 'rb') as photo:
                    await bot.send_photo(chat_id=chat_id, photo=photo, caption=caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
                logger.info('route=listing_card listing_id=%s result=photo_success', listing_id or '-')
            except Exception as e:
                logger.warning('route=listing_card listing_id=%s result=photo_failed fallback=text exception=%s', listing_id or '-', type(e).__name__)
                await bot.send_message(chat_id=chat_id, text='📷 图片暂时无法显示，先看文字信息：\n\n' + caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
                logger.info('route=listing_card listing_id=%s result=text_fallback_success', listing_id or '-')
        else:
            await bot.send_message(chat_id=chat_id, text=caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            logger.info('route=listing_card listing_id=%s result=text_success', listing_id or '-')

async def send_find_results_as_cards(update: Update, context: ContextTypes.DEFAULT_TYPE, matches: list[dict], match_mode: str='strict') -> None:
    """搜索结果只发送一张可切换卡片，避免连续图片和消息把页面顶走。"""
    count = len(matches)
    if count == 0:
        await update.effective_message.reply_text('暂时没有完全符合条件的房源。\n\n可以换个预算或区域再试；也可以让中文顾问按你的需求继续留意。', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('💬 让中文顾问帮我找', callback_data='keyword:handoff')], [InlineKeyboardButton('✏️ 换个条件', callback_data='home_smart_search')], [InlineKeyboardButton('🏠 返回首页', callback_data='home')]]), parse_mode=ParseMode.HTML)
        return
    ids = [str(item.get('listing_id') or '').strip() for item in matches if item.get('listing_id')]
    context.user_data['find_card_listing_ids'] = ids
    context.user_data['find_card_match_mode'] = str(match_mode or 'strict')
    query = getattr(update, 'callback_query', None)
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
    # Telegram cannot convert a text message into a photo message. In that one case
    # send exactly one anchored card; never delete the home/search panel.
    await send_find_result_card(update, context, 0, replace=replace)


def _find_result_card_content(item: dict, index: int, total: int, result_ids: list[str] | None=None) -> tuple[str, InlineKeyboardMarkup, str]:
    from .listing import listing_context
    from .text_utils import clean_inline_text
    from .utils_formatting import _display_layout, _fmt_price
    listing_id = str(item.get('listing_id') or '').strip()
    full = listing_context(listing_id)
    if full:
        item = {**item, **{k: v for k, v in full.items() if v not in (None, '', [], {})}}
    area = clean_inline_text(str(item.get('project') or item.get('community') or item.get('area') or '金边'))
    property_type = clean_inline_text(str(item.get('property_type') or ''))
    layout = _display_layout(clean_inline_text(str(item.get('layout') or item.get('property_type') or '房源')), property_type)
    price = _fmt_price(item.get('price'))
    size = clean_inline_text(str(item.get('size_sqm') or item.get('size') or ''))
    deposit = clean_inline_text(str(item.get('deposit_rule') or item.get('deposit') or ''))
    status = str(item.get('status') or 'active').strip().lower()
    status_text = '🟡 <b>已有预约 · 仍可预约</b>' if status == 'reserved' else '🟢 <b>当前可预约</b>'
    lines = [
        f'<b>🏠 {he(area)}｜{he(layout)}</b>',
        f'💰 <b>{he(price)}</b>' + (f'　·　📐 {he(size)}㎡' if size else ''),
    ]
    extras = [value for value in (property_type, deposit) if value and value not in {layout, area}]
    if extras:
        lines.append(' · '.join(he(value) for value in extras[:2]))
    lines.extend(['', status_text, f'第 {index + 1}/{total} 套'])
    nav = []
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

        if len(ids) == total:
            nav = [
                InlineKeyboardButton(nav_label(prev_id, '上一套', left=True), callback_data=f'findcard:{prev_index}:{prev_id}'),
                InlineKeyboardButton(nav_label(next_id, '下一套', left=False), callback_data=f'findcard:{next_index}:{next_id}'),
            ]
        else:
            # Compatibility for callers that only provide index/total. Live cards
            # always pass result_ids and therefore use the named navigation above.
            nav = [
                InlineKeyboardButton('⬅️ 上一套', callback_data=f'findcard:{prev_index}'),
                InlineKeyboardButton('下一套 ➡️', callback_data=f'findcard:{next_index}'),
            ]
    rows = []
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton('📋 租赁详情', callback_data=f'listing:detail:{listing_id}'), InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}')])
    rows.append([InlineKeyboardButton('📸 查看更多实拍', callback_data=f'listing:photos:{listing_id}'), InlineKeyboardButton('💬 咨询这套', callback_data=f'listing:consult:{listing_id}')])
    rows.append([InlineKeyboardButton('✏️ 换条件', callback_data='home_smart_search')])
    media_files = item.get('media_files') if isinstance(item.get('media_files'), list) else []
    photo_path = next((p for p in media_files if isinstance(p, str) and os.path.exists(p)), '')
    if not photo_path:
        candidate = str(item.get('media_file_id') or '')
        photo_path = candidate if candidate and os.path.exists(candidate) else ''
    return ('\n'.join(lines), InlineKeyboardMarkup(rows), photo_path)

async def send_find_result_card(update: Update, context: ContextTypes.DEFAULT_TYPE, index: int, *, replace: bool=True) -> None:
    from .listing import listing_context
    ids = list(context.user_data.get('find_card_listing_ids') or [])
    if not ids:
        await update.effective_message.reply_text('这批推荐已经失效，请重新选择找房条件。', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔍 重新找房', callback_data='home_smart_search')], [InlineKeyboardButton('🏠 返回首页', callback_data='home')]]))
        return
    index = int(index) % len(ids)
    from .listing import listing_is_available
    valid_ids = []
    for lid in ids:
        is_available, _reason = listing_is_available(lid)
        if is_available:
            valid_ids.append(lid)
    if not valid_ids:
        context.user_data['find_card_listing_ids'] = []
        text = '这批推荐的房态都已经变化。\n可以换个条件，或让中文顾问继续帮你找。'
        kb = InlineKeyboardMarkup([[InlineKeyboardButton('✏️ 换条件', callback_data='home_smart_search')], [InlineKeyboardButton('💬 联系中文顾问', callback_data='keyword:handoff')]])
        query = getattr(update, 'callback_query', None)
        if replace and query is not None and getattr(query.message, 'photo', None):
            await query.edit_message_caption(caption=text, reply_markup=kb)
        elif replace and query is not None:
            await query.edit_message_text(text, reply_markup=kb)
        else:
            sent = await context.bot.send_message(chat_id=update.effective_chat.id, text=text, reply_markup=kb)
            context.user_data['find_card_anchor'] = {'chat_id': int(sent.chat_id), 'message_id': int(sent.message_id)}
        return
    requested_id = ids[index]
    context.user_data['find_card_listing_ids'] = valid_ids
    if requested_id in valid_ids:
        index = valid_ids.index(requested_id)
    else:
        index = min(index, len(valid_ids) - 1)
    ids = valid_ids
    item = listing_context(ids[index])
    caption, keyboard, photo_path = _find_result_card_content(item, index, len(ids), ids)
    query = getattr(update, 'callback_query', None)
    if replace and query is not None and getattr(query.message, 'photo', None) and photo_path:
        from telegram import InputMediaPhoto
        with open(photo_path, 'rb') as photo:
            await query.edit_message_media(media=InputMediaPhoto(media=photo.read(), caption=caption, parse_mode=ParseMode.HTML), reply_markup=keyboard)
        return
    if replace and query is not None and not getattr(query.message, 'photo', None):
        if not photo_path:
            await query.edit_message_text(caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            context.user_data['find_card_anchor'] = {
                'chat_id': int(update.effective_chat.id),
                'message_id': int(query.message.message_id),
            }
            return
        # Telegram cannot turn a text message into a photo in place. Keep the
        # home panel intact and create exactly one photo-card anchor; every
        # button on that card subsequently edits the same message.
        with open(photo_path, 'rb') as photo:
            sent = await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=photo,
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
        context.user_data['find_card_anchor'] = {
            'chat_id': int(sent.chat_id),
            'message_id': int(sent.message_id),
        }
        return
    if photo_path:
        with open(photo_path, 'rb') as photo:
            sent = await context.bot.send_photo(chat_id=update.effective_chat.id, photo=photo, caption=caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            context.user_data['find_card_anchor'] = {'chat_id': int(sent.chat_id), 'message_id': int(sent.message_id)}
    else:
        sent = await context.bot.send_message(chat_id=update.effective_chat.id, text=caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        context.user_data['find_card_anchor'] = {'chat_id': int(sent.chat_id), 'message_id': int(sent.message_id)}

def _format_match_line(item: dict) -> str:
    from .utils_formatting import _display_layout, _fmt_price
    head = f"• <b>{he(str(item.get('listing_id', '') or '-'))}</b> | {he(str(item.get('area', '') or '金边'))} | {he(_fmt_price(item.get('price')))}"
    detail_parts = []
    if item.get('layout'):
        detail_parts.append(_display_layout(item.get('layout'), item.get('property_type')))
    if item.get('size_sqm'):
        detail_parts.append(f"{item.get('size_sqm')}㎡")
    detail = f"\n  {he(' · '.join(detail_parts))}" if detail_parts else ''
    reminder = ''
    if item.get('drawbacks'):
        reminder = f"\n  ⚠️ {he(str(item.get('drawbacks'))[:60])}"
    return f'{head}{detail}{reminder}'

def _format_listing_choice_lines(matches: list[dict]) -> str:
    if not matches:
        return ''
    lines = [listing_match_intro_text()]
    for item in matches[:3]:
        lines.append(_format_match_line(item))
    lines.append(listing_match_footer_text())
    return '\n'.join(lines)

def search_results_keyboard(matches: list[dict]) -> InlineKeyboardMarkup:
    """让搜索结果可以直接点开，不要求用户复制房源编号。"""
    from .utils_formatting import _fmt_price
    rows: list[list[InlineKeyboardButton]] = []
    for item in matches[:3]:
        listing_id = str(item.get('listing_id') or '').strip()
        if not listing_id:
            continue
        area = str(item.get('area') or '金边').strip()
        layout = _display_layout(item.get('layout') or item.get('property_type') or '房源', item.get('property_type'))
        label = f"🏠 {area} · {layout} · {_fmt_price(item.get('price'))}"
        rows.append([InlineKeyboardButton(label[:55], callback_data=f'listing:open:{listing_id}')])
    rows.append([InlineKeyboardButton('✏️ 修改条件', callback_data='home_smart_search'), InlineKeyboardButton('💬 让中文顾问帮我找', callback_data='keyword:handoff')])
    rows.append([InlineKeyboardButton('🏠 返回首页', callback_data='home')])
    return InlineKeyboardMarkup(rows)

async def _notify_admins(context: ContextTypes.DEFAULT_TYPE, *, title: str, lines: list[str], reply_markup: InlineKeyboardMarkup | None=None, show_bell: bool=True) -> None:
    from .admin_contract import _all_user_admin_ids
    admin_ids = _all_user_admin_ids()
    if not admin_ids:
        return
    body = '\n'.join([line for line in lines if str(line or '').strip()])
    source_labels = {'channel_deeplink': '频道帖子', 'channel_post': '频道帖子', 'channel_index': '频道首页', 'channel_topic': '频道专题', 'user_search': '用户找房', 'home_layout': '按户型找房', 'listing_card': '房源详情页', 'listing_landing': '房源详情', 'appointment_hub': '预约中心', 'smart_find_play': '智能找房', 'help_inline': '帮助页', 'service_hub': '入住服务'}
    for raw, label in source_labels.items():
        body = body.replace(raw, label)
    prefix = '🔔 ' if show_bell else ''
    text = f'{prefix}<b>{he(title)}</b>\n\n{body}'.strip()
    for admin_id in sorted(admin_ids):
        try:
            await context.bot.send_message(chat_id=admin_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=reply_markup)
        except Exception:
            logger.exception('发送管理号消息失败: admin_id=%s title=%s', admin_id, title)

def admin_lead_keyboard(*, lead_id: int, appointment_id: int, user_id: int) -> InlineKeyboardMarkup:
    """顾问预约跟进按钮；callback 保持兼容，仅优化手机端措辞。"""
    suffix = f'{lead_id}:{appointment_id}:{user_id}'
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('✅ 我来跟进', callback_data=f'adminlead:claim:{suffix}')],
        [InlineKeyboardButton('📞 已联系客户', callback_data=f'adminlead:contacted:{suffix}'), InlineKeyboardButton('🚫 结束跟进', callback_data=f'adminlead:invalid:{suffix}')],
    ])

def admin_repair_keyboard(ticket_id: int) -> InlineKeyboardMarkup:
    ticket = int(ticket_id or 0)
    return InlineKeyboardMarkup([[InlineKeyboardButton('✅ 已接手', callback_data=f'adminrepair:accepted:{ticket}'), InlineKeyboardButton('📅 已安排', callback_data=f'adminrepair:scheduled:{ticket}')], [InlineKeyboardButton('🔧 处理中', callback_data=f'adminrepair:in_progress:{ticket}'), InlineKeyboardButton('✅ 已完成', callback_data=f'adminrepair:done:{ticket}')], [InlineKeyboardButton('💬 需要客户补充', callback_data=f'adminrepair:need_info:{ticket}')]])

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

async def send_listing_photo_preview(bot, chat_id: int, listing_id: str) -> None:
    """Send every frozen photo in original order, chunked by Telegram's 10-media limit."""
    from .listing import listing_context
    from .text_utils import clean_inline_text
    from telegram import InputMediaPhoto
    from .utils_formatting import _display_layout, _display_listing_id
    info = listing_context(str(listing_id or '').strip())
    media_files = info.get('media_files', []) if isinstance(info, dict) else []
    photos = list(dict.fromkeys(
        p for p in media_files
        if isinstance(p, str) and os.path.exists(p)
        and os.path.basename(p).lower() not in {'cover.jpg', 'cover.jpeg', 'cover.png'}
    ))
    area = clean_inline_text(str(info.get('project') or info.get('community') or info.get('area') or '金边'))
    layout = _display_layout(clean_inline_text(str(info.get('layout') or '')), info.get('property_type'))
    qc_id = _display_listing_id(str(listing_id or '').strip())
    heading = f'{qc_id}｜{area}' if area else qc_id
    caption_lines = ['<b>📸 完整实拍</b>', '', f'🏠 <b>{he(heading)}</b>']
    if layout:
        caption_lines.append(he(layout))
    caption_lines.append(f'以下是这套房的完整实拍，共 {len(photos)} 张。')
    caption_lines.append('点击图片查看大图。')
    caption = '\n'.join(caption_lines)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('📋 租赁详情', callback_data=f'listing:detail:{listing_id}'), InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}')],
        [InlineKeyboardButton('🤖 侨联找房助手', callback_data='home_smart_search')],
    ])
    if photos:
        for offset in range(0, len(photos), 10):
            chunk = photos[offset:offset + 10]
            media = []
            for index, path in enumerate(chunk):
                with open(path, 'rb') as photo:
                    first = offset == 0 and index == 0
                    media.append(InputMediaPhoto(media=photo.read(), caption=caption if first else None, parse_mode=ParseMode.HTML if first else None))
            if len(media) == 1:
                await bot.send_photo(chat_id=chat_id, photo=media[0].media, caption=media[0].caption, parse_mode=ParseMode.HTML)
            else:
                await bot.send_media_group(chat_id=chat_id, media=media)
        await bot.send_message(chat_id=chat_id, text=f'🏠 <b>{he(qc_id)}</b>\n请选择下一步', parse_mode=ParseMode.HTML, reply_markup=keyboard)
    else:
        await bot.send_message(
            chat_id=chat_id,
            text='<b>📸 完整实拍</b>\n\n这套房目前没有更多可用实拍。\n需要补充图片时，可以联系中文顾问。',
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )

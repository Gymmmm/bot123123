"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

async def send_listing_card(bot, chat_id: int, listing: dict, index: int=0, total: int=1) -> None:
    """发送单个房源卡片（带图片）"""
    from .keyboards_common import _advisor_listing_url
    from .text_utils import clean_inline_text, clean_highlights_for_card, remove_test_markers, fix_duplicate_words
    from .location_mapping import get_display_location
    listing_id = listing.get('listing_id', '')
    title = listing.get('title', '房源')
    area_raw = listing.get('area', '金边')
    community = listing.get('community', '')
    price = listing.get('price', 0)
    layout = listing.get('layout', '')
    size_sqm = listing.get('size_sqm', '')
    available_date = listing.get('available_date', '')
    highlights_raw = listing.get('highlights', '')
    media_file = listing.get('media_file_id', '')
    logger.info('route=listing_card listing_id=%s send_mode=%s', listing_id or '-', 'photo' if media_file and os.path.exists(media_file) else 'text')
    area = get_display_location(area_raw)
    title = remove_test_markers(clean_inline_text(title))
    area = remove_test_markers(clean_inline_text(area))
    community = remove_test_markers(clean_inline_text(community))
    layout = clean_inline_text(layout)
    available_date = fix_duplicate_words(clean_inline_text(available_date))
    highlights_clean = clean_highlights_for_card(highlights_raw)
    title_parts = []
    for value in (area, layout):
        if value and value not in title_parts:
            title_parts.append(value)
    caption_parts = [f"<b>{'｜'.join((he(x) for x in title_parts)) or '金边房源'}</b>"]
    price_line = f'<b>${price}/月</b>'
    if size_sqm:
        price_line += f' · {he(size_sqm)}㎡'
    caption_parts.append(price_line)
    short_benefits = []
    if highlights_clean:
        short_benefits.append(highlights_clean.split(' · ')[0])
    if available_date:
        short_benefits.append(available_date)
    if short_benefits:
        caption_parts.extend(['', '｜'.join((he(x) for x in short_benefits[:2]))])
    caption = '\n'.join(caption_parts)
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton('💬 联系顾问', url=_advisor_listing_url(listing_id)), InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}')]])
    if media_file and listing_id not in media_file:
        logger.warning(f'图文不符！listing_id={listing_id}, media_file={media_file}')
        media_file = None
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
    """以卡片形式发送找房结果"""
    chat_id = update.effective_chat.id
    bot = context.bot
    count = len(matches)
    if count == 0:
        await update.effective_message.reply_text('暂时没有完全符合条件的房源。\n\n你可以调整预算或区域，也可以直接告诉我们你的要求。', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('💬 联系我们', callback_data='keyword:handoff')], [InlineKeyboardButton('✏️ 换个条件', callback_data='home_smart_search')], [InlineKeyboardButton('🏠 返回首页', callback_data='home')]]), parse_mode=ParseMode.HTML)
        return
    if match_mode == 'strict':
        intro = f'为你找到 <b>{count}</b> 套比较合适的房源：'
    elif match_mode in {'no_type', 'no_area', 'budget_only'}:
        intro = f'已放宽条件，为你找到 <b>{count}</b> 套接近的房源：'
    else:
        intro = f'为你找到 <b>{count}</b> 套房源：'
    await update.effective_message.reply_text(intro, parse_mode=ParseMode.HTML)
    display_count = min(3, count)
    for i, listing in enumerate(matches[:display_count]):
        await send_listing_card(bot, chat_id, listing, i + 1, display_count)
        await asyncio.sleep(0.5)
    if count > 3:
        context.user_data['find_more_listing_ids'] = [str(item.get('listing_id') or '') for item in matches[3:] if item.get('listing_id')]
        more_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(f'再看 {count - 3} 套', callback_data='find:show_more'), InlineKeyboardButton('💬 联系我们', callback_data='keyword:handoff')], [InlineKeyboardButton('🏠 返回首页', callback_data='home')]])
        await bot.send_message(chat_id=chat_id, text=f'还有 {count - 3} 套符合你的条件。', reply_markup=more_keyboard)
    else:
        context.user_data.pop('find_more_listing_ids', None)
        final_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton('✏️ 换个条件', callback_data='home_smart_search'), InlineKeyboardButton('💬 联系我们', callback_data='keyword:handoff')], [InlineKeyboardButton('🏠 返回首页', callback_data='home')]])
        await bot.send_message(chat_id=chat_id, text='—', reply_markup=final_keyboard)

def _format_match_line(item: dict) -> str:
    from .utils_formatting import _fmt_price
    head = f"• <b>{he(str(item.get('listing_id', '') or '-'))}</b> | {he(str(item.get('area', '') or '金边'))} | {he(_fmt_price(item.get('price')))}"
    detail_parts = []
    if item.get('layout'):
        detail_parts.append(str(item.get('layout')))
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
        layout = str(item.get('layout') or item.get('property_type') or '房源').strip()
        label = f"🏠 {area} · {layout} · {_fmt_price(item.get('price'))}"
        rows.append([InlineKeyboardButton(label[:55], callback_data=f'listing:open:{listing_id}')])
    rows.append([InlineKeyboardButton('✏️ 修改条件', callback_data='home_smart_search'), InlineKeyboardButton('💬 让顾问帮我找', callback_data='keyword:handoff')])
    rows.append([InlineKeyboardButton('🏠 返回首页', callback_data='home')])
    return InlineKeyboardMarkup(rows)

async def _notify_admins(context: ContextTypes.DEFAULT_TYPE, *, title: str, lines: list[str], reply_markup: InlineKeyboardMarkup | None=None) -> None:
    from .admin_contract import _all_user_admin_ids
    admin_ids = _all_user_admin_ids()
    if not admin_ids:
        return
    body = '\n'.join([line for line in lines if str(line or '').strip()])
    text = f'🔔 <b>{he(title)}</b>\n\n{body}'.strip()
    for admin_id in sorted(admin_ids):
        try:
            await context.bot.send_message(chat_id=admin_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=reply_markup)
        except Exception:
            logger.exception('发送管理号消息失败: admin_id=%s title=%s', admin_id, title)

def admin_lead_keyboard(*, lead_id: int, appointment_id: int, user_id: int) -> InlineKeyboardMarkup:
    suffix = f'{lead_id}:{appointment_id}:{user_id}'
    return InlineKeyboardMarkup([[InlineKeyboardButton('✅ 我来接单', callback_data=f'adminlead:claim:{suffix}'), InlineKeyboardButton('💬 已联系', callback_data=f'adminlead:contacted:{suffix}')], [InlineKeyboardButton('❌ 无效线索', callback_data=f'adminlead:invalid:{suffix}')]])

def _allow_admin_notify(context: ContextTypes.DEFAULT_TYPE, *, key: str, cooldown_seconds: int=180) -> bool:
    """简单节流：避免同一用户短时间重复点击导致管理号刷屏。"""
    from .session_deeplink import now_ts
    box = context.user_data.setdefault('_notify_throttle', {})
    if not isinstance(box, dict):
        box = {}
    now_ts = datetime.now().timestamp()
    last_ts = float(box.get(key) or 0)
    if now_ts - last_ts < max(1, int(cooldown_seconds)):
        return False
    box[key] = now_ts
    context.user_data['_notify_throttle'] = box
    return True

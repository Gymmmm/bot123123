"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

def welcome_text() -> str:
    return (
        '🏠 <b>侨联地产｜您在金边的自己人</b>\n\n'
        '在金边租房，找到房子只是第一步。\n\n'
        '房子怎么样？费用怎么算？押金怎么退？住进去以后谁来帮？\n\n'
        '这些容易踩坑的地方，侨联先帮您看清楚。\n\n'
        '您可以直接告诉我需求：\n\n'
        '「BKK1 两房，$800以内」\n'
        '「富力城一房，要能做饭」\n'
        '「想住安静一点，高楼层」\n'
        '「先帮我看看房子视频」\n\n'
        '您说需求，我们帮您筛。\n\n'
        '👇 也可以直接选择'
    )

def channel_welcome_text(first_name: str='') -> str:
    return welcome_text()

def discussion_entry_welcome_text(first_name: str='', listing_id: str='') -> str:
    return copy_discussion_entry_welcome_text(first_name=first_name, listing_id=listing_id)

def lead_capture_text() -> str:
    return copy_lead_capture_text()

def _channel_index_action(action: str) -> dict | None:
    mapping = {'find_area': {'action': 'index_area', 'target': '', 'post_token': '', 'channel_message_id': None}, 'find_budget': {'action': 'index_budget', 'target': '', 'post_token': '', 'channel_message_id': None}, 'find_layout': {'action': 'index_layout', 'target': '', 'post_token': '', 'channel_message_id': None}, 'latest': {'action': 'index_latest', 'target': '', 'post_token': '', 'channel_message_id': None}, 'video': {'action': 'index_video', 'target': '', 'post_token': '', 'channel_message_id': None}, 'advisor': {'action': 'index_advisor', 'target': '', 'post_token': '', 'channel_message_id': None}, 'service': {'action': 'index_service', 'target': '', 'post_token': '', 'channel_message_id': None}}
    return mapping.get(action)

def _personal_greeting(update: Update) -> str:
    user = getattr(update, 'effective_user', None)
    name = getattr(user, 'first_name', '') or getattr(user, 'full_name', '') or getattr(user, 'username', '') or '您'
    hour = datetime.now(ZoneInfo('Asia/Phnom_Penh')).hour
    if 5 <= hour < 12:
        salutation = '上午好'
    elif 12 <= hour < 18:
        salutation = '下午好'
    else:
        salutation = '晚上好'
    return f'👋 <b>{he(str(name))}</b>，{salutation}'

async def render_panel(update: Update, *, text: str, reply_markup: InlineKeyboardMarkup | None=None, parse_mode: str | None=None, context: ContextTypes.DEFAULT_TYPE | None=None, prefer_edit_anchor: bool=False) -> None:
    query = getattr(update, 'callback_query', None)
    if query is not None and query.message is not None:
        kwargs: dict[str, object] = {'text': text, 'reply_markup': reply_markup}
        if parse_mode:
            kwargs['parse_mode'] = parse_mode
        try:
            _edit_text = str(kwargs.pop('text', ''))
            await query.edit_message_text(_edit_text, **kwargs)
            if context is not None:
                context.user_data[PANEL_ANCHOR_KEY] = {'chat_id': int(query.message.chat_id), 'message_id': int(query.message.message_id)}
            return
        except Exception as exc:
            if 'message is not modified' not in str(exc).lower():
                logger.debug('render_panel edit failed, fallback to send: %s', exc)
    if context is not None and prefer_edit_anchor and (query is None):
        anchor = context.user_data.get(PANEL_ANCHOR_KEY) or {}
        chat_id = anchor.get('chat_id')
        message_id = anchor.get('message_id')
        if isinstance(chat_id, int) and isinstance(message_id, int):
            kwargs_anchor: dict[str, object] = {'chat_id': chat_id, 'message_id': message_id, 'text': text}
            if reply_markup is not None:
                kwargs_anchor['reply_markup'] = reply_markup
            if parse_mode:
                kwargs_anchor['parse_mode'] = parse_mode
            try:
                await update.get_bot().edit_message_text(**kwargs_anchor)
                return
            except Exception as exc:
                logger.debug('render_panel anchor edit failed, fallback to send: %s', exc)
    msg = update.effective_message
    kwargs2: dict[str, object] = {'text': text, 'reply_markup': reply_markup}
    if parse_mode:
        kwargs2['parse_mode'] = parse_mode
    _reply_text = str(kwargs2.pop('text', ''))
    sent = await msg.reply_text(_reply_text, **kwargs2)
    if context is not None:
        context.user_data[PANEL_ANCHOR_KEY] = {'chat_id': int(sent.chat_id), 'message_id': int(sent.message_id)}

def promise_text() -> str:
    return ('🛡 <b>侨联保障</b>\n\n重要的事，先帮您看清楚。\n费用、入住留档和退租核对，都尽量提前说清。')

def deposit_text() -> str:
    return copy_deposit_text()

def advisor_text() -> str:
    return ('💬 <b>联系我们</b>\n\n有什么需要，直接告诉我们。\n具体房源会自动带上，不用重复说明。')

def advisor_handoff_text(*, listing_id: str='', user_id: int | None=None) -> str:
    from .admin_contract import _binding_end_date
    from .listing import listing_context
    from .utils_formatting import _display_layout, _fmt_price
    listing_id = str(listing_id or '').strip()
    if listing_id:
        item = listing_context(listing_id)
        area = str(item.get('area') or '金边')
        layout = _display_layout(item.get('layout') or item.get('property_type'), item.get('property_type')) or '房源'
        price_text = _fmt_price(item.get('price'))
        return f"💬 <b>已记录您咨询的房源</b>\n\n🏠 <b>{he(area)}｜{he(layout)}</b>\n💰 <b>{he(price_text)}</b>\n\n房源信息已经带上，不用重新说明。"
    if user_id:
        binding = db.get_active_binding(user_id)
        if binding:
            property_name = str(binding.get('property_name') or '').strip()
            end_date = str(_binding_end_date(binding) or '').strip()
            facts = []
            if property_name:
                facts.append(f"🏠 <b>当前房源：</b> {he(property_name)}")
            if end_date:
                facts.append(f"📅 <b>到期：</b> {he(end_date)}")
            details = ('\n\n' + '\n'.join(facts)) if facts else ''
            return f"💬 <b>联系我们</b>{details}\n\n我们会按您当前的租约继续跟进。"
    return advisor_text()

def smart_search_text() -> str:
    return copy_smart_search_text()

def about_text() -> str:
    return (
        '🏠 <b>关于侨联</b>\n\n'
        '来到金边，很多事情都需要重新熟悉。\n\n'
        '从住哪里、怎么租，到签约、入住，再到日常生活中遇到的问题。\n\n'
        '侨联想做的，不只是给您发几套房源。\n\n'
        '找房时，帮您多看一步；\n'
        '签约时，帮您多问一句；\n'
        '住下以后，有事还能找到人。\n\n'
        '认识侨联 · 选择侨联 · 信赖侨联\n\n'
        '我们希望成为您在金边，愿意长期联系的那个自己人。'
    )

def brand_story_text() -> str:
    return about_text()

def help_text() -> str:
    return (
        '❓ <b>怎么使用</b>\n\n'
        '找房：点“帮我找房”，也可以直接发区域、预算和户型。\n'
        '看房：从具体房源点“预约看房”，日期页可切换实地/视频。\n'
        '咨询：所有页面统一点“联系我们”。\n'
        '入住后：报修、物业和生活服务都在“入住服务”。'
    )

def service_hub_text() -> str:
    return ('🛠 <b>入住服务</b>\n\n入住后的常用事项放在这里。\n留档、押金、设备问题，直接点就行。')

def local_life_text() -> str:
    return copy_local_life_text()

def rfcity_text() -> str:
    return copy_rfcity_text()

def want_home_prompt_text() -> str:
    return copy_want_home_text()

def want_home_ack_text() -> str:
    return copy_want_home_ack_text()

def listing_detail_text(item: dict) -> str:
    from .talk_engine import generate_talk

    base = copy_listing_detail(item)
    talk = generate_talk(item, max_points=2, allow_empty=True).strip()
    if not talk:
        return base
    safe_talk = '\n'.join(he(line) for line in talk.splitlines() if line.strip())
    return f"{base}\n\n💬 <b>侨联说</b>\n{safe_talk}"

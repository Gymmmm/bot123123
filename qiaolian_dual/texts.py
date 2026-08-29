"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

def welcome_text() -> str:
    return copy_channel_welcome_text()

def channel_welcome_text(first_name: str='') -> str:
    return copy_channel_welcome_text(first_name=first_name)

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
    return ('🛡 <b>租期服务保障</b>\n\n绑定租客档案后：\n• 租约到期前 <b>7 天</b>提醒你确认是否续租\n• 报修提交即生成工单，处理进度会通知你\n\n每一项都有记录，不只是口头跟进。')

def deposit_text() -> str:
    return copy_deposit_text()

def advisor_text() -> str:
    return ('✅ <b>中文顾问已收到</b>\n\n中文顾问会通过 Telegram 联系你。\n如果是具体房源，房源信息会自动带上，不用重复说明。')

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
        return f"✅ <b>收到，我帮你联系中文顾问</b>\n\n🏠 {he(area)}｜{he(layout)}\n💰 <b>{he(price_text)}</b>\n\n房源信息已经带上，不用再重复发送。"
    if user_id:
        binding = db.get_active_binding(user_id)
        if binding:
            return f"✅ <b>中文顾问已收到</b>\n\n🏠 当前房源：<b>{he(str(binding.get('property_name') or '待确认'))}</b>\n📅 到期：<b>{he(_binding_end_date(binding) or '待确认')}</b>\n\n中文顾问会按你当前的租约继续跟进。"
    return advisor_text()

def smart_search_text() -> str:
    return copy_smart_search_text()

def about_text() -> str:
    return copy_about_text().replace('联系我们：', '联系中文顾问：')

def brand_story_text() -> str:
    return copy_brand_text()

def help_text() -> str:
    return ('❓ <b>怎么使用</b>\n\n找房：点“帮我找房”，选择类型、位置和预算。\n看房：打开一套可预约房源，点“预约看房”。\n咨询：点“联系中文顾问”，具体房源会自动带上。\n入住后：报修、物业沟通和周边生活都在“入住服务”。\n租约：可在“我的租约”查看，到期前 7 天可开启提醒。')

def service_hub_text() -> str:
    return ('🛠 <b>入住服务</b>\n\n房子有问题或需要物业沟通，直接点下面办理。\n侨联在租客户绑定租约后，报修会自动带上房屋信息；租约到期前 7 天会提醒你确认是否续租。')

def local_life_text() -> str:
    return copy_local_life_text()

def rfcity_text() -> str:
    return copy_rfcity_text()

def want_home_prompt_text() -> str:
    return copy_want_home_text()

def want_home_ack_text() -> str:
    return copy_want_home_ack_text()

def listing_detail_text(item: dict) -> str:
    return copy_listing_detail(item)

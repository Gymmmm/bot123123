"""Customer-facing rental-service guide flow for bound tenants."""
from __future__ import annotations

from pathlib import Path

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .common import MAIN


_CALLBACKS = {
    'hub:rental',
    'hub:rental:fees',
    'hub:rental:handover',
    'hub:rental:handover:preview',
    'hub:rental:handover:details',
    'hub:rental:handover:pdf',
    'hub:rental:deposit',
    'hub:rental:deposit:pdf',
    'hub:rental:viewing',
    'hub:rental:moving',
    'hub:rental:about',
    'service:handover',
    'service:deposit',
}


def matches(data: str) -> bool:
    return data in _CALLBACKS


def rental_home_text() -> str:
    return (
        '📄 <b>租赁服务指南</b>\n\n'
        '租房过程中，几个重要节点建议提前留好记录。\n\n'
        '<b>1. 签约前</b>\n'
        '确认租金、押金、水电、物业、网络及其他费用。\n\n'
        '<b>2. 入住时</b>\n'
        '记录房屋现状、家具家电、水电表和钥匙/门卡。\n\n'
        '<b>3. 退租时</b>\n'
        '按合同和入住留档逐项核对房屋、费用及押金。'
    )


def rental_home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📋 入住交接清单', callback_data='hub:rental:handover'), InlineKeyboardButton('🔐 退租与押金说明', callback_data='hub:rental:deposit')],
        [InlineKeyboardButton('🔄 续租', callback_data='contract:renew'), InlineKeyboardButton('🚪 退租', callback_data='contract:terminate')],
        [InlineKeyboardButton('⬅️ 返回已入住用户', callback_data='service:hub')],
    ])


def handover_text() -> str:
    return (
        '📋 <b>入住交接清单</b>\n\n'
        '用于入住当天逐项检查并留档。\n\n'
        '建议验房时边检查、边拍照记录，包括：\n'
        '• 房屋整体和已有瑕疵\n'
        '• 家具家电状态\n'
        '• 水表、电表读数\n'
        '• 钥匙、门卡数量\n'
        '• 双方确认的费用和其他事项\n\n'
        '需要保存到手机时，再点击下方下载。'
    )


def handover_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📥 下载入住交接清单', callback_data='hub:rental:handover:pdf')],
        [InlineKeyboardButton('🔐 查看退租与押金说明', callback_data='hub:rental:deposit')],
        [InlineKeyboardButton('⬅️ 返回租赁服务指南', callback_data='hub:rental')],
    ])


def deposit_text() -> str:
    return (
        '🔐 <b>退租与押金说明</b>\n\n'
        '退租前建议先核对合同中的通知期、押金退还条件和扣费约定。\n\n'
        '退租交接时，结合入住留档逐项确认：\n'
        '• 房屋及家具家电状态\n'
        '• 水电、物业及其他费用\n'
        '• 钥匙 / 门卡交还\n'
        '• 押金扣费项目及依据\n\n'
        '最终押金退还金额以合同约定和实际交接核对结果为准。\n\n'
        '需要保存完整说明时，再点击下方下载。'
    )


def deposit_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📥 下载退租与押金说明', callback_data='hub:rental:deposit:pdf')],
        [InlineKeyboardButton('📋 查看入住交接清单', callback_data='hub:rental:handover')],
        [InlineKeyboardButton('⬅️ 返回租赁服务指南', callback_data='hub:rental')],
    ])


def fees_text() -> str:
    return (
        '💡 <b>费用说明</b>\n\n'
        '签约前确认租金、押金、水电、物业、网络、停车及其他可能产生的费用。\n'
        '没有确认的数据不要自行推测，以合同和双方确认内容为准。'
    )


def fees_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ 返回租赁服务指南', callback_data='hub:rental')]])


def viewing_text() -> str:
    return '🎥 <b>实地 / 视频看房</b>\n\n看中具体房源后，从房源页点「预约看房」，在日期页切换实地或实时视频看房。'


def viewing_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔍 开始找房', callback_data='home_smart_search')],
        [InlineKeyboardButton('⬅️ 返回首页', callback_data='home')],
    ])


def moving_text() -> str:
    return '🚚 <b>搬家协助</b>\n\n需要协调搬家时间、车辆或入住安排，可以联系中文顾问。'


def moving_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('💬 中文顾问', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回入住服务', callback_data='service:hub')],
    ])


def about_text() -> str:
    return (
        '🏠 <b>关于侨联</b>\n\n'
        '侨联地产专注于柬埔寨本地房产与居住服务。\n\n'
        '从找房、签约到入住后的租住服务，尽量让每一步都有记录、有人跟进。\n\n'
        '<b>侨联地产｜您在金边的自己人</b>'
    )


def about_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('💬 中文顾问', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回首页', callback_data='home')],
    ])


_ASSET_DIR = Path(__file__).resolve().parents[1] / 'assets' / 'v2_2' / 'generated'


async def _send_asset_bundle(update, context, kind: str) -> int:
    if kind not in {'handover', 'deposit'}:
        raise ValueError('unsupported rental asset')
    chat_id = update.effective_chat.id
    title = '入住交接清单' if kind == 'handover' else '退租与押金说明'
    pdf = _ASSET_DIR / f'{kind}.pdf'
    if not pdf.exists():
        from .texts import render_panel
        await render_panel(
            update,
            text=f'📄 <b>{title}</b>\n\n资料暂时无法下载，请联系中文顾问获取。',
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('💬 中文顾问', callback_data='hub:advisor')],
                [InlineKeyboardButton('⬅️ 返回租赁服务指南', callback_data='hub:rental')],
            ]),
            context=context,
        )
        return MAIN
    await context.bot.send_document(chat_id=chat_id, document=pdf.open('rb'), filename=pdf.name)
    sent = await context.bot.send_message(
        chat_id=chat_id,
        text=f'✅ <b>{title}已发送</b>\n\n建议保存到手机，需要使用时再打开核对。',
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ 返回租赁服务指南', callback_data='hub:rental')]]),
    )
    context.user_data['_panel_anchor'] = {'chat_id': int(sent.chat_id), 'message_id': int(sent.message_id)}
    return MAIN


async def handle_rental_callback(update, context, query, data: str, user) -> int | None:
    from .flows import show_service_hub
    from .texts import render_panel

    if data == 'service:handover':
        data = 'hub:rental:handover'
    elif data == 'service:deposit':
        data = 'hub:rental:deposit'

    binding = db.get_active_binding(user.id) if user else None
    if data.startswith('hub:rental') and data not in {'hub:rental:viewing', 'hub:rental:moving', 'hub:rental:about'} and not binding:
        return await show_service_hub(update, context)

    if data == 'hub:rental':
        await render_panel(update, text=rental_home_text(), parse_mode=ParseMode.HTML, reply_markup=rental_home_keyboard(), context=context)
        return MAIN
    if data == 'hub:rental:fees':
        await render_panel(update, text=fees_text(), parse_mode=ParseMode.HTML, reply_markup=fees_keyboard(), context=context)
        return MAIN
    if data in {'hub:rental:handover', 'hub:rental:handover:preview', 'hub:rental:handover:details'}:
        await render_panel(update, text=handover_text(), parse_mode=ParseMode.HTML, reply_markup=handover_keyboard(), context=context, prefer_edit_anchor=True)
        return MAIN
    if data == 'hub:rental:handover:pdf':
        return await _send_asset_bundle(update, context, 'handover')
    if data == 'hub:rental:deposit':
        await render_panel(update, text=deposit_text(), parse_mode=ParseMode.HTML, reply_markup=deposit_keyboard(), context=context)
        return MAIN
    if data == 'hub:rental:deposit:pdf':
        return await _send_asset_bundle(update, context, 'deposit')
    if data == 'hub:rental:viewing':
        await render_panel(update, text=viewing_text(), parse_mode=ParseMode.HTML, reply_markup=viewing_keyboard(), context=context)
        return MAIN
    if data == 'hub:rental:moving':
        await render_panel(update, text=moving_text(), parse_mode=ParseMode.HTML, reply_markup=moving_keyboard(), context=context)
        return MAIN
    if data == 'hub:rental:about':
        await render_panel(update, text=about_text(), parse_mode=ParseMode.HTML, reply_markup=about_keyboard(), context=context)
        return MAIN
    return None

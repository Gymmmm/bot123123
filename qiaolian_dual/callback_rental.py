"""Customer-facing Qiaolian assurance flow."""
from __future__ import annotations

from pathlib import Path

from PIL import ImageFont
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
        '🛡 <b>侨联保障</b>\n\n'
        '签约前核对费用；入住时把房屋、表计和物品状态留档；退租时按记录逐项核对。\n\n'
        '发生问题时，侨联协助沟通。'
    )

def rental_home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📋 入住交接', callback_data='hub:rental:handover'), InlineKeyboardButton('🔐 押金与退租', callback_data='hub:rental:deposit')],
        [InlineKeyboardButton('🚚 搬家协助', callback_data='hub:rental:moving'), InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')],
    ])


def handover_text() -> str:
    return (
        '🔐 <b>押金与入住留档</b>\n\n'
        '很多退租时的分歧，都来自入住时没有留下足够清楚的记录。\n\n'
        '入住前，建议将房屋现状、家具家电、表计读数及相关费用规则进行确认并留档。\n\n'
        '这些资料不会替代合同，但可以作为后续交接、费用核对和争议沟通时的重要参考。'
    )

def handover_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔒 押金保障', callback_data='hub:rental:deposit'), InlineKeyboardButton('📄 入住交接留档', callback_data='hub:rental:handover:details')],
        [InlineKeyboardButton('💡 费用说明', callback_data='hub:rental:fees'), InlineKeyboardButton('📥 下载完整资料', callback_data='hub:rental:handover:pdf')],
        [InlineKeyboardButton('⬅️ 返回', callback_data='hub:rental')],
    ])

def preview_followup_text() -> str:
    return (
        '📸 <b>这是入住交接留档单示例。</b>\n\n'
        '建议入住当天现场填写，并将双方确认后的内容拍照保存。'
    )


def preview_followup_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📄 查看押金说明', callback_data='hub:rental:deposit'), InlineKeyboardButton('📥 下载完整版 PDF', callback_data='hub:rental:handover:pdf')],
        [InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回', callback_data='hub:rental:handover')],
    ])


def details_text() -> str:
    return (
        '📄 <b>入住交接留档</b>\n\n'
        '通过侨联成交的房源，入住交接时建议记录：\n\n'
        '• 房屋整体状态\n'
        '• 家具家电使用情况\n'
        '• 空调、冰箱、洗衣机等设备照片\n'
        '• 电表、水表读数\n'
        '• 墙面、地面、门锁等易产生争议的位置\n'
        '• 押金、电费、水费、物业费等费用规则\n\n'
        '这些记录在退租时，是核对情况的重要依据。'
    )

def details_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📸 查看留档示例', callback_data='hub:rental:handover:preview')],
        [InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回', callback_data='hub:rental:handover')],
    ])

def deposit_text() -> str:
    return (
        '🔒 <b>押金保障</b>\n\n'
        '入住前，侨联协助完成房屋现状留档，包含设施状态、已有瑕疵、水电表读数等信息。\n\n'
        '退租时，侨联可作为第三方协助核对房屋状态、费用明细及扣费依据。如出现明显争议，优先协助沟通协调。\n\n'
        '押金保障不是退租那天才开始，而是从入住第一天就把细节留清楚。'
    )

def deposit_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📄 入住交接留档', callback_data='hub:rental:handover:details')],
        [InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回', callback_data='hub:rental:handover')],
    ])

def fees_text() -> str:
    return (
        '💡 <b>费用说明</b>\n\n'
        '在金边租房，费用相关的问题往往集中在以下细节：\n\n'
        '• 电费、水费计算方式\n'
        '• 物业费由谁承担\n'
        '• 网络是否需要自行安装\n'
        '• 停车费是否另计\n'
        '• 垃圾费、清洁费\n'
        '• 是否允许做饭\n'
        '• 提前退租的处理方式\n\n'
        '侨联会尽量提前确认并说明这些内容。'
    )

def fees_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔍 去找房', callback_data='home_smart_search')],
        [InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回', callback_data='hub:rental:handover')],
    ])

def viewing_text() -> str:
    return (
        '🎥 <b>实地 / 视频看房</b>\n\n'
        '可以到现场实地看房；不方便到现场时，也可以安排视频看房。\n\n'
        '看中具体房源后，从房源页点「预约看房」，在日期页切换实地或视频。'
    )


def viewing_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔍 帮我找房', callback_data='home_smart_search')],
        [InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回', callback_data='hub:rental')],
    ])


def moving_text() -> str:
    return (
        '🚚 <b>搬家协助</b>\n\n'
        '通过侨联成交的客户，可获得基础搬家协助：\n\n'
        '• 协调搬家时间\n'
        '• 对接车辆和人手\n'
        '• 提供必要的现场支持\n\n'
        '搬家不是租约的终点，而是生活真正开始的节点。我们能做的，是让这一小段过渡，稍微从容一些。'
    )

def moving_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回', callback_data='hub:rental')],
    ])


def about_text() -> str:
    return (
        '🏠 <b>关于侨联</b>\n\n'
        '这些年，我们始终专注于柬埔寨本地房产与居住服务。\n\n'
        '在长期服务客户的过程中，我们越来越清楚，一次租房真正需要解决的，不只是找到房源，更包括信息确认、签约衔接、入住交接，以及住下以后仍然有人可以联系。\n\n'
        '我们相信，真正有价值的服务，不止于完成一次交易，更在于建立长久而持续的信任。\n\n'
        '从第一次认识，到长期选择，侨联希望以稳定的服务与责任，成为您在柬埔寨值得长期信赖的生活伙伴。\n\n'
        '<b>侨联地产｜您在金边的自己人</b>'
    )

def about_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回', callback_data='hub:rental')],
    ])


_ASSET_DIR = Path(__file__).resolve().parents[1] / 'assets' / 'v2_2' / 'generated'

def _font(size: int, *, bold: bool = False):
    """CJK font helper for the publisher's private sample card."""
    paths = [Path('/System/Library/Fonts/PingFang.ttc'), Path('/System/Library/Fonts/STHeiti Medium.ttc'), Path('/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc' if bold else '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc')]
    for path in paths:
        if path.exists(): return ImageFont.truetype(str(path), size)
    return ImageFont.load_default()


async def _send_asset_bundle(update, context, query, kind: str) -> int:
    if kind not in {'handover', 'deposit'}:
        raise ValueError('unsupported assurance asset')
    chat_id = update.effective_chat.id
    try:
        await query.message.delete()
    except Exception:
        pass
    title = '入住交接' if kind == 'handover' else '押金说明'
    await context.bot.send_photo(chat_id=chat_id, photo=(_ASSET_DIR / f'{kind}.png').open('rb'))
    await context.bot.send_document(chat_id=chat_id, document=(_ASSET_DIR / f'{kind}.pdf').open('rb'))
    instruction = (
        '请在入住当天逐项核对并填写，双方确认后各自保存，退租时再按留档记录核对。'
        if kind == 'handover' else
        '请在签约前核对押金金额、退还条件和扣费依据；退租时结合合同与入住留档逐项确认。最终押金退还金额仍以合同和实际核对结果为准。'
    )
    sent = await context.bot.send_message(chat_id=chat_id, text=f'✅ <b>{title}资料已发送</b>\n\n{instruction}', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')], [InlineKeyboardButton('⬅️ 返回侨联保障', callback_data='hub:rental')]]))
    context.user_data['_panel_anchor'] = {'chat_id': int(sent.chat_id), 'message_id': int(sent.message_id)}
    return MAIN


async def handle_rental_callback(update, context, query, data: str, user) -> int | None:
    from .texts import render_panel

    if data == 'service:handover':
        data = 'hub:rental:handover'
    elif data == 'service:deposit':
        data = 'hub:rental:deposit'

    if data == 'hub:rental':
        await render_panel(update, text=rental_home_text(), parse_mode=ParseMode.HTML, reply_markup=rental_home_keyboard(), context=context)
        return MAIN
    if data == 'hub:rental:fees':
        await render_panel(update, text=fees_text(), parse_mode=ParseMode.HTML, reply_markup=fees_keyboard(), context=context)
        return MAIN
    if data == 'hub:rental:handover':
        return await _send_asset_bundle(update, context, query, 'handover')
    if data == 'hub:rental:handover:preview':
        return await _send_asset_bundle(update, context, query, 'handover')
    if data == 'hub:rental:handover:details':
        await render_panel(update, text=details_text(), parse_mode=ParseMode.HTML, reply_markup=details_keyboard(), context=context, prefer_edit_anchor=True)
        return MAIN
    if data == 'hub:rental:handover:pdf':
        return await _send_asset_bundle(update, context, query, 'handover')
    if data == 'hub:rental:deposit':
        return await _send_asset_bundle(update, context, query, 'deposit')
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

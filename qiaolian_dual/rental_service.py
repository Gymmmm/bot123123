"""租房服务页：费用 / 留档 / 押金 / 看房方式。"""
from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .keyboards_common import rental_service_keyboard


def rental_hub_text() -> str:
    return (
        '🛡 <b>租房服务</b>\n\n'
        '看房、费用、入住怎么记、押金怎么退，\n'
        '都可以先在这里看明白。'
    )


def rental_fees_text() -> str:
    return (
        '💰 <b>费用说明</b>\n\n'
        '月租之外，签约前先问清：\n\n'
        '押金和怎么付\n'
        '物业谁出\n'
        '水电怎么计\n'
        '网络装好了没有\n'
        '停车含不含\n'
        '还有没有别的固定费\n\n'
        '具体数字写进合同和入住留档。\n'
        '问到哪项写哪项，不替房东填。'
    )


def rental_handover_text() -> str:
    return (
        '📋 <b>入住留档</b>\n\n'
        '入住当天，顾问按同一张单子现场记：\n\n'
        '电表、水表读数\n'
        '电费、水费怎么算\n'
        '物业、停车、网络谁出\n'
        '墙面、地板、门窗成色\n'
        '空调、冰箱、洗衣机、热水器\n'
        '钥匙和门卡数量\n'
        '现场照片、视频\n\n'
        '退租时拿当天这份对。\n'
        '入住前就有的问题，不记成新损坏。\n\n'
        '单一式两份，双方各留一份。'
    )


def rental_deposit_text() -> str:
    return (
        '🔐 <b>押金说明</b>\n\n'
        '押金退得干净，关键看入住当天有没有记下房屋现状。\n\n'
        '我们做三件事：\n\n'
        '签约前\n'
        '退租扣什么、提前走怎么算，写进合同。\n'
        '口头说的不算。\n\n'
        '入住当天\n'
        '按留档单拍照、记仪表、点家电和钥匙。\n'
        '这份是退租时的对照。\n\n'
        '退租当天\n'
        '按留档逐项看。\n'
        '入住前就有的，不找租客。\n'
        '住进去以后弄坏的，再单独算。\n\n'
        '两边对不上，顾问拿留档一起对。'
    )


def rental_viewing_text() -> str:
    return (
        '🎥 <b>实地 / 视频看房</b>\n\n'
        '实地\n'
        '约好日期时段，顾问带看。\n\n'
        '视频\n'
        '人还没到金边，或当天跑不开，\n'
        '日期页改成「视频看房」即可。\n'
        '顾问确认时间后，会把通话入口发到 Telegram。\n\n'
        '两套都从房源页的「预约看房」进去。'
    )


def _page_keyboard(page: str) -> InlineKeyboardMarkup:
    back = InlineKeyboardButton('⬅️ 返回租房服务', callback_data='hub:rental')
    advisor = InlineKeyboardButton('💬 联系中文顾问', callback_data='hub:advisor')
    if page == 'fees':
        extra = InlineKeyboardButton('📋 看入住留档', callback_data='hub:rental:handover')
        return InlineKeyboardMarkup([[extra], [advisor], [back]])
    if page == 'handover':
        extra = InlineKeyboardButton('🔐 押金怎么退', callback_data='hub:rental:deposit')
        return InlineKeyboardMarkup([[extra], [advisor], [back]])
    if page == 'deposit':
        extra = InlineKeyboardButton('📋 看入住留档', callback_data='hub:rental:handover')
        return InlineKeyboardMarkup([[extra], [advisor], [back]])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔍 帮我找房', callback_data='home_smart_search'),
         InlineKeyboardButton('📅 我的预约', callback_data='hub:appointments')],
        [advisor],
        [back],
    ])


async def handle_rental_callback(update, context, data: str):
    from .texts import render_panel
    from .common import MAIN
    page = data.split(':', 2)[2] if data.startswith('hub:rental:') else ''
    mapping = {
        'fees': (rental_fees_text(), _page_keyboard('fees')),
        'handover': (rental_handover_text(), _page_keyboard('handover')),
        'deposit': (rental_deposit_text(), _page_keyboard('deposit')),
        'viewing': (rental_viewing_text(), _page_keyboard('viewing')),
    }
    text, markup = mapping.get(page, (rental_hub_text(), rental_service_keyboard()))
    await render_panel(update, text=text, parse_mode=ParseMode.HTML, reply_markup=markup, context=context)
    return MAIN

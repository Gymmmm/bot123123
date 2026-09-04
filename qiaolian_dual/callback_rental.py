"""Customer-facing rental service flow: fees, handover records, deposit explanation."""
from __future__ import annotations

from io import BytesIO
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile
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
    # Legacy aliases kept for old buttons/deep links.
    'service:handover',
    'service:deposit',
}


def matches(data: str) -> bool:
    return data in _CALLBACKS


def rental_home_text() -> str:
    return (
        '🛡️ <b>租房服务</b>\n\n'
        '看房、费用、入住留档和退租押金，\n'
        '都可以先在这里了解清楚。'
    )


def rental_home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('💰 费用说明', callback_data='hub:rental:fees'), InlineKeyboardButton('📋 入住留档', callback_data='hub:rental:handover')],
        [InlineKeyboardButton('🔐 押金说明', callback_data='hub:rental:deposit'), InlineKeyboardButton('🎥 实地 / 视频看房', callback_data='hub:rental:viewing')],
        [InlineKeyboardButton('💬 联系中文顾问', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回首页', callback_data='home')],
    ])


def handover_text() -> str:
    return (
        '📋 <b>入住留档</b>\n\n'
        '入住当天，顾问会把房屋现状、费用、\n'
        '水电表、家电和钥匙门卡逐项记录。\n\n'
        '现场照片和视频会和房源档案一起保存，\n'
        '退租时按入住当天的记录核对。\n\n'
        '入住前已经存在的问题，\n'
        '不会到退租时才说不清。'
    )


def handover_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('👀 看留档样例', callback_data='hub:rental:handover:preview'), InlineKeyboardButton('🔐 押金说明', callback_data='hub:rental:deposit')],
        [InlineKeyboardButton('💬 联系中文顾问', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回租房服务', callback_data='hub:rental')],
    ])


def preview_followup_text() -> str:
    return (
        '📄 <b>侨联地产｜入住交接留档单</b>\n\n'
        '正式入住时由顾问现场填写，\n'
        '房屋状态、仪表读数和影像资料统一关联档案编号。\n\n'
        '一式两份，双方各留一份。'
    )


def preview_followup_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔍 查看留档内容', callback_data='hub:rental:handover:details')],
        [InlineKeyboardButton('🔐 押金说明', callback_data='hub:rental:deposit')],
        [InlineKeyboardButton('💬 联系中文顾问', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回入住留档', callback_data='hub:rental:handover')],
    ])


def details_text() -> str:
    return (
        '📋 <b>留档内容</b>\n\n'
        '<b>01｜房源信息</b>\n楼盘、房号、户型、楼层、面积\n\n'
        '<b>02｜租约与费用</b>\n月租、押金、付款方式、租期\n水费、电费、物业、停车、网络\n\n'
        '<b>03｜仪表读数</b>\n入住时记录水表、电表\n退租时再次记录并核对\n\n'
        '<b>04｜房屋状态</b>\n客厅、卧室、厨房、卫生间\n墙面、地板、门窗、家具等\n\n'
        '<b>05｜家电状态</b>\n空调、冰箱、洗衣机、热水器等\n\n'
        '<b>06｜钥匙与门卡</b>\n房门钥匙、门禁卡、停车卡等\n\n'
        '<b>07｜补充与影像留档</b>\n已有损坏、待修事项、特殊约定\n以及现场照片和视频\n\n'
        '正式交接时由顾问现场填写，\n不是让客户自己回去填表。'
    )


def details_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📄 查看完整样表 PDF', callback_data='hub:rental:handover:pdf')],
        [InlineKeyboardButton('🔐 押金说明', callback_data='hub:rental:deposit')],
        [InlineKeyboardButton('💬 联系中文顾问', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回入住留档', callback_data='hub:rental:handover')],
    ])


def deposit_text() -> str:
    return (
        '🔐 <b>押金说明</b>\n\n'
        '侨联不替房东决定押金退多少。\n\n'
        '我们做的是把退租时最容易说不清的地方，\n提前留下依据。\n\n'
        '<b>签约前</b>\n确认押金、提前退租和扣费规则，\n尽量写进合同。\n\n'
        '<b>入住当天</b>\n记录房屋现状、仪表、家电、钥匙门卡，\n并保留现场照片和视频。\n\n'
        '<b>退租当天</b>\n按合同和入住留档逐项核对。\n\n'
        '入住前已经存在的问题，\n不应该在退租时当成新增损坏。\n\n'
        '如果双方对某一项有分歧，\n顾问会拿合同和留档一起协助核对。\n\n'
        '最终押金退还金额，\n仍以合同和实际核对结果为准。'
    )


def deposit_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📄 看入住留档', callback_data='hub:rental:handover')],
        [InlineKeyboardButton('💬 联系中文顾问', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回租房服务', callback_data='hub:rental')],
    ])


def fees_text() -> str:
    return (
        '💰 <b>费用说明</b>\n\n'
        '除了月租，签约前建议把以下费用确认清楚：\n\n'
        '押金和付款方式\n'
        '物业费由谁承担\n'
        '水费、电费如何计算\n'
        '网络是否已经安装\n'
        '停车是否包含\n'
        '是否还有其他固定费用\n\n'
        '已确认的费用，会按实际情况记录在合同或入住留档中。\n'
        '没有确认的数据，不代替房东填写。'
    )


def fees_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📋 看入住留档', callback_data='hub:rental:handover')],
        [InlineKeyboardButton('💬 联系中文顾问', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回租房服务', callback_data='hub:rental')],
    ])


def viewing_text() -> str:
    return (
        '🎥 <b>实地 / 视频看房</b>\n\n'
        '可以到现场实地看房，\n'
        '不方便到现场时，也可以安排实时视频看房。\n\n'
        '看中具体房源后，直接从房源页点「📅 预约看房」选择日期和时间。'
    )


def viewing_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔍 帮我找房', callback_data='home_smart_search')],
        [InlineKeyboardButton('💬 联系中文顾问', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回租房服务', callback_data='hub:rental')],
    ])


def _font_path(bold: bool = False) -> str:
    names = ['NotoSansCJK-Bold.ttc'] if bold else ['NotoSansCJK-Regular.ttc']
    roots = [
        Path('/usr/share/fonts/opentype/noto'),
        Path('/usr/share/fonts/truetype/noto'),
        Path('/usr/share/fonts/truetype/wqy'),
    ]
    for root in roots:
        for name in names:
            p = root / name
            if p.exists():
                return str(p)
    # Existing production image pipeline already requires Noto; fail loudly if missing.
    raise FileNotFoundError('Noto Sans CJK font not found')


def _font(size: int, *, bold: bool = False):
    return ImageFont.truetype(_font_path(bold=bold), size)


def _build_preview_jpg() -> BytesIO:
    w, h = 1050, 1425
    img = Image.new('RGB', (w, h), (22, 20, 18))
    d = ImageDraw.Draw(img)
    gold, cream, muted, line, card = (207, 172, 91), (240, 233, 216), (180, 174, 160), (82, 72, 54), (31, 29, 26)
    title, en, sub, head, body = _font(40, bold=True), _font(20), _font(21), _font(23, bold=True), _font(18)
    m = 68
    d.text((m, 58), '侨联地产｜入住交接留档单', font=title, fill=cream)
    d.text((m, 112), 'MOVE-IN HANDOVER & RECORD FORM', font=en, fill=gold)
    d.text((m, 153), '把入住当天的真实状态，留作退租与押金核对依据', font=sub, fill=muted)
    d.line((m, 205, w - m, 205), fill=gold, width=2)
    meta = [('档案编号', 'QL-HO-2026-0008'), ('交接日期', 'YYYY / MM / DD'), ('经办顾问', '侨联中文顾问')]
    x = m
    for label, value in meta:
        d.rounded_rectangle((x, 235, x + 285, 308), radius=12, fill=card, outline=line, width=2)
        d.text((x + 16, 246), label, font=body, fill=muted)
        d.text((x + 16, 274), value, font=head, fill=cream)
        x += 307
    sections = [
        ('01｜房源信息', '楼盘 / 房号 / 户型 / 楼层 / 面积'),
        ('02｜租约与费用摘要', '月租 / 押金 / 付款方式 / 水电物业网络停车'),
        ('03｜水表 / 电表读数', '入住读数现场记录，退租再次核对'),
        ('04｜房屋状态', '客厅 / 卧室 / 厨房 / 卫生间 / 墙地门窗家具'),
        ('05｜家电状态', '空调 / 冰箱 / 洗衣机 / 热水器等'),
        ('06｜钥匙 / 门卡', '房门钥匙 / 门禁卡 / 停车卡'),
        ('07｜现场照片与视频留档', '已有损坏 / 待修事项 / 特殊约定 / 现场影像'),
        ('三方确认', '租客 / 房东（或授权方）/ 经办顾问'),
    ]
    y = 350
    for title_text, body_text in sections:
        box_h = 112
        d.rounded_rectangle((m, y, w - m, y + box_h), radius=14, fill=card, outline=line, width=2)
        d.text((m + 18, y + 15), title_text, font=head, fill=gold)
        d.text((m + 22, y + 61), '• ' + body_text, font=body, fill=cream)
        y += box_h + 12
    d.text((m, h - 68), '客户预览版｜正式入住时由顾问现场填写并关联影像资料', font=body, fill=muted)
    out = BytesIO()
    img.save(out, format='JPEG', quality=90, optimize=True)
    out.seek(0)
    return out


def _build_sample_pdf() -> BytesIO:
    pages = []
    sections = [
        ('01｜房源信息', ['楼盘 / 项目：____________________________', '房号：______  户型：______  楼层：______  面积：______']),
        ('02｜租约与费用', ['月租：______  押金：______  付款方式：______  租期：______', '水：______  电：______  物业：______  网络：______  停车：______']),
        ('03｜仪表读数', ['水表读数：____________________', '电表读数：____________________']),
        ('04｜房屋状态', ['客厅 / 卧室 / 厨房 / 卫生间：____________________________', '墙面 / 地板 / 门窗 / 家具：______________________________']),
        ('05｜家电状态', ['空调：______  冰箱：______  洗衣机：______  热水器：______', '其他家电 / 异常：______________________________________']),
        ('06｜钥匙与门卡', ['房门钥匙：______  门禁卡：______  停车卡：______', '其他：________________________________________________']),
        ('07｜补充与影像留档', ['已有损坏 / 待修事项 / 特殊约定：', '____________________________________________________', '现场照片与视频统一关联本档案编号保存。']),
        ('三方确认', ['租客签字：____________________', '房东 / 授权方：________________', '经办顾问：____________________', '日期：________________________']),
    ]
    for page_index in range(2):
        w, h = 1000, 1414
        img = Image.new('RGB', (w, h), 'white')
        d = ImageDraw.Draw(img)
        title, head, body, small = _font(34, bold=True), _font(23, bold=True), _font(18), _font(15)
        x, y = 65, 55
        heading = '侨联地产｜入住交接留档单' if page_index == 0 else '侨联地产｜入住交接留档单（续）'
        d.text((x, y), heading, font=title, fill='black')
        y += 54
        if page_index == 0:
            d.text((x, y), 'MOVE-IN HANDOVER & RECORD FORM', font=small, fill='black'); y += 30
            d.text((x, y), '把入住当天的真实状态，留作退租与押金核对依据', font=body, fill='black'); y += 42
            d.line((x, y, w - x, y), fill='black', width=2); y += 28
            for line_text in ('档案编号：____________________', '交接日期：____________________', '经办顾问：____________________'):
                d.text((x, y), line_text, font=body, fill='black'); y += 34
            y += 10
        for section_title, lines in sections[page_index * 4:(page_index + 1) * 4]:
            d.text((x, y), section_title, font=head, fill='black'); y += 38
            for line_text in lines:
                d.text((x + 16, y), line_text, font=body, fill='black'); y += 33
            y += 20
        if page_index == 1:
            d.text((x, h - 78), '说明：最终押金退还金额仍以合同和实际核对结果为准。', font=small, fill='black')
        pages.append(img)
    out = BytesIO()
    pages[0].save(out, format='PDF', save_all=True, append_images=pages[1:], resolution=130)
    out.seek(0)
    return out


async def _send_preview(update, context, query) -> int:
    chat_id = update.effective_chat.id
    # Delete the menu message so the media + follow-up appear in the intended order.
    try:
        await query.message.delete()
    except Exception:
        pass
    preview = _build_preview_jpg()
    await context.bot.send_photo(chat_id=chat_id, photo=InputFile(preview, filename='qiaolian_move_in_handover_preview.jpg'))
    sent = await context.bot.send_message(
        chat_id=chat_id,
        text=preview_followup_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=preview_followup_keyboard(),
    )
    context.user_data['_panel_anchor'] = {'chat_id': int(sent.chat_id), 'message_id': int(sent.message_id)}
    return MAIN


async def _send_pdf(update, context) -> int:
    pdf = _build_sample_pdf()
    await context.bot.send_document(
        chat_id=update.effective_chat.id,
        document=InputFile(pdf, filename='qiaolian_move_in_handover_sample.pdf'),
        caption='📄 侨联地产｜入住交接留档单（完整样表）',
    )
    return MAIN


async def handle_rental_callback(update, context, query, data: str, user) -> int | None:
    from .texts import render_panel

    # Map historical callbacks to the new customer-facing wording and hierarchy.
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
        await render_panel(update, text=handover_text(), parse_mode=ParseMode.HTML, reply_markup=handover_keyboard(), context=context)
        return MAIN
    if data == 'hub:rental:handover:preview':
        return await _send_preview(update, context, query)
    if data == 'hub:rental:handover:details':
        await render_panel(update, text=details_text(), parse_mode=ParseMode.HTML, reply_markup=details_keyboard(), context=context, prefer_edit_anchor=True)
        return MAIN
    if data == 'hub:rental:handover:pdf':
        return await _send_pdf(update, context)
    if data == 'hub:rental:deposit':
        await render_panel(update, text=deposit_text(), parse_mode=ParseMode.HTML, reply_markup=deposit_keyboard(), context=context, prefer_edit_anchor=True)
        return MAIN
    if data == 'hub:rental:viewing':
        await render_panel(update, text=viewing_text(), parse_mode=ParseMode.HTML, reply_markup=viewing_keyboard(), context=context)
        return MAIN
    return None

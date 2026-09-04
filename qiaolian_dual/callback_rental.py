"""Customer-facing Qiaolian assurance flow."""
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
    'hub:rental:moving',
    'hub:rental:about',
    'service:handover',
    'service:deposit',
}


def matches(data: str) -> bool:
    return data in _CALLBACKS


def rental_home_text() -> str:
    return (
        '🛡 <b>通过侨联租房，多一层安心。</b>\n\n'
        '🎥 <b>看房更方便</b>\n'
        '支持实地看房，也可以先视频看房。\n\n'
        '💰 <b>费用先说清</b>\n'
        '水电、物业、网络等重要费用，签约前尽量确认。\n\n'
        '🔐 <b>入住有留档</b>\n'
        '房屋现状、家具家电等做好记录，退租时更有依据。\n\n'
        '🤝 <b>签约后也有人跟进</b>\n'
        '从找房、签约到入住，不因为拿到钥匙就结束。'
    )


def rental_home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔐 押金与入住留档', callback_data='hub:rental:handover'), InlineKeyboardButton('🚚 搬家协助', callback_data='hub:rental:moving')],
        [InlineKeyboardButton('🏠 关于侨联', callback_data='hub:rental:about'), InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回首页', callback_data='home')],
    ])


def handover_text() -> str:
    return (
        '🔐 <b>押金与入住留档</b>\n\n'
        '入住前，建议把重要情况说清楚、写下来、留好证据。\n\n'
        '侨联可协助记录：\n\n'
        '• 家具、家电现状\n'
        '• 墙面、地面情况\n'
        '• 水表、电表读数\n'
        '• 水电、物业、网络等费用规则\n'
        '• 其他需要双方确认的事项\n\n'
        '退租时，如果对扣费存在疑问，也可以根据留档协助核对和沟通。'
    )


def handover_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📸 查看留档单示例', callback_data='hub:rental:handover:preview')],
        [InlineKeyboardButton('📄 查看押金说明', callback_data='hub:rental:deposit')],
        [InlineKeyboardButton('📥 下载完整版 PDF', callback_data='hub:rental:handover:pdf')],
        [InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')],
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
    return handover_text()


def details_keyboard() -> InlineKeyboardMarkup:
    return handover_keyboard()


def deposit_text() -> str:
    return (
        '📄 <b>押金说明</b>\n\n'
        '<b>签约前</b>\n'
        '确认押金、提前退租和扣费规则，尽量写进合同。\n\n'
        '<b>入住当天</b>\n'
        '记录房屋现状、仪表、家电和钥匙门卡，并保留现场照片和视频。\n\n'
        '<b>退租当天</b>\n'
        '按合同和入住留档逐项核对。\n\n'
        '如果双方对某一项有分歧，侨联可以根据合同和留档协助核对和沟通。\n\n'
        '最终押金退还金额，仍以合同和实际核对结果为准。'
    )


def deposit_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📸 查看留档单示例', callback_data='hub:rental:handover:preview')],
        [InlineKeyboardButton('📥 下载完整版 PDF', callback_data='hub:rental:handover:pdf')],
        [InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回', callback_data='hub:rental:handover')],
    ])


def fees_text() -> str:
    return (
        '💰 <b>费用说明</b>\n\n'
        '除了月租，签约前建议确认：押金和付款方式、物业、水电、网络、停车，以及其他固定费用。\n\n'
        '已确认的费用按实际情况记录；没有确认的数据，不代替房东填写。'
    )


def fees_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔐 押金与入住留档', callback_data='hub:rental:handover')],
        [InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回', callback_data='hub:rental')],
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
        '🚚 <b>搬家，也可以让侨联帮您协调。</b>\n\n'
        '通过侨联成交的客户，可咨询基础搬家协助。\n\n'
        '我们可以协助：\n'
        '• 确认搬家时间\n'
        '• 对接车辆和搬运人员\n'
        '• 协调入住相关事项\n\n'
        '从签约到真正住进去，我们希望都有人接得上。'
    )


def moving_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor'), InlineKeyboardButton('🛠 入住服务', callback_data='hub:service')],
        [InlineKeyboardButton('⬅️ 返回', callback_data='hub:rental')],
    ])


def about_text() -> str:
    return (
        '🏠 <b>关于侨联</b>\n\n'
        '来到金边，很多事情都需要重新熟悉。\n\n'
        '从住哪里、怎么租，到签约、入住，再到日常生活中遇到的各种问题。\n\n'
        '侨联想做的，不只是给您发几套房源。\n\n'
        '找房时，帮您多看一步；\n'
        '签约时，帮您多问一句；\n'
        '住下以后，有事还能找到人。\n\n'
        '认识侨联 · 选择侨联 · 信赖侨联\n\n'
        '我们希望成为您在金边，愿意长期联系的那个自己人。'
    )


def about_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回', callback_data='hub:rental')],
    ])


def _font_path(bold: bool = False) -> str:
    names = ['NotoSansCJK-Bold.ttc'] if bold else ['NotoSansCJK-Regular.ttc']
    roots = [Path('/usr/share/fonts/opentype/noto'), Path('/usr/share/fonts/truetype/noto'), Path('/usr/share/fonts/truetype/wqy')]
    for root in roots:
        for name in names:
            p = root / name
            if p.exists():
                return str(p)
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
    meta = [('档案编号', 'QL-HO-2026-0008'), ('交接日期', 'YYYY / MM / DD'), ('经办顾问', '侨联顾问')]
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
    try:
        await query.message.delete()
    except Exception:
        pass
    preview = _build_preview_jpg()
    await context.bot.send_photo(chat_id=chat_id, photo=InputFile(preview, filename='qiaolian_move_in_handover_preview.jpg'))
    sent = await context.bot.send_message(chat_id=chat_id, text=preview_followup_text(), parse_mode=ParseMode.HTML, reply_markup=preview_followup_keyboard())
    context.user_data['_panel_anchor'] = {'chat_id': int(sent.chat_id), 'message_id': int(sent.message_id)}
    return MAIN


async def _send_pdf(update, context) -> int:
    pdf = _build_sample_pdf()
    await context.bot.send_document(
        chat_id=update.effective_chat.id,
        document=InputFile(pdf, filename='qiaolian_move_in_handover_sample.pdf'),
        caption='📥 完整资料已发送\n\n包含入住交接留档单及押金说明，可保存或打印使用。',
    )
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
    if data == 'hub:rental:moving':
        await render_panel(update, text=moving_text(), parse_mode=ParseMode.HTML, reply_markup=moving_keyboard(), context=context)
        return MAIN
    if data == 'hub:rental:about':
        await render_panel(update, text=about_text(), parse_mode=ParseMode.HTML, reply_markup=about_keyboard(), context=context)
        return MAIN
    return None

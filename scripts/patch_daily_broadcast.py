from pathlib import Path
import re

p = Path('autopilot_publish_bot.py')
text = p.read_text(encoding='utf-8')

templates = '''DAILY_TEMPLATES: dict[int, tuple[str, str]] = {
    1: (
        "今日可看房源",
        f"<b>🏠 {BRAND_NAME}｜今日可看房源</b>\\n\\n"
        "正在找房，发三项就够：区域、月预算、户型 / 入住时间。\\n"
        "中文顾问只按当前可预约的实拍房源帮你筛选，不用先看一大堆无关房源。\\n\\n"
        f"{BRAND_NAME}｜您在金边的自己人"
    ),
    2: (
        "看房前准备",
        "<b>📅 看房前，先确认这三件事</b>\\n\\n"
        "1. 月预算，以及是否包含物业、水电、网络、停车\\n"
        "2. 最想住的区域和可接受通勤时间\\n"
        "3. 预计入住日期\\n\\n"
        "先把这三项定下来，看房会快很多。"
    ),
    3: (
        "租房费用",
        f"<b>💰 {BRAND_NAME}｜租房费用别只看月租</b>\\n\\n"
        "看房前建议一起确认：押付方式、水费、电费、物业费、网络费、停车费。\\n"
        "没有明确写清的费用，我们会标为待确认，不替房东补答案。\\n\\n"
        "看中具体房源后，可以直接让中文顾问逐项核对。"
    ),
    4: (
        "周末看房",
        "<b>📹 周末看房安排</b>\\n\\n"
        "实地看房、视频代看都可以约。\\n"
        "如果准备周末集中看房，建议提前发：区域、预算、户型、方便时间。\\n\\n"
        "顾问先确认房态，再排实际可看的房源。"
    ),
    5: (
        "金边今日信息",
        "<b>☀️ 金边今日信息</b>\\n\\n"
        "天气和 USD/KHR 参考汇率会在发送前实时更新。\\n"
        "数据仅作出行和换汇参考。"
    ),
    6: (
        "区域怎么选",
        "<b>📍 金边租房｜区域怎么选</b>\\n\\n"
        "先看每天最常去哪里，再看预算和户型，不必先追热门小区。\\n"
        "同样预算，在不同区域能换到的面积、楼龄和配套差很多。\\n\\n"
        "不知道从哪选，可以把通勤地点和预算发给中文顾问。"
    ),
    7: (
        "签约前确认",
        f"<b>📝 {BRAND_NAME}｜签约前再确认一次</b>\\n\\n"
        "签约前重点看：租期、押金退还条件、维修责任、提前退租约定、交房清单。\\n"
        "入住时建议把房屋现状、仪表读数、钥匙和门卡一起留档。\\n\\n"
        "不清楚的条款先问清，再签。"
    ),
}

WEEKLY_DAILY_PLAN: dict[int, int] = {
    0: 5,
    1: 1,
    2: 6,
    3: 3,
    4: 4,
    5: 2,
    6: 7,
}

WEEKLY_DAILY_LABELS: dict[int, str] = {
    0: "周一｜金边今日信息",
    1: "周二｜今日可看房源",
    2: "周三｜区域怎么选",
    3: "周四｜租房费用",
    4: "周五｜周末看房",
    5: "周六｜看房前准备",
    6: "周日｜签约前确认",
}
'''
text, n = re.subn(
    r'DAILY_TEMPLATES: dict\[int, tuple\[str, str\]\] = \{.*?\n\}\n\n\n_WEATHER_LABELS = \{',
    templates + '\n\n_WEATHER_LABELS = {',
    text,
    count=1,
    flags=re.S,
)
if n != 1:
    raise SystemExit('DAILY_TEMPLATES block not found')

helpers = '''def _daily_template_number() -> int:
    raw = _get_setting(KEY_DAILY_TEMPLATE, "weekly").strip().lower()
    if raw == "weekly":
        return WEEKLY_DAILY_PLAN.get(datetime.now(TZ).weekday(), 5)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = 1
    return value if value in DAILY_TEMPLATES else 1


def _daily_is_weekly() -> bool:
    return _get_setting(KEY_DAILY_TEMPLATE, "weekly").strip().lower() == "weekly"


def _daily_weekly_plan_text() -> str:
    lines = ["🗓 <b>每周自动广播安排</b>", ""]
    for weekday in range(7):
        lines.append(WEEKLY_DAILY_LABELS[weekday])
    lines.extend(["", "每天只发 1 条；时间统一在“每日广播”里设置。"])
    return "\\n".join(lines)


def _ensure_daily_defaults() -> None:
    """首次打开或升级后保存一份可用内容，但绝不自动开启广播。"""
    if not _get_setting(KEY_DAILY_TEMPLATE, "").strip():
        _set_setting(KEY_DAILY_TEMPLATE, "weekly")
    template_no = _daily_template_number()
    if not _get_setting(KEY_DAILY_TEXT, "").strip():
        _set_setting(KEY_DAILY_TEXT, DAILY_TEMPLATES[template_no][1])
    if not _get_setting(KEY_DAILY_TIME, "").strip():
        _set_setting(KEY_DAILY_TIME, "09:30")
    if not _get_setting(KEY_DAILY_DYNAMIC, "").strip():
        _set_setting(KEY_DAILY_DYNAMIC, "0")


def _daily_keyboard(on: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🗓 7天自动轮播", callback_data="daily:weekly")],
        [
            InlineKeyboardButton("👀 预览今天", callback_data="daily:preview"),
            InlineKeyboardButton("📋 查看7天安排", callback_data="daily:plan"),
        ],
        [
            InlineKeyboardButton("⏰ 09:30", callback_data="daily:time:0930"),
            InlineKeyboardButton("⏰ 12:30", callback_data="daily:time:1230"),
        ],
        [
            InlineKeyboardButton("⏰ 18:30", callback_data="daily:time:1830"),
            InlineKeyboardButton("✏️ 其他时间", callback_data="daily:time:custom"),
        ],
        [InlineKeyboardButton("✏️ 临时改成固定文案", callback_data="daily:custom")],
        [InlineKeyboardButton("⏸ 暂停每日广播" if on else "▶️ 开启每日广播", callback_data="daily:off" if on else "daily:on")],
        [InlineKeyboardButton("⬅️ 返回首页", callback_data="cmd:quick_help")],
    ])
'''
text, n = re.subn(
    r'def _daily_template_number\(\) -> int:.*?\n\n\nasync def cmd_daily\(',
    helpers + '\n\nasync def cmd_daily(',
    text,
    count=1,
    flags=re.S,
)
if n != 1:
    raise SystemExit('daily helper block not found')

old = '''    template_raw = _get_setting(KEY_DAILY_TEMPLATE, "1").strip()
    template_no = _daily_template_number()
    title = "自定义内容" if template_raw == "custom" else DAILY_TEMPLATES[template_no][0]
    await update.effective_message.reply_text(
        "📢 <b>每日广播</b>\\n\\n"
        f"状态：<b>{'已开启' if on else '未开启'}</b>\\n"
        f"发送时间：<b>{html.escape(tm)}</b>（{html.escape(TZ_NAME)}）\\n"
        f"当前内容：{html.escape(title)}\\n\\n"
        "先选模板并预览。确认没问题后，再点“开启每日广播”。",
'''
new = '''    template_raw = _get_setting(KEY_DAILY_TEMPLATE, "weekly").strip().lower()
    template_no = _daily_template_number()
    if template_raw == "weekly":
        title = f"7天自动轮播 · 今天：{DAILY_TEMPLATES[template_no][0]}"
    elif template_raw == "custom":
        title = "固定自定义内容"
    else:
        title = DAILY_TEMPLATES[template_no][0]
    await update.effective_message.reply_text(
        "📢 <b>每日广播</b>\\n\\n"
        f"状态：<b>{'已开启' if on else '未开启'}</b>\\n"
        f"发送时间：<b>{html.escape(tm)}</b>（{html.escape(TZ_NAME)}）\\n"
        f"内容：{html.escape(title)}\\n\\n"
        "默认按星期自动换内容，每天只发 1 条。先预览今天，再决定是否开启。",
'''
if old not in text:
    raise SystemExit('cmd_daily summary block not found')
text = text.replace(old, new, 1)

marker = '    if data.startswith("daily:tpl:"):\n'
weekly_branch = '''    if data == "daily:weekly":
        _set_setting(KEY_DAILY_TEMPLATE, "weekly")
        _set_setting(KEY_DAILY_DYNAMIC, "0")
        template_no = _daily_template_number()
        _set_setting(KEY_DAILY_TEXT, DAILY_TEMPLATES[template_no][1])
        await q.edit_message_text(
            "✅ <b>已设为 7 天自动轮播</b>\\n\\n"
            f"今天：{html.escape(DAILY_TEMPLATES[template_no][0])}\\n"
            "每天只发 1 条，周一到周日内容自动切换。",
            parse_mode=ParseMode.HTML,
            reply_markup=_daily_keyboard(on),
        )
        return
    if data == "daily:plan":
        await q.message.reply_text(
            _daily_weekly_plan_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=_daily_keyboard(on),
        )
        return
'''
if marker not in text:
    raise SystemExit('daily callback marker not found')
text = text.replace(marker, weekly_branch + marker, 1)

old_preview = '''    if data == "daily:preview":
        if _get_setting(KEY_DAILY_DYNAMIC, "0").strip() == "1":
            body = await asyncio.to_thread(_fetch_phnom_penh_daily_info)
            _set_setting(KEY_DAILY_TEXT, body)
        else:
            body = _get_setting(KEY_DAILY_TEXT, "").strip()
'''
new_preview = '''    if data == "daily:preview":
        template_no = _daily_template_number()
        if _daily_is_weekly():
            body = await asyncio.to_thread(_fetch_phnom_penh_daily_info) if template_no == 5 else DAILY_TEMPLATES[template_no][1]
        elif _get_setting(KEY_DAILY_DYNAMIC, "0").strip() == "1":
            body = await asyncio.to_thread(_fetch_phnom_penh_daily_info)
            _set_setting(KEY_DAILY_TEXT, body)
        else:
            body = _get_setting(KEY_DAILY_TEXT, "").strip()
'''
if old_preview not in text:
    raise SystemExit('daily preview block not found')
text = text.replace(old_preview, new_preview, 1)

old_send = '''    if _get_setting(KEY_DAILY_DYNAMIC, "0").strip() == "1":
        body = await asyncio.to_thread(_fetch_phnom_penh_daily_info)
        _set_setting(KEY_DAILY_TEXT, body)
    else:
        body = _get_setting(KEY_DAILY_TEXT, "").strip()
'''
new_send = '''    template_no = _daily_template_number()
    if _daily_is_weekly():
        body = await asyncio.to_thread(_fetch_phnom_penh_daily_info) if template_no == 5 else DAILY_TEMPLATES[template_no][1]
    elif _get_setting(KEY_DAILY_DYNAMIC, "0").strip() == "1":
        body = await asyncio.to_thread(_fetch_phnom_penh_daily_info)
        _set_setting(KEY_DAILY_TEXT, body)
    else:
        body = _get_setting(KEY_DAILY_TEXT, "").strip()
'''
if old_send not in text:
    raise SystemExit('scheduled daily block not found')
text = text.replace(old_send, new_send, 1)
p.write_text(text, encoding='utf-8')

k = Path('v2/qiaolian_publisher_v2/keyboards.py')
kt = k.read_text(encoding='utf-8')
old_menu = '''            [
                InlineKeyboardButton("📚 发布记录", callback_data="cmd:logs"),
                InlineKeyboardButton("⚙️ 运营设置", callback_data="cmd:settings_hub"),
            ],
            [
                InlineKeyboardButton("❓ 使用帮助", callback_data="cmd:quick_help"),
            ],
'''
new_menu = '''            [
                InlineKeyboardButton("📢 每日广播", callback_data="cmd:daily"),
                InlineKeyboardButton("📚 发布记录", callback_data="cmd:logs"),
            ],
            [
                InlineKeyboardButton("⚙️ 运营设置", callback_data="cmd:settings_hub"),
                InlineKeyboardButton("❓ 使用帮助", callback_data="cmd:quick_help"),
            ],
'''
if old_menu not in kt:
    raise SystemExit('admin menu block not found')
k.write_text(kt.replace(old_menu, new_menu, 1), encoding='utf-8')

bot = Path('v2/qiaolian_publisher_v2/bot.py')
bt = bot.read_text(encoding='utf-8')
if '"daily": ap.cmd_daily,' not in bt:
    bt = bt.replace('            "logs": ap.cmd_logs,\n', '            "logs": ap.cmd_logs,\n            "daily": ap.cmd_daily,\n', 1)
bot.write_text(bt, encoding='utf-8')

test = Path('tests/test_daily_broadcast_rotation.py')
test.write_text('''from pathlib import Path


def test_admin_menu_has_daily_broadcast_entry():
    source = Path("v2/qiaolian_publisher_v2/keyboards.py").read_text(encoding="utf-8")
    assert "📢 每日广播" in source
    assert "cmd:daily" in source


def test_weekly_broadcast_plan_is_complete_and_quiet():
    source = Path("autopilot_publish_bot.py").read_text(encoding="utf-8")
    for label in ("周一｜金边今日信息", "周二｜今日可看房源", "周三｜区域怎么选", "周四｜租房费用", "周五｜周末看房", "周六｜看房前准备", "周日｜签约前确认"):
        assert label in source
    assert "每天只发 1 条" in source
    assert "daily:weekly" in source
    assert "daily:plan" in source


def test_scheduled_broadcast_uses_weekly_content():
    source = Path("autopilot_publish_bot.py").read_text(encoding="utf-8")
    block = source.split("async def scheduled_daily_broadcast", 1)[1].split("async def tick_daily_broadcast", 1)[0]
    assert "_daily_is_weekly()" in block
    assert "DAILY_TEMPLATES[template_no][1]" in block
    assert "_fetch_phnom_penh_daily_info" in block
''', encoding='utf-8')

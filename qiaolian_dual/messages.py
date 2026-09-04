from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from .config import ADVISOR_PHONE, ADVISOR_TG, ADVISOR_WECHAT, BRAND_NAME, CHANNEL_URL
from .utils import compact_join, e


def public_brand_name() -> str:
    """用户可见品牌名不携开发环境标记。"""
    return (BRAND_NAME or "侨联地产").replace("测试", "").strip() or "侨联地产"


def listing_summary(item: dict) -> str:
    tags = compact_join(item.get("tags", []), " / ")
    lines = [
        f"🏠 <b>{e(item.get('title'))}</b>",
        f"<b>金额：</b><b>${e(item.get('price'))}/月</b>",
        f"<b>区域：</b>{e(item.get('area'))} · {e(item.get('community'))}",
    ]
    if item.get("layout"):
        lines.append(f"<b>户型：</b>{e(item.get('layout'))}")
    if item.get("size_sqm"):
        lines.append(f"<b>面积：</b>{e(item.get('size_sqm'))}㎡")
    if tags:
        lines.append(f"<b>标签：</b>{e(tags)}")
    return "\n".join(lines)


def listing_detail(item: dict) -> str:
    from .text_utils import clean_telegram_text, remove_test_markers, fix_duplicate_words
    from .location_mapping import get_display_location
    from .utils_formatting import _fmt_price

    title = remove_test_markers(item.get('title', ''))
    listing_id = item.get('listing_id', '')
    area_raw = item.get('area', '')
    area = get_display_location(area_raw)
    community = remove_test_markers(item.get('community', ''))
    highlights = clean_telegram_text(item.get('highlights', ''))
    hidden_costs = clean_telegram_text(item.get('hidden_costs', ''))
    drawbacks = clean_telegram_text(item.get('drawbacks', ''))
    available_date = fix_duplicate_words(item.get('available_date', ''))

    location_values: list[str] = []
    for value in (area, community):
        normalized = str(value or '').strip()
        if normalized and normalized.casefold() not in {item.casefold() for item in location_values}:
            location_values.append(normalized)
    location = ' · '.join(location_values)
    lines = [f"🏠 <b>{e(title)}</b>", f"💰 <b>{e(_fmt_price(item.get('price')))}</b>"]
    if location and location not in title:
        lines.append(f"📍 {e(location)}")
    if item.get("layout") and str(item.get('layout')) not in title:
        lines.append(f"🛏 {e(item.get('layout'))}")
    if item.get("size_sqm"):
        lines.append(f"📐 {e(item.get('size_sqm'))}㎡")
    if item.get("deposit_rule"):
        lines.append(f"🔑 {e(item.get('deposit_rule'))}")
    if available_date:
        lines.append(f"📅 {e(available_date)}")
    if highlights:
        lines.append(f"✨ {e(highlights)}")
    if hidden_costs:
        lines.append(f"💡 {e(hidden_costs)}")
    if drawbacks:
        lines.append(f"⚠️ {e(drawbacks)}")
    return "\n".join(lines)


def viewing_delivery_assurance_text() -> str:
    return (
        "\n🛡️ <b>看房与交付保障</b>\n"
        "看中后：费用逐项核对。\n"
        "入住时：验房、水电表和钥匙/门卡确认留档。"
    )


def discussion_entry_welcome_text(first_name: str = "", listing_id: str = "") -> str:
    from .text_utils import remove_test_markers
    return ""


def lead_capture_text() -> str:
    return (
        "侨联顾问会直接通过 Telegram 接手，"
        "你不用重复填写联系方式或重新说明需求。"
    )


def advisor_text() -> str:
    return (
        "✅ <b>顾问已收到</b>\n\n"
        "顾问会通过 Telegram 联系你。\n"
        "这套房的信息已带上，不用重复说明。"
    )


def advisor_contact_supplement_text() -> str:
    return "顾问会直接通过 Telegram 联系你，无需另外填写手机号或微信。"


def deposit_text() -> str:
    return (
        f"🔒 <b>{BRAND_NAME} 费用与押金</b>\n\n"
        "<b>看房和签约前</b>\n"
        "我们会逐项核对：月租、押付、起租日、水电、网络、物业、停车。\n\n"
        "<b>入住当天</b>\n"
        "房屋现状、水电表和家具家电会留档，之后有事好对照。\n\n"
        "<b>准备退租时</b>\n"
        "如需，我们会协助和房东或物业沟通押金与交接事项。\n\n"
        "具体金额、责任和协助范围，以签约材料和双方确认内容为准。"
    )


def home_text() -> str:
    return (
        "你好，我是侨联小管家。\n\n"
        "金边租房，先选你现在要做的事。"
    )


def channel_welcome_text(first_name: str = "") -> str:
    if not first_name:
        return home_text()
    return f"👋 你好 <b>{e(first_name)}</b>。\n\n{home_text()}"


def about_text() -> str:
    return (
        f"🏠 <b>{public_brand_name()}｜金边租房中介</b>\n\n"
        "<b>我们的服务：</b>\n"
        "✅ 看房透明 — 实拍更新，所见即所得\n"
        "✅ 费用透明 — 无隐藏费用，提前说清\n"
        "✅ 入住留档 — 全程记录，售后无忧\n"
        "✅ 全程跟进 — 从看房到入住一条龙\n\n"
        f"📱 联系我们：{ADVISOR_TG}\n"
        f"📢 房源频道：{CHANNEL_URL}"
    )


def brand_text() -> str:
    return (
        f"🏠 <b>{public_brand_name()}｜金边租房中介</b>\n\n"
        "中文顾问帮你筛房、带看、确认费用、沟通条件并跟进签约入住。\n\n"
        "✅ 房源先筛 — 按需求收窄到 1-3 套\n"
        "✅ 费用透明 — 押付、水电提前说清\n"
        "✅ 实地/视频看房\n"
        "✅ 顾问全程跟进\n\n"
        f"📱 联系：{ADVISOR_TG}\n"
        f"📢 房源频道：{CHANNEL_URL}"
    )


def want_home_text() -> str:
    return (
        "📍 <b>提交找房需求</b>\n\n"
        "选出你最在意的条件即可。\n"
        "顾问会据此帮你缩小到 1–3 套更值得看的房源。"
    )


def service_promise_text() -> str:
    return (
        "🛡️ <b>服务承诺</b>\n\n"
        "<b>1. 看房无忧</b>\n"
        "支持实地看房与实时视频代看，您指到哪里，我们拍到哪里。\n\n"
        "<b>2. 费用透明</b>\n"
        "水电、物业、网络、停车等隐性成本，我们会在售前尽量说明清楚。\n\n"
        "<b>3. 入住交付有据可查</b>\n"
        "入住交付时，会逐项记录房屋现状、水电表、家具家电与钥匙/门卡；双方确认后归档，后续需要时可调取。\n\n"
        "<b>4. 全程跟进</b>\n"
        "从咨询到售后，需求都会同步到顾问，确保不断档。"
    )


def appointment_hub_text() -> str:
    return (
        "📅 <b>预约看房</b>\n\n"
        "看中具体房源后，直接点「预约看房」选择日期和时间。\n"
        "还没选好房源，可以先联系顾问。"
    )


def service_hub_text() -> str:
    return (
        "🛠 <b>入住服务</b>\n\n"
        "房子有问题或需要物业沟通，直接点下面办理。\n"
        "已绑定租约时会自动带上房屋信息，不用重复说明。"
    )


def help_text() -> str:
    return (
        "❓ <b>怎么使用</b>\n\n"
        "找房：点“帮我找房”，选择类型、位置和预算。\n"
        "看房：打开一套房源，点“预约看房”。\n"
        "咨询：点“联系中文顾问”，房源信息会自动带上。\n"
        "入住后：报修和物业沟通都在“入住服务”；绑定租约后，到期前 7 天会收到提醒。"
    )


def help_repeat_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton("🏠 入住服务", callback_data="hub:service")],
    ]
    ch = (CHANNEL_URL or "").strip()
    if ch:
        rows.append([InlineKeyboardButton("📢 频道实拍上新", url=ch)])
    rows.append([InlineKeyboardButton("🏠 返回首页", callback_data="home")])
    return InlineKeyboardMarkup(rows)


def search_entry_intro_text() -> str:
    return "🏠 <b>想找什么房？</b>\n\n选一个大概类型就行。"


def smart_find_play_prompt_text() -> str:
    return (
        "🎲 <b>一句话关键词</b>\n\n"
        "请直接发一句需求，无需固定格式。\n"
        "例如：<code>BKK1 预算800内 1房</code>\n"
        "也可以只发：<code>钻石岛</code>、<code>500以内</code>、<code>两房</code>、<code>视频看房</code>\n\n"
        "我会先按你说的方向筛一轮，再引导你继续缩小范围。"
    )


def smart_find_guided_header_text() -> str:
    return "🏠 <b>想找什么房？</b>\n\n选一个大概类型就行。"


def smart_find_play_footer_hint_text(*, used_fallback: bool) -> str:
    if used_fallback:
        return "\n\n先给你看接近的房源，还可以继续调整位置或预算。"
    return "\n\n如需更精准，可点「帮我找房」再按类型筛选。"


def repeat_tenant_ack_text() -> str:
    ch = (CHANNEL_URL or "").strip()
    ch_line = f"\n\n📢 实拍频道：<a href=\"{e(ch)}\">点这里关注上新</a>" if ch else ""
    return (
        "✅ <b>已登记为侨联老客回流</b>\n\n"
        "收到，顾问会继续帮你处理换房、续租或升级户型。"
        + ch_line
    )


def find_area_budget_hint_text() -> str:
    return (
        "💵 <b>预算大概在哪个区间？（USD/月）</b>\n\n"
        "<b>公寓常见参考</b>：<code>300以下</code> · <code>300–500</code> · <code>500–800</code> · "
        "<code>800–1200</code> · <code>1200以上</code>\n"
        "<b>别墅常见参考</b>：<code>800–1500</code> · <code>1500–2500</code> · <code>2500以上</code>\n\n"
        "一条消息里可同时带上户型，例如：<code>800–1200 两房</code>。"
    )


def listing_match_intro_text() -> str:
    return "✅ <b>已为你筛出更匹配的房源</b>（优先展示可快速决策的少量选项）"


def listing_match_footer_text() -> str:
    return (
        "\n\n<b>下一步</b>：点「📅 预约看房」安排到场，或点「💬 联系中文顾问」帮你对比选择。"
    )


def find_no_match_text() -> str:
    return (
        "暂时没找到完全符合条件的房源。\n"
        "可以换一个预算或位置，\n"
        "也可以让顾问继续帮你找。"
    )


def want_home_ack_text() -> str:
    return (
        "✅ <b>已收到你的找房条件</b>\n\n"
        "顾问会按这些条件帮你筛出 1–3 套，并提前标注关键费用，方便你对比。\n\n"
        "想更快收到推荐，也可以补充预算上限、民水民电、电梯/泳池需求，或直接发截图。"
    )

"""Integrated product copy for the modular User Bot.

Keeps the UI-clean module graph while applying the newer customer-facing
positioning from master.  The original V2.2 implementation remains in
messages_base.py and is re-exported here for compatibility.
"""
from __future__ import annotations

from .messages_base import *  # noqa: F401,F403


def home_text() -> str:
    advisor = (ADVISOR_TG or "").strip()
    if advisor and not advisor.startswith("@"):
        advisor = f"@{advisor}"
    advisor = advisor or "@qiaolian_support"
    return (
        "🏠 <b>侨联地产 · 金边华人房产服务</b>\n\n"
        "真实房源 · 实拍更新\n"
        "公寓 · 别墅 · 商铺 · 土地\n\n"
        "━━━━━━━━━━━━\n\n"
        "在金边找房，找自己人\n\n"
        f"📱 顾问：{e(advisor)}\n\n"
        "👇 点按钮开始："
    )


def channel_welcome_text(first_name: str = "") -> str:
    name_part = f" {e(first_name)}" if first_name else ""
    return (
        f"👋 欢迎{name_part}来到 <b>侨联找房助手</b>\n\n"
        "找房、看房、视频代看、入住后的服务，我都可以帮你。\n\n"
        "你可以直接告诉我需求，例如：\n\n"
        "<code>富力800公寓</code>\n"
        "<code>BKK一居</code>\n"
        "<code>1000以内房子</code>\n\n"
        "也可以点击下面入口开始："
    )


def discussion_entry_welcome_text(first_name: str = "", listing_id: str = "") -> str:
    name_part = f" {e(first_name)}" if first_name else ""
    listing_line = f"🏠 房源：<code>{e(listing_id)}</code>\n\n" if listing_id else ""
    return (
        f"🤖 <b>侨联找房助手{name_part}</b>\n\n"
        f"{listing_line}"
        "已同步房源信息到顾问后台，继续点按钮即可：\n\n"
        "📅 预约实地看房 / 视频代看\n"
        "📍 按区域继续找同类房源\n"
        "💰 按预算收窄到 1–3 套\n\n"
        "👇 先选一个继续"
    )


def advisor_text() -> str:
    return (
        "✅ 已收到你的需求\n\n"
        "侨联顾问会直接通过 Telegram 联系你确认：\n"
        "• 房源是否还在\n"
        "• 看房时间安排\n"
        "• 实地看房 / 视频看房方式\n\n"
        "你也可以继续看其他房源。"
    )


def about_text() -> str:
    return (
        f"🏢 <b>关于{public_brand_name()}</b>\n\n"
        f"{public_brand_name()}是金边华人房产服务团队，核心定位是<b>你在金边的自己人</b>。\n\n"
        "<b>我们的工作方式</b>\n"
        "• <b>房源先筛选</b>：按预算 + 区域 + 户型先收窄到 1–3 套，减少无效看房\n"
        "• <b>费用先对齐</b>：押付、水电、物业、网络等关键项尽量提前说清楚\n"
        "• <b>过程可追踪</b>：预约、咨询、入住、售后统一由管理号持续跟进\n"
        "• <b>入住后不断档</b>：报修、物业、续租、换房和退租继续有人接\n\n"
        "你可以先从「智能找房」或「预约看房」开始。"
    )


def brand_text() -> str:
    return (
        "<b>📖 侨联地产</b>\n\n"
        "在金边找房，真正麻烦的往往不只是找到一套房。\n\n"
        "房源是否真实、费用有没有说清楚、合同怎么签、入住以后出了问题找谁、退租押金怎么处理，都是租房的一部分。\n\n"
        "所以侨联希望做的不只是一个发房源的中介。\n\n"
        "从找房、看房、签约、入住，到报修、续租、换房和退租，都有人继续跟进。\n\n"
        "<b>侨联地产｜你在金边的自己人</b>"
    )


def want_home_text() -> str:
    return (
        "<b>📍 条件筛选</b>\n\n"
        "按预算、区域和户型快速收窄，不用反复打字。\n"
        "提交后会同步给顾问，人工收窄到 1–3 套。"
    )


def service_promise_text() -> str:
    return (
        "<b>🛡️ 服务承诺</b>\n\n"
        "<b>1）看房无忧</b>\n"
        "支持实地看房与实时视频代看，你关心哪里，我们重点看哪里。\n\n"
        "<b>2）费用提前说清</b>\n"
        "押付、水电、物业、网络、停车等关键费用尽量在签约前对齐。\n\n"
        "<b>3）入住交接有留档</b>\n"
        "房屋现状、水电表、家具家电与钥匙/门卡逐项记录，退租时可对照。\n\n"
        "<b>4）整个租期有人接</b>\n"
        "报修、物业沟通、续租、换房和退租，都可以继续从入住服务发起。"
    )


def appointment_hub_text() -> str:
    return (
        "<b>📅 预约看房</b>\n\n"
        "看中某套房，预约会自动带上房源，不用重新说明。\n"
        "支持实地看房和视频代看；还没定房，也可以先让顾问继续帮你收窄。"
    )


def service_hub_text() -> str:
    return (
        "🛡 <b>入住服务</b>\n\n"
        "<b>入住才是侨联服务的开始。</b>\n\n"
        "通过侨联租房后，整个租期内的住房问题，都可以从这里继续处理：\n\n"
        "• 报修与维护\n"
        "• 物业沟通\n"
        "• 续租 / 换房\n"
        "• 退租与押金核对\n"
        "• 租约与入住资料\n\n"
        "已绑定租约时会自动带上房屋信息，不用重复说明。\n\n"
        "<b>签约不是结束，入住后有人接得住，才是真的省心。</b>"
    )


def help_text() -> str:
    return (
        "<b>📘 使用说明</b>\n\n"
        "找房：点「🔍 智能找房」按区域、预算、户型筛选。\n"
        "看房：打开房源后直接预约实地看房或视频代看。\n"
        "咨询：点「💬 联系顾问」，房源信息会自动带上。\n"
        "入住后：报修、物业、续租、换房和退租统一进入「🛡 入住服务」。\n"
        "已绑定租约的客户，可直接查看租约并接收到期提醒。"
    )


def help_repeat_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton("📋 我的租约", callback_data="contract:view"), InlineKeyboardButton("🛡 入住服务", callback_data="service:hub")],
    ]
    ch = (CHANNEL_URL or "").strip()
    if ch:
        rows.append([InlineKeyboardButton("📢 频道实拍上新", url=ch)])
    rows.append([InlineKeyboardButton("🏠 返回首页", callback_data="home")])
    return InlineKeyboardMarkup(rows)


def search_entry_intro_text() -> str:
    return (
        "<b>🔍 智能找房</b>\n\n"
        "按区域、预算和户型直接筛，也可以发一句需求给我。\n"
        "例如：<code>BKK1 500以内 一房</code>。"
    )


def smart_find_guided_header_text() -> str:
    return (
        "<b>📍 按条件找</b>\n\n"
        "区域、预算、户型任选一个入口开始，后面可以继续收窄。"
    )


def find_no_match_text() -> str:
    return (
        "暂时没有完全符合条件的在架房源。\n\n"
        "需求已经可以继续交给顾问跟进；也可以调整预算或区域看看相近房源。"
    )


def want_home_ack_text() -> str:
    return (
        "✅ <b>已收到你的找房条件</b>\n\n"
        "顾问会按这些条件人工收窄到 1–3 套，并提前标注关键费用，方便比较。"
    )

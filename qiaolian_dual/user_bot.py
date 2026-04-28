"""侨联用户 Bot：承接频道深链，记录留资、预约和找房偏好。"""
from __future__ import annotations

from html import escape as he

import logging
import re
import sqlite3
from datetime import datetime, time as dt_time, timedelta
from urllib.parse import quote

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

try:
    from .config import ADVISOR_TG, ADMIN_IDS, DB_PATH, USER_BOT_TOKEN, USER_BOT_USERNAME
    from .db import Database
    from .messages import (
        about_text as copy_about_text,
        appointment_hub_text as copy_appointment_hub_text,
        advisor_text as copy_advisor_text,
        brand_text as copy_brand_text,
        channel_welcome_text as copy_channel_welcome_text,
        deposit_text as copy_deposit_text,
        discussion_entry_welcome_text as copy_discussion_entry_welcome_text,
        find_area_budget_hint_text,
        find_no_match_text,
        help_repeat_keyboard,
        help_text as copy_help_text,
        home_text as copy_home_text,
        lead_capture_text as copy_lead_capture_text,
        listing_match_footer_text,
        listing_match_intro_text,
        local_life_text as copy_local_life_text,
        merchant_join_text as copy_merchant_join_text,
        rfcity_bbq_text as copy_rfcity_bbq_text,
        rfcity_drinks_text as copy_rfcity_drinks_text,
        rfcity_hotel_text as copy_rfcity_hotel_text,
        rfcity_logistics_text as copy_rfcity_logistics_text,
        rfcity_property_text as copy_rfcity_property_text,
        rfcity_recreation_text as copy_rfcity_recreation_text,
        rfcity_restaurant_text as copy_rfcity_restaurant_text,
        rfcity_supermarket_text as copy_rfcity_supermarket_text,
        rfcity_text as copy_rfcity_text,
        search_entry_intro_text,
        service_promise_text as copy_service_promise_text,
        service_hub_text as copy_service_hub_text,
        smart_find_guided_header_text,
        smart_find_play_footer_hint_text,
        smart_find_play_prompt_text,
        want_home_text as copy_want_home_text,
    )
except ImportError:  # pragma: no cover - script mode fallback
    from qiaolian_dual.config import ADVISOR_TG, ADMIN_IDS, DB_PATH, USER_BOT_TOKEN, USER_BOT_USERNAME
    from qiaolian_dual.db import Database
    from qiaolian_dual.messages import (
        about_text as copy_about_text,
        appointment_hub_text as copy_appointment_hub_text,
        advisor_text as copy_advisor_text,
        brand_text as copy_brand_text,
        channel_welcome_text as copy_channel_welcome_text,
        deposit_text as copy_deposit_text,
        discussion_entry_welcome_text as copy_discussion_entry_welcome_text,
        find_area_budget_hint_text,
        find_no_match_text,
        help_repeat_keyboard,
        help_text as copy_help_text,
        home_text as copy_home_text,
        lead_capture_text as copy_lead_capture_text,
        listing_match_footer_text,
        listing_match_intro_text,
        local_life_text as copy_local_life_text,
        merchant_join_text as copy_merchant_join_text,
        rfcity_bbq_text as copy_rfcity_bbq_text,
        rfcity_drinks_text as copy_rfcity_drinks_text,
        rfcity_hotel_text as copy_rfcity_hotel_text,
        rfcity_logistics_text as copy_rfcity_logistics_text,
        rfcity_property_text as copy_rfcity_property_text,
        rfcity_recreation_text as copy_rfcity_recreation_text,
        rfcity_restaurant_text as copy_rfcity_restaurant_text,
        rfcity_supermarket_text as copy_rfcity_supermarket_text,
        rfcity_text as copy_rfcity_text,
        search_entry_intro_text,
        service_promise_text as copy_service_promise_text,
        service_hub_text as copy_service_hub_text,
        smart_find_guided_header_text,
        smart_find_play_footer_hint_text,
        smart_find_play_prompt_text,
        want_home_text as copy_want_home_text,
    )


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

MAIN, FIND_AREA, FIND_BUDGET, APPT_MODE, APPT_FOCUS, APPT_DATE, APPT_TIME, APPT_CONFIRM = range(8)
AREA_HINTS = [
    "BKK1",
    "BKK2",
    "金边市区",
    "森速",
    "万景岗",
    "俄罗斯市场",
    "钻石岛",
    "不限",
]
ROOM_TYPE_HINTS = {
    "studio": ["studio", "开间", "单间"],
    "1房": ["1房", "一房", "1br", "1 bed", "一居"],
    "2房": ["2房", "二房", "2br", "2 bed", "两居"],
    "3房": ["3房", "三房", "3br", "3 bed", "三居"],
}
START_ACTIONS = ("consult", "appoint", "fav", "more")
START_ACTION_ALIASES = {
    "a": "appoint",
    "f": "fav",
    "m": "more",
    "q": "consult",
}
START_ACTION_CODES = {action: alias for alias, action in START_ACTION_ALIASES.items()}
APPOINTMENT_MODE_LABELS = {
    "offline": "实地看房",
    "video": "实时视频代看",
}
APPOINTMENT_TIME_LABELS = {
    "am": "上午 (9:00-12:00)",
    "pm": "下午 (14:00-18:00)",
}
APPOINTMENT_FOCUS_LABELS = {
    "ac": "空调型号和老旧程度",
    "appliances": "冰箱/洗衣机/家具使用痕迹",
    "light_noise": "采光、噪音、窗外环境",
    "water": "水压、热水、排水",
    "fee_contract": "费用和合同细节",
}
APPOINTMENT_FOCUS_ORDER = ["ac", "appliances", "light_noise", "water", "fee_contract"]
APPOINTMENT_STATUS_LABELS = {
    "pending": "待确认",
    "confirmed": "已确认",
    "done": "已完成",
    "cancelled": "已取消",
}
LEASE_REMINDER_DAYS = (30, 7, 3)


def _fmt_price(price: object) -> str:
    if isinstance(price, (int, float)):
        return f"${int(price)}/月" if price > 0 else "价格待确认"
    s = str(price or "").strip()
    if not s:
        return "价格待确认"
    digits = re.sub(r"[^\d]", "", s)
    if digits:
        try:
            value = int(digits)
        except ValueError:
            return "价格待确认"
        if value > 0:
            return f"${value}/月"
    return "价格待确认"
SERVICE_REQUEST_LABELS = {
    "repair_ac": "空调 / 家电故障",
    "repair_water": "水管漏水 / 下水堵塞",
    "repair_power": "门锁 / 灯具 / 电路问题",
    "property": "物业沟通",
}

PREF_CONDITION_LABELS = {
    "budget": "预算优先",
    "area": "区域优先",
    "utility": "必须民水民电",
    "parking": "停车方便",
    "quiet": "安静不吵",
    "sunlight": "采光好",
    "pet": "可养宠物",
    "furnished": "拎包入住",
    "chinese_owner": "中国房东",
    "amenity": "电梯/泳池",
}

FIND_AREA_OPTIONS: list[tuple[str, str]] = [
    ("a1", "富力城"),
    ("a2", "炳发城"),
    ("a3", "太子/幸福"),
    ("a4", "BKK1"),
    ("a5", "钻石岛"),
    ("a6", "TK/7月区"),
    ("a7", "洪森大道"),
    ("a8", "森速"),
    ("a9", "金边市区"),
    ("a0", "不限"),
]
FIND_AREA_CODE_MAP = {code: area for code, area in FIND_AREA_OPTIONS}

FIND_BUDGET_OPTIONS: dict[str, list[tuple[str, str, int | None, int | None]]] = {
    "住宅": [
        ("r1", "$300以下", None, 300),
        ("r2", "$300-500", 300, 500),
        ("r3", "$500-800", 500, 800),
        ("r4", "$800-1200", 800, 1200),
        ("r5", "$1200以上", 1200, None),
        ("rn", "不限", None, None),
    ],
    "别墅/排屋": [
        ("v1", "$800-1500", 800, 1500),
        ("v2", "$1500-2500", 1500, 2500),
        ("v3", "$2500以上", 2500, None),
        ("vn", "不限", None, None),
    ],
    "商铺/办公": [
        ("o1", "$500以下", None, 500),
        ("o2", "$500-1000", 500, 1000),
        ("o3", "$1000-2000", 1000, 2000),
        ("o4", "$2000以上", 2000, None),
        ("on", "不限", None, None),
    ],
    "any": [
        ("n1", "$300以下", None, 300),
        ("n2", "$300-500", 300, 500),
        ("n3", "$500-800", 500, 800),
        ("n4", "$800-1200", 800, 1200),
        ("n5", "$1200以上", 1200, None),
        ("nn", "不限", None, None),
    ],
}

db = Database(DB_PATH)
PANEL_ANCHOR_KEY = "_panel_anchor"


def main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🏠 推荐房源", callback_data="hub:latest"),
                InlineKeyboardButton("📍 按区域找", callback_data="hub:area"),
            ],
            [
                InlineKeyboardButton("📅 预约看房", callback_data="hub:appoint"),
                InlineKeyboardButton("🎥 视频代看", callback_data="hub:video_tour"),
            ],
            [
                InlineKeyboardButton("🧰 入住服务", callback_data="hub:service"),
                InlineKeyboardButton("💬 联系顾问", callback_data="hub:advisor"),
            ],
        ]
    )


def no_match_followup_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("💬 联系顾问", callback_data="appointment_menu:contact")],
            [
                InlineKeyboardButton("🎯 继续筛选", callback_data="findmode:guided"),
                InlineKeyboardButton("🏠 返回首页", callback_data="home"),
            ],
        ]
    )


def quick_start_keyboard() -> InlineKeyboardMarkup:
    return main_keyboard()


def room_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🛏 单间 / Studio", callback_data="roompick:studio"),
                InlineKeyboardButton("🛏 1房", callback_data="roompick:1房"),
            ],
            [
                InlineKeyboardButton("🛏 2房", callback_data="roompick:2房"),
                InlineKeyboardButton("🛏 3房+", callback_data="roompick:3房"),
            ],
            [
                InlineKeyboardButton("🏡 别墅 / 排屋", callback_data="roompick:别墅"),
                InlineKeyboardButton("🏬 商铺 / 办公", callback_data="roompick:商铺"),
            ],
            [InlineKeyboardButton("🏠 返回首页", callback_data="home")],
        ]
    )


def latest_listing_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📍 按区域继续找", callback_data="hub:area"),
                InlineKeyboardButton("💰 按预算继续找", callback_data="hub:budget"),
            ],
            [
                InlineKeyboardButton("🛏 按户型继续找", callback_data="hub:layout"),
                InlineKeyboardButton("🎥 视频代看", callback_data="hub:video_tour"),
            ],
            [
                InlineKeyboardButton("💬 联系顾问", callback_data="hub:advisor"),
                InlineKeyboardButton("🏠 返回首页", callback_data="home"),
            ],
        ]
    )


def keyword_followup_keyboard(*, area: str = "", room_type: str = "") -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if area:
        rows.append(
            [
                InlineKeyboardButton("💰 再按预算缩小", callback_data="hub:budget"),
                InlineKeyboardButton("🛏 再按户型缩小", callback_data="hub:layout"),
            ]
        )
    elif room_type:
        rows.append(
            [
                InlineKeyboardButton("📍 补一个区域", callback_data="hub:area"),
                InlineKeyboardButton("💰 补一个预算", callback_data="hub:budget"),
            ]
        )
    else:
        rows.append(
            [
                InlineKeyboardButton("📍 按区域继续找", callback_data="hub:area"),
                InlineKeyboardButton("💰 按预算继续找", callback_data="hub:budget"),
            ]
        )
    rows.append(
        [
            InlineKeyboardButton("🎥 视频代看", callback_data="appointment_menu:video"),
            InlineKeyboardButton("💬 联系顾问", callback_data="appointment_menu:contact"),
        ]
    )
    rows.append([InlineKeyboardButton("🏠 返回首页", callback_data="home")])
    return InlineKeyboardMarkup(rows)


def _advisor_tg_url() -> str:
    handle = str(ADVISOR_TG or "").strip().lstrip("@")
    return f"https://t.me/{handle}" if handle else ""


def contact_handoff_keyboard() -> InlineKeyboardMarkup:
    advisor_url = _advisor_tg_url()
    chat_btn = (
        InlineKeyboardButton("💬 联系顾问", url=advisor_url)
        if advisor_url
        else InlineKeyboardButton("💬 联系顾问", callback_data="appointment_menu:contact")
    )
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📅 预约实地看房", callback_data="appointment_menu:offline"),
                InlineKeyboardButton("🎥 改视频代看", callback_data="appointment_menu:video"),
            ],
            [
                InlineKeyboardButton("🏠 继续看房", callback_data="home"),
                chat_btn,
            ],
        ]
    )


def channel_return_keyboard(channel_url: str = "") -> InlineKeyboardMarkup:
    """完成私聊转化后，给用户返回频道或继续筛选的选择。"""
    rows: list[list[InlineKeyboardButton]] = []
    if channel_url and channel_url.strip():
        rows.append([InlineKeyboardButton("📺 返回频道继续看", url=channel_url)])
    rows.append(
        [
            InlineKeyboardButton("🔍 继续筛选其他房源", callback_data="hub:find"),
            InlineKeyboardButton("🏠 返回首页", callback_data="home"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def lead_capture_keyboard() -> InlineKeyboardMarkup:
    """留资触发后的操作按钮：发手机号 / 直接联系顾问 / 继续看房。"""
    rows: list[list[InlineKeyboardButton]] = []
    advisor_url = _advisor_tg_url()
    rows.append([InlineKeyboardButton("📱 发送手机号", callback_data="lead_capture:phone")])
    if advisor_url:
        rows.append([InlineKeyboardButton("💬 联系顾问", url=advisor_url)])
    else:
        rows.append([InlineKeyboardButton("💬 联系顾问", callback_data="hub:advisor")])
    rows.append([InlineKeyboardButton("🏠 继续看房", callback_data="home")])
    return InlineKeyboardMarkup(rows)


def old_tenant_followup_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📋 我的租约", callback_data="contract:view"),
                InlineKeyboardButton("🔄 续租咨询", callback_data="contract:renew"),
            ],
            [
                InlineKeyboardButton("🏠 我要换房", callback_data="contract:change"),
                InlineKeyboardButton("💬 联系顾问", callback_data="appointment_menu:contact"),
            ],
            [InlineKeyboardButton("📅 预约看房", callback_data="appointment_menu:offline")],
            [InlineKeyboardButton("🏠 返回首页", callback_data="home")],
        ]
    )


def welcome_text() -> str:
    return copy_home_text()


def channel_welcome_text(first_name: str = "") -> str:
    """首屏 /start 欢迎语，压缩版：一屏内展示核心动作按钮。"""
    return copy_channel_welcome_text(first_name=first_name)


def discussion_entry_welcome_text(first_name: str = "", listing_id: str = "") -> str:
    return copy_discussion_entry_welcome_text(first_name=first_name, listing_id=listing_id)


def lead_capture_text() -> str:
    """留资触发节点文案：在关键行为后请求联系方式。"""
    return copy_lead_capture_text()


def _channel_index_action(action: str) -> dict | None:
    mapping = {
        "find_area": {"action": "index_area", "target": "", "post_token": "", "channel_message_id": None},
        "find_budget": {"action": "index_budget", "target": "", "post_token": "", "channel_message_id": None},
        "find_layout": {"action": "index_layout", "target": "", "post_token": "", "channel_message_id": None},
        "latest": {"action": "index_latest", "target": "", "post_token": "", "channel_message_id": None},
        "video": {"action": "index_video", "target": "", "post_token": "", "channel_message_id": None},
        "advisor": {"action": "index_advisor", "target": "", "post_token": "", "channel_message_id": None},
        "service": {"action": "index_service", "target": "", "post_token": "", "channel_message_id": None},
    }
    return mapping.get(action)


async def render_panel(
    update: Update,
    *,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
    parse_mode: str | None = None,
    context: ContextTypes.DEFAULT_TYPE | None = None,
    prefer_edit_anchor: bool = False,
) -> None:
    """统一面板渲染：优先就地编辑，其次回退新消息。"""
    query = update.callback_query
    if query is not None and query.message is not None:
        kwargs: dict[str, object] = {"text": text, "reply_markup": reply_markup}
        if parse_mode:
            kwargs["parse_mode"] = parse_mode
        try:
            await query.edit_message_text(**kwargs)
            if context is not None:
                context.user_data[PANEL_ANCHOR_KEY] = {
                    "chat_id": int(query.message.chat_id),
                    "message_id": int(query.message.message_id),
                }
            return
        except Exception as exc:
            # 某些场景（文本完全一致）会报 message is not modified，回退发新消息避免卡死
            if "message is not modified" not in str(exc).lower():
                logger.debug("render_panel edit failed, fallback to send: %s", exc)
    if context is not None and prefer_edit_anchor and query is None:
        anchor = context.user_data.get(PANEL_ANCHOR_KEY) or {}
        chat_id = anchor.get("chat_id")
        message_id = anchor.get("message_id")
        if isinstance(chat_id, int) and isinstance(message_id, int):
            kwargs_anchor: dict[str, object] = {"chat_id": chat_id, "message_id": message_id, "text": text}
            if reply_markup is not None:
                kwargs_anchor["reply_markup"] = reply_markup
            if parse_mode:
                kwargs_anchor["parse_mode"] = parse_mode
            try:
                await update.get_bot().edit_message_text(**kwargs_anchor)
                return
            except Exception as exc:
                logger.debug("render_panel anchor edit failed, fallback to send: %s", exc)
    msg = update.effective_message
    kwargs2: dict[str, object] = {"text": text, "reply_markup": reply_markup}
    if parse_mode:
        kwargs2["parse_mode"] = parse_mode
    sent = await msg.reply_text(**kwargs2)
    if context is not None:
        context.user_data[PANEL_ANCHOR_KEY] = {
            "chat_id": int(sent.chat_id),
            "message_id": int(sent.message_id),
        }


def promise_text() -> str:
    return copy_service_promise_text()


def deposit_text() -> str:
    return copy_deposit_text()


def advisor_text() -> str:
    return copy_advisor_text()


def advisor_handoff_text(*, listing_id: str = "", user_id: int | None = None) -> str:
    listing_id = str(listing_id or "").strip()
    if listing_id:
        item = listing_context(listing_id)
        area = str(item.get("area") or "金边")
        layout = str(item.get("layout") or item.get("property_type") or "房源")
        price_text = _fmt_price(item.get("price"))
        return (
            f"✅ <b>你刚看的这套房源已接入</b>\n\n"
            f"🏠 {he(area)} | {he(layout)}\n"
            f"💰 {he(price_text)}\n\n"
            "<b>接下来我可以帮你：</b>\n"
            "📋 给你这个预算 / 区域的其他 3-5 套对比选项\n"
            "📅 安排实地看房或实时视频代看\n"
            "🧑‍💼 直接连顾问，他会按当前房源继续跟进\n\n"
            "直接点下方按钮开始，管理号同步接入。"
        )
    if user_id:
        binding = db.get_active_binding(user_id)
        if binding:
            return (
                "💬 <b>顾问已接手当前租约事项</b>\n\n"
                f"🏠 当前房号：{he(str(binding.get('property_name') or '-'))}\n"
                f"📅 到期：{he(_binding_end_date(binding) or '待确认')}\n\n"
                "请直接点下方按钮继续，管理号会按您当前租约继续跟进。"
            )
    return copy_advisor_text()


def appointment_hub_text() -> str:
    return copy_appointment_hub_text()


def about_text() -> str:
    return copy_about_text()


def brand_story_text() -> str:
    return copy_brand_text()


def help_text() -> str:
    return copy_help_text()


def service_hub_text() -> str:
    return copy_service_hub_text()


def local_life_text() -> str:
    return copy_local_life_text()


def rfcity_text() -> str:
    return copy_rfcity_text()


def want_home_prompt_text() -> str:
    return copy_want_home_text()


def _search_type_button_rows() -> list[list[InlineKeyboardButton]]:
    return [
        [
            InlineKeyboardButton("🏢 公寓 / 住宅", callback_data="findtype:住宅"),
            InlineKeyboardButton("🏡 别墅 / 排屋", callback_data="findtype:别墅/排屋"),
        ],
        [
            InlineKeyboardButton("🏬 写字楼 / 商铺", callback_data="findtype:商铺/办公"),
            InlineKeyboardButton("🤝 直接帮我找", callback_data="findtype:any"),
        ],
    ]


def search_entry_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton("🎲 一句话关键词", callback_data="findmode:play"),
            InlineKeyboardButton("📍 按类型找（2～3步）", callback_data="findmode:guided"),
        ],
    ]
    rows.extend(_search_type_button_rows())
    rows.append([InlineKeyboardButton("🏠 返回首页", callback_data="home")])
    return InlineKeyboardMarkup(rows)


def guided_search_keyboard() -> InlineKeyboardMarkup:
    rows = list(_search_type_button_rows())
    rows.append([InlineKeyboardButton("🏠 返回首页", callback_data="home")])
    return InlineKeyboardMarkup(rows)


def find_area_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for idx, (code, label) in enumerate(FIND_AREA_OPTIONS, start=1):
        row.append(InlineKeyboardButton(label, callback_data=f"findarea:{code}"))
        if idx % 2 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("🏠 返回首页", callback_data="home")])
    return InlineKeyboardMarkup(rows)


def _budget_options_for_goal(goal: str) -> list[tuple[str, str, int | None, int | None]]:
    key = goal if goal in FIND_BUDGET_OPTIONS else "any"
    return FIND_BUDGET_OPTIONS.get(key, FIND_BUDGET_OPTIONS["any"])


def find_budget_keyboard(goal: str) -> InlineKeyboardMarkup:
    options = _budget_options_for_goal(goal)
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for idx, (code, label, _, _) in enumerate(options, start=1):
        row.append(InlineKeyboardButton(label, callback_data=f"findbudget:{code}"))
        if idx % 2 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append(
        [
            InlineKeyboardButton("⬅️ 返回区域", callback_data="findback:area"),
            InlineKeyboardButton("🏠 返回首页", callback_data="home"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def _decode_budget_choice(goal: str, code: str) -> tuple[str, int | None, int | None]:
    for opt_code, label, bmin, bmax in _budget_options_for_goal(goal):
        if opt_code == code:
            return label, bmin, bmax
    return "不限", None, None


def appointment_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📅 预约实地看房", callback_data="appointment_menu:offline"),
                InlineKeyboardButton("🎥 视频代看", callback_data="appointment_menu:video"),
            ],
            [
                InlineKeyboardButton("📋 查看我的预约", callback_data="appointment_menu:list"),
                InlineKeyboardButton("💬 联系顾问", callback_data="appointment_menu:contact"),
            ],
            [InlineKeyboardButton("🏠 返回首页", callback_data="home")],
        ]
    )


def precise_filter_keyboard(selected: set[str] | None = None) -> InlineKeyboardMarkup:
    picked = selected or set()

    def _btn(key: str) -> InlineKeyboardButton:
        label = PREF_CONDITION_LABELS.get(key, key)
        prefix = "✅ " if key in picked else "▫️ "
        return InlineKeyboardButton(f"{prefix}{label}", callback_data=f"pref:toggle:{key}")

    rows = [
        [_btn("budget"), _btn("area")],
        [_btn("utility"), _btn("parking")],
        [_btn("quiet"), _btn("sunlight")],
        [_btn("pet"), _btn("furnished")],
        [_btn("chinese_owner"), _btn("amenity")],
        [
            InlineKeyboardButton("✅ 提交条件", callback_data="pref:submit"),
            InlineKeyboardButton("♻️ 清空", callback_data="pref:clear"),
        ],
        [InlineKeyboardButton("🏠 返回首页", callback_data="home")],
    ]
    return InlineKeyboardMarkup(rows)


def service_hub_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔧 我要报修", callback_data="service:repair_hub")],
            [InlineKeyboardButton("🏢 物业沟通", callback_data="service_request:property")],
            [InlineKeyboardButton("🔁 续租/换房", callback_data="service:renew_change")],
            [InlineKeyboardButton("🗺️ 周边生活", callback_data="service:local_life")],
            [InlineKeyboardButton("💬 联系顾问", callback_data="service:contact")],
        ]
    )


def service_repair_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("❄️ 空调 / 家电故障", callback_data="service_request:repair_ac"),
                InlineKeyboardButton("💧 水管漏水 / 下水堵塞", callback_data="service_request:repair_water"),
            ],
            [
                InlineKeyboardButton("🔌 门锁 / 灯具 / 电路问题", callback_data="service_request:repair_power"),
                InlineKeyboardButton("🏢 物业沟通", callback_data="service_request:property"),
            ],
            [InlineKeyboardButton("⬅️ 返回入住服务", callback_data="service:hub")],
        ]
    )


def service_detail_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("💬 联系顾问", callback_data="service:contact")],
            [
                InlineKeyboardButton("⬅️ 返回入住服务", callback_data="service:hub"),
            ],
        ]
    )


def local_life_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🏙 富力周边", callback_data="local:rfcity")],
            [InlineKeyboardButton("⬅️ 返回入住服务", callback_data="service:hub")],
        ]
    )


def rfcity_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🍴 餐厅小吃", callback_data="rfcity:restaurant"),
                InlineKeyboardButton("🔥 烧烤夜宵", callback_data="rfcity:bbq"),
            ],
            [
                InlineKeyboardButton("🥤 奶茶饮品", callback_data="rfcity:drinks"),
                InlineKeyboardButton("🛒 超市便利", callback_data="rfcity:supermarket"),
            ],
            [
                InlineKeyboardButton("🏨 酒店租房", callback_data="rfcity:hotel"),
                InlineKeyboardButton("🏋️ 休闲生活", callback_data="rfcity:recreation"),
            ],
            [
                InlineKeyboardButton("🚛 快递物流", callback_data="rfcity:logistics"),
                InlineKeyboardButton("👨‍💻 富力物业", callback_data="rfcity:property"),
            ],
            [InlineKeyboardButton("🤝 商家入驻", callback_data="rfcity:join")],
            [InlineKeyboardButton("⬅️ 返回周边生活", callback_data="service:local_life")],
        ]
    )


def rfcity_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🏙 返回富力周边", callback_data="local:rfcity")],
        ]
    )


def merchant_join_keyboard() -> InlineKeyboardMarkup:
    advisor_url = _advisor_tg_url()
    rows: list[list[InlineKeyboardButton]] = []
    if advisor_url:
        rows.append([InlineKeyboardButton("📩 提交商家信息", url=advisor_url)])
        rows.append([InlineKeyboardButton("💬 联系侨联合作", url=advisor_url)])
    else:
        rows.append([InlineKeyboardButton("📩 提交商家信息", callback_data="service:contact")])
        rows.append([InlineKeyboardButton("💬 联系侨联合作", callback_data="service:contact")])
    rows.append([InlineKeyboardButton("🏙 返回富力周边", callback_data="local:rfcity")])
    return InlineKeyboardMarkup(rows)


def user_display_name(user) -> str:
    return (getattr(user, "full_name", "") or getattr(user, "first_name", "") or "").strip()


def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def clear_main_flags(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in (
        "awaiting_consult",
        "awaiting_want_home",
        "awaiting_service_request",
        "awaiting_general_appointment",
        "awaiting_keyword_find",
        "search_pref",
    ):
        context.user_data.pop(key, None)


def clear_session_for_fresh_entry(context: ContextTypes.DEFAULT_TYPE) -> None:
    """新 /start 或作为入口的斜杠命令：清咨询/找房标志，并丢弃未完成的预约草稿，避免与新的深链打架。"""
    clear_main_flags(context)
    context.user_data.pop("appt", None)
    context.user_data.pop("contact_touch_payload", None)


def _remember_video_pref(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    area: str | None = None,
    budget_min: int | None = None,
    budget_max: int | None = None,
    layout: str | None = None,
) -> None:
    snap = context.user_data.get("video_pref")
    if not isinstance(snap, dict):
        snap = {}
    if area is not None:
        snap["area"] = str(area or "").strip()
    if budget_min is not None:
        snap["budget_min"] = budget_min
    if budget_max is not None:
        snap["budget_max"] = budget_max
    if layout is not None:
        snap["layout"] = str(layout or "").strip()
    context.user_data["video_pref"] = snap


def _base36_decode(token: str) -> int | None:
    try:
        return int((token or "").lower(), 36)
    except ValueError:
        return None


def parse_start_arg_payload(arg: str) -> dict | None:
    index_payload = _channel_index_action(arg)
    if index_payload is not None:
        return index_payload
    if arg in {"brand", "about", "want_home", "ask"}:
        return {
            "action": arg,
            "target": "",
            "post_token": "",
            "channel_message_id": None,
        }
    if arg == "more":
        return {
            "action": "more",
            "target": "",
            "post_token": "",
            "channel_message_id": None,
        }
    if arg.startswith("t_bind_"):
        return {
            "action": "tenant_bind",
            "target": arg[len("t_bind_") :],
            "post_token": "",
            "channel_message_id": None,
        }
    if arg.startswith("ch__"):
        return {
            "action": "channel_topic",
            "target": arg[len("ch__") :],
            "post_token": "",
            "channel_message_id": None,
        }
    if arg.startswith("ch_"):
        # 兼容旧单下划线 topic 深链，例如 ch_bkk1
        return {
            "action": "channel_topic",
            "target": arg[len("ch_") :],
            "post_token": "",
            "channel_message_id": None,
        }
    # 讨论区入口深链：discussion_entry__<post_token>__<listing_id>
    if arg.startswith("discussion_entry__"):
        parts = arg.split("__", 2)
        if len(parts) == 3:
            post_token, listing_id = parts[1], parts[2]
            return {
                "action": "discussion_entry",
                "target": listing_id,
                "post_token": post_token,
                "channel_message_id": _base36_decode(post_token),
            }
        return {
            "action": "discussion_entry",
            "target": "",
            "post_token": "",
            "channel_message_id": None,
        }
    if re.match(r"^l_\d+$", arg):
        # 兼容裸 listing 深链，默认走咨询入口。
        return {
            "action": "consult",
            "target": arg,
            "post_token": "",
            "channel_message_id": None,
        }
    for alias, action in START_ACTION_ALIASES.items():
        prefix = f"{alias}__"
        if arg.startswith(prefix):
            parts = arg.split("__", 2)
            if len(parts) == 3:
                post_token, target = parts[1], parts[2]
                return {
                    "action": action,
                    "target": target,
                    "post_token": post_token,
                    "channel_message_id": _base36_decode(post_token),
                }
        legacy_prefix = f"{alias}_"
        if arg.startswith(legacy_prefix):
            return {
                "action": action,
                "target": arg[len(legacy_prefix) :],
                "post_token": "",
                "channel_message_id": None,
            }
    for action in START_ACTIONS:
        prefix = f"{action}__"
        if arg.startswith(prefix):
            parts = arg.split("__", 2)
            if len(parts) == 3:
                post_token, target = parts[1], parts[2]
                return {
                    "action": action,
                    "target": target,
                    "post_token": post_token,
                    "channel_message_id": _base36_decode(post_token),
                }
        legacy_prefix = f"{action}_"
        if arg.startswith(legacy_prefix):
            # 兼容旧版短链 consult_/appoint_/fav_/more_，生产发布侧仍可能出现。
            return {
                "action": action,
                "target": arg[len(legacy_prefix) :],
                "post_token": "",
                "channel_message_id": None,
            }
    return None


def build_source_label(post_token: str) -> str:
    return f"channel_post:{post_token}" if post_token else "channel_deeplink"


def _deep_link(payload: str) -> str:
    return f"https://t.me/{USER_BOT_USERNAME}?start={quote(payload)}"


def _build_start_payload(action: str, target: str, **meta: str) -> str:
    action_code = START_ACTION_CODES.get(action, action)
    payload = str(target or "").strip()
    meta_parts = [f"{key}={value}" for key, value in meta.items() if str(value or "").strip()]
    if meta_parts:
        payload = "|".join([payload, *meta_parts])
    return f"{action_code}_{payload}"


def _extract_caption_variant(review_note: str | None) -> str:
    m = re.search(r"caption_variant:(a|b|c)", str(review_note or ""), flags=re.IGNORECASE)
    return m.group(1).lower() if m else "a"


def _normalize_variant(raw: str | None) -> str:
    v = str(raw or "").strip().lower()
    return v if v in {"a", "b", "c"} else ""


def _split_target_meta(raw_target: str | None) -> tuple[str, dict[str, str]]:
    raw = str(raw_target or "").strip()
    if not raw:
        return "", {}
    parts = [p.strip() for p in raw.split("|") if p.strip()]
    if not parts:
        return "", {}
    target = parts[0]
    meta: dict[str, str] = {}
    for item in parts[1:]:
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        key = key.strip().lower()
        value = value.strip()
        if key:
            meta[key] = value
    return target, meta


def _latest_draft_context(listing_id: str) -> dict:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """SELECT listing_id, title, project, area, layout, property_type, price, floor, size, review_note
                   FROM drafts
                   WHERE listing_id=?
                   ORDER BY id DESC
                   LIMIT 1""",
                (listing_id,),
            ).fetchone()
            return dict(row) if row else {}
    except Exception:
        logger.debug("读取 drafts 上下文失败: %s", listing_id, exc_info=True)
        return {}


def _latest_draft_review_status(listing_id: str) -> str:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT review_status
                FROM drafts
                WHERE listing_id=?
                ORDER BY id DESC
                LIMIT 1
                """,
                (listing_id,),
            ).fetchone()
            return str((dict(row) if row else {}).get("review_status") or "").strip().lower()
    except Exception:
        logger.debug("读取 drafts 状态失败: %s", listing_id, exc_info=True)
        return ""


def listing_context(listing_id: str) -> dict:
    listing_id = str(listing_id or "").strip()
    if not listing_id:
        return {}
    merged: dict = {}
    try:
        listing = db.get_listing(listing_id)
        if listing:
            merged.update(dict(listing))
    except Exception:
        logger.debug("用户 Bot 读取 listings 失败: %s", listing_id, exc_info=True)

    draft_ctx = _latest_draft_context(listing_id)
    if draft_ctx:
        for key in ("listing_id", "area", "layout", "property_type", "price", "floor", "size", "title", "project"):
            if key not in merged or merged.get(key) in (None, "", 0, "0"):
                merged[key] = draft_ctx.get(key, merged.get(key))
        merged["caption_variant"] = _extract_caption_variant(draft_ctx.get("review_note"))
    if "caption_variant" not in merged:
        merged["caption_variant"] = "a"
    if not merged:
        return {"listing_id": listing_id, "caption_variant": "a"}
    merged.setdefault("listing_id", listing_id)
    return merged


def listing_is_available(listing_id: str) -> tuple[bool, str]:
    listing_id = str(listing_id or "").strip()
    if not listing_id:
        return False, "missing"
    listing = db.get_listing(listing_id)
    if listing:
        status = str(listing.get("status") or "active").strip().lower()
        return status == "active", status or "inactive"
    draft_status = _latest_draft_review_status(listing_id)
    if draft_status in {"ready", "published"}:
        return True, draft_status
    if draft_status:
        return False, draft_status
    return False, "missing"


def listing_unavailable_text() -> str:
    return (
        "这套刚好已经租出去了 🏃\n"
        "不过别急，我们还有类似区域和价位的房源，告诉我你的预算，马上帮你找 ↓"
    )


def listing_unavailable_keyboard(listing_id: str = "") -> InlineKeyboardMarkup:
    area = str(listing_context(listing_id).get("area") or "").strip()
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton("🔍 继续找房", callback_data="findmode:guided")],
        [InlineKeyboardButton("💬 联系顾问", callback_data="appointment_menu:contact")],
    ]
    if area and area != "不限":
        rows.append([InlineKeyboardButton("🏠 同区推荐", callback_data=f"unavail:more:{area}")])
    rows.append([InlineKeyboardButton("🏠 返回首页", callback_data="home")])
    return InlineKeyboardMarkup(rows)


def _store_active_entry(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    arg: str,
    action: str,
    listing_id: str = "",
    touch_payload: dict | None = None,
) -> None:
    context.user_data["active_entry"] = {
        "arg": arg,
        "action": action,
        "listing_id": str(listing_id or "").strip(),
        "touch_payload": dict(touch_payload or {}),
        "saved_at": now_ts(),
    }


def _active_entry_resume_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("▶️ 继续当前流程", callback_data="resume:continue")],
            [InlineKeyboardButton("🔄 重新开始本次入口", callback_data="resume:restart")],
            [InlineKeyboardButton("🏠 返回首页", callback_data="home")],
        ]
    )


def channel_topic_welcome_text(topic: str) -> str:
    topic = str(topic or "").strip().lower()
    topic_map = {
        "district_guide": (
            "📍 已收到区域导流。\n\n"
            "想看这个区域的实拍房源，直接发预算和户型偏好，或点下方按钮开始找房。"
        ),
        "service": (
            "🧰 已进入侨联服务入口。\n\n"
            "想咨询代看、合同、入住协助或租后问题，直接点按钮就能接上顾问。"
        ),
        "video_tour": (
            "🎥 已进入视频代看入口。\n\n"
            "先发你要找的区域、预算、户型，我先推两套，再接顾问视频代看。"
        ),
    }
    return topic_map.get(
        topic,
        "已收到频道入口。\n\n告诉我你的需求，我马上帮你接上顾问或开始找房。",
    )


def _resolve_area_from_target(target: str) -> tuple[str, str]:
    raw_target = str(target or "").strip()
    if not raw_target:
        return "", ""
    listing_id = raw_target if raw_target.startswith("l_") else ""
    area = detect_area(raw_target)
    if area == raw_target[:40]:
        area = ""
    if listing_id:
        area = str(listing_context(listing_id).get("area") or area).strip()
    return area, listing_id


def _daily_listing_line(item: dict) -> str:
    area = str(item.get("area") or "金边").strip() or "金边"
    layout = str(item.get("layout") or item.get("property_type") or "房源").strip() or "房源"
    return f"{he(area)}｜{he(layout)}｜{he(_fmt_price(item.get('price')))}"


def _latest_listing_text(limit: int = 5) -> str:
    matches = db.list_recent_listings(limit)
    if not matches:
        return "🏠 <b>今日可看房源更新</b>\n\n暂时还没有可展示的最新房源，你可以先发区域和预算，我马上筛一轮。"
    lines = [
        "🏠 <b>今日可看房源更新</b>",
        "",
        "今日新增：",
        "",
    ]
    for item in matches[:limit]:
        lines.append(_daily_listing_line(item))
    lines.extend(
        [
            "",
            "房源变动很快，",
            "看到合适的建议先咨询是否还在。",
            "",
            "侨联可以先帮您确认：",
            "• 是否可入住",
            "• 押金怎么收",
            "• 费用怎么算",
            "• 能不能视频代看",
        ]
    )
    return "\n".join(lines)


def _resolve_video_pref_snapshot(context: ContextTypes.DEFAULT_TYPE) -> dict[str, object]:
    def _int_or_none(v: object) -> int | None:
        if v in (None, ""):
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    snap = context.user_data.get("video_pref")
    snap = dict(snap) if isinstance(snap, dict) else {}

    area = str(snap.get("area") or "").strip()
    budget_min = _int_or_none(snap.get("budget_min"))
    budget_max = _int_or_none(snap.get("budget_max"))
    layout = str(snap.get("layout") or "").strip()

    pref = context.user_data.get("search_pref")
    if isinstance(pref, dict):
        if not area:
            area = str(pref.get("area") or "").strip()
        if not layout:
            goal = str(pref.get("goal") or "").strip()
            if goal and goal not in {"any", "住宅"}:
                layout = goal

    listing_id = str(context.user_data.get("contact_listing_id") or "").strip()
    if listing_id:
        info = listing_context(listing_id)
        if not area:
            area = str(info.get("area") or "").strip()
        if not layout:
            layout = str(info.get("layout") or info.get("property_type") or "").strip()

    area_display = area or "未填写"
    budget_display = _budget_text(budget_min, budget_max)
    if budget_display == "-":
        budget_display = "未填写"
    layout_display = layout or "未填写"

    return {
        "area": area,
        "area_display": area_display,
        "budget_min": budget_min,
        "budget_max": budget_max,
        "budget_display": budget_display,
        "layout": layout,
        "layout_display": layout_display,
    }


def _video_tour_intro_text(*, area: str, budget: str, layout: str) -> str:
    return (
        "🎥 可以，侨联可以先帮您视频代看。\n\n"
        "适合这些情况：\n\n"
        "✔ 人还没到金边\n"
        "✔ 没时间一套套跑\n"
        "✔ 想先确认房子真实情况\n"
        "✔ 想看周边环境\n"
        "✔ 想提前看家具家电状态\n"
        "稍等，小助手正在为你从侨联房源库中检索...\n\n"
        "按你的需求：\n"
        f"区域：{he(area)}\n"
        f"预算：{he(budget)}\n"
        f"户型：{he(layout)}"
    )


def _video_tour_match_text(matches: list[dict], *, match_mode: str = "strict") -> str:
    if not matches:
        return "暂时没有完全匹配的在架房源，我先把你接给顾问优先人工匹配。"
    lines = ["已为你先匹配 2 套：", ""]
    for idx, item in enumerate(matches[:2], start=1):
        area = str(item.get("area") or "金边").strip() or "金边"
        layout = str(item.get("layout") or item.get("property_type") or "房源").strip() or "房源"
        listing_id = str(item.get("listing_id") or "-").strip() or "-"
        lines.append(f"{idx}. {he(area)}｜{he(layout)}｜{he(_fmt_price(item.get('price')))}")
        lines.append(f"房源编号：<code>{he(listing_id)}</code>")
    if match_mode in {"no_type", "no_area", "budget_only", "fuzzy", "fallback_recent"}:
        lines.append("\n已自动放宽条件先给你匹配，顾问会继续人工精筛。")
    return "\n".join(lines)


def _video_match_keyboard(matches: list[dict]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for item in matches[:2]:
        listing_id = str(item.get("listing_id") or "").strip()
        if listing_id:
            rows.append([InlineKeyboardButton(f"💬 咨询 {listing_id}", callback_data=f"appointment_menu:contact:listing:{listing_id}")])
    rows.append([InlineKeyboardButton("📅 安排视频代看", callback_data="appointment_menu:video")])
    rows.append([InlineKeyboardButton("🏠 查看更多房源", callback_data="hub:latest")])
    return InlineKeyboardMarkup(rows)


async def start_video_tour_flow(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    source: str,
    area: str = "",
    budget_min: int | None = None,
    budget_max: int | None = None,
    layout: str = "",
) -> int:
    _remember_video_pref(
        context,
        area=area or None,
        budget_min=budget_min,
        budget_max=budget_max,
        layout=layout or None,
    )
    pref = _resolve_video_pref_snapshot(context)
    area_value = str(pref.get("area") or "")
    budget_lo = pref.get("budget_min")
    budget_hi = pref.get("budget_max")
    layout_value = str(pref.get("layout") or "")

    create_lead(
        update.effective_user,
        action="video_tour_click",
        source=source,
        area=area_value if area_value and area_value != "不限" else "",
        budget_min=budget_lo if isinstance(budget_lo, int) else None,
        budget_max=budget_hi if isinstance(budget_hi, int) else None,
        payload={
            "preferred_mode": "video",
            "layout": layout_value,
            "area": area_value,
        },
    )

    notify_key = f"video_tour_click:{source}"
    if _allow_admin_notify(context, key=notify_key, cooldown_seconds=120):
        await _notify_admins(
            context,
            title="视频代看入口点击",
            lines=[
                f"用户：{_user_mention_html(update.effective_user)}",
                f"联系方式：{he(_user_contact_text(update.effective_user))}",
                f"来源：{he(source)}",
                f"区域：{he(str(pref.get('area_display') or '未填写'))}",
                f"预算：{he(str(pref.get('budget_display') or '未填写'))}",
                f"户型：{he(str(pref.get('layout_display') or '未填写'))}",
            ],
        )

    await render_panel(
        update,
        text=_video_tour_intro_text(
            area=str(pref.get("area_display") or "未填写"),
            budget=str(pref.get("budget_display") or "未填写"),
            layout=str(pref.get("layout_display") or "未填写"),
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard(),
        context=context,
    )

    has_condition = bool(area_value or layout_value or isinstance(budget_lo, int) or isinstance(budget_hi, int))
    if not has_condition:
        await update.effective_message.reply_text(
            "请先发找房条件，我再推 2 套：例如 <code>BKK1 500-800 一房</code>。",
            parse_mode=ParseMode.HTML,
            reply_markup=keyword_followup_keyboard(),
        )
        return MAIN

    property_type = detect_property_type(layout_value)
    matches, match_mode = search_listings_with_fallback(
        property_type=property_type or None,
        area=area_value if area_value and area_value != "不限" else "",
        budget_min=budget_lo if isinstance(budget_lo, int) else None,
        budget_max=budget_hi if isinstance(budget_hi, int) else None,
        text_fragment=f"{area_value} {layout_value} {pref.get('budget_display')}",
        limit=2,
    )

    await update.effective_message.reply_text(
        _video_tour_match_text(matches, match_mode=match_mode),
        parse_mode=ParseMode.HTML,
        reply_markup=_video_match_keyboard(matches),
    )
    return MAIN

def _keyword_intro_text(*, area: str = "", room_type: str = "", budget_min: int | None = None, budget_max: int | None = None) -> str:
    parts: list[str] = []
    if area:
        parts.append(f"区域：<b>{he(area)}</b>")
    if room_type:
        parts.append(f"户型：<b>{he(room_type)}</b>")
    if budget_min is not None or budget_max is not None:
        parts.append(f"预算：<b>{he(_budget_text(budget_min, budget_max))}</b>")
    if not parts:
        return "我先按您刚才的需求找一轮，您也可以继续补区域、预算或户型。"
    return "已按您的需求接上：" + " ｜ ".join(parts)


def listing_landing_text(listing_id: str) -> str:
    item = listing_context(listing_id)
    area = item.get("area") or "金边"
    layout = item.get("layout") or item.get("property_type") or "房源"
    variant = str(item.get("caption_variant") or "a").lower()
    price = item.get("price")
    price_text = _fmt_price(price)
    details = []
    if item.get("floor"):
        details.append(str(item["floor"]))
    if item.get("size"):
        size_text = str(item["size"]).strip()
        if size_text and size_text.replace(".", "", 1).isdigit():
            size_text = f"{size_text}㎡"
        details.append(size_text)
    detail_line = f"\n📌 {' · '.join(details)}" if details else ""
    if variant == "b":
        # 对应频道 B 款（实拍决策型）：强调快速行动
        next_step = "实拍帖里所见即入住所得。如果看着合适，直接约视频代看或实地看房是最快的路。"
    elif variant == "c":
        # 对应频道 C 款（费用对齐型）：强调透明成本 + 顾问核实
        next_step = "押付、水电、物业等费用顾问都可以帮您逐项核实，点下方「咨询顾问」最直接。"
    else:
        # 对应频道 A 款（信息决策型）：下一步按钮最短路径
        next_step = "这套房我先帮您接住了，下面的按钮直接就是下一步，不用重新解释是哪套。"
    return (
        f"🏠 <b>{he(area)}｜{he(layout)}</b>\n"
        f"💰 {he(price_text)}"
        f"{he(detail_line)}\n\n"
        f"{he(next_step)}"
    )


def listing_landing_keyboard(listing_id: str, area: str = "") -> InlineKeyboardMarkup:
    area_payload = area or listing_context(listing_id).get("area") or ""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📅 预约实地看房",
                    url=_deep_link(_build_start_payload("appoint", listing_id, mode="offline")),
                ),
                InlineKeyboardButton(
                    "🎥 视频代看",
                    url=_deep_link(_build_start_payload("appoint", listing_id, mode="video")),
                ),
            ],
            [
                InlineKeyboardButton("❤️ 先收藏这套", url=_deep_link(_build_start_payload("fav", listing_id))),
                InlineKeyboardButton("🏠 看同区域更多", url=_deep_link(_build_start_payload("more", listing_id or area_payload))),
            ],
            [InlineKeyboardButton("💬 联系顾问", callback_data=f"appointment_menu:contact:listing:{listing_id}")],
        ]
    )


def parse_budget_range(text: str) -> tuple[int | None, int | None]:
    values = [int(x) for x in re.findall(r"\d{2,5}", text or "")]
    if not values:
        return None, None
    if len(values) == 1:
        value = values[0]
        return max(0, value - 200), value + 200
    low, high = min(values[0], values[1]), max(values[0], values[1])
    return low, high


def detect_area(text: str) -> str:
    raw = (text or "").strip()
    lowered = raw.lower()
    aliases = {
        "bkk1": "BKK1",
        "bkk2": "BKK2",
        "bkk3": "BKK3",
        "sen sok": "森速",
        "sensok": "森速",
        "森速": "森速",
        "富力城": "富力城",
        "炳发城": "炳发城",
        "太子": "太子/幸福",
        "幸福": "太子/幸福",
        "太子/幸福": "太子/幸福",
        "洪森大道": "洪森大道",
        "桑园": "桑园",
        "万景岗": "万景岗",
        "toul kork": "Toul Kork",
        "tk": "Toul Kork",
        "俄罗斯市场": "俄罗斯市场",
        "钻石岛": "钻石岛",
        "金边市区": "金边市区",
        "不限": "不限",
    }
    for key, value in aliases.items():
        if key in lowered or key in raw:
            return value
    return raw[:40]


def detect_room_type(text: str) -> str:
    lowered = (text or "").lower()
    for label, variants in ROOM_TYPE_HINTS.items():
        if any(v in lowered for v in variants):
            return label
    return ""


def detect_property_type(text: str) -> str:
    """仅提取可用于 listings.property_type 精确过滤的类型。"""
    lowered = (text or "").lower()
    mapping = (
        ("别墅", ("别墅", "villa")),
        ("排屋", ("排屋", "townhouse")),
        ("商铺", ("商铺", "店铺", "shophouse", "shop")),
        ("办公室", ("办公室", "office")),
        ("公寓", ("公寓", "apartment", "studio")),
        ("住宅", ("住宅",)),
    )
    for canonical, keys in mapping:
        if any(k in lowered for k in keys):
            return canonical
    return ""


def search_listings_with_fallback(
    *,
    property_type: str | None,
    area: str | None,
    budget_min: int | None,
    budget_max: int | None,
    text_fragment: str = "",
    limit: int = 3,
) -> tuple[list[dict], str]:
    """分层放宽条件，避免“有房但全空结果”。

    返回：(matches, mode)
    mode: strict / no_type / no_area / budget_only / fuzzy / fallback_recent
    """
    area_arg = [area] if area and area != "不限" else None

    matches = db.search_listings(
        property_type=property_type or None,
        areas=area_arg,
        budget_min=budget_min,
        budget_max=budget_max,
        limit=limit,
    )
    if matches:
        return matches, "strict"

    if property_type:
        matches = db.search_listings(
            areas=area_arg,
            budget_min=budget_min,
            budget_max=budget_max,
            limit=limit,
        )
        if matches:
            return matches, "no_type"

    if area and area != "不限":
        matches = db.search_listings(
            property_type=property_type or None,
            budget_min=budget_min,
            budget_max=budget_max,
            limit=limit,
        )
        if matches:
            return matches, "no_area"

    matches = db.search_listings(
        budget_min=budget_min,
        budget_max=budget_max,
        limit=limit,
    )
    if matches:
        return matches, "budget_only"

    frag = (text_fragment or "").strip()
    if frag:
        matches = db.search_listings(ilike_fragment=frag, limit=limit)
        if matches:
            return matches, "fuzzy"

    return db.list_recent_listings(limit), "fallback_recent"


def upsert_user_profile(user) -> None:
    db.upsert_user(
        user.id,
        getattr(user, "username", "") or "",
        getattr(user, "first_name", "") or "",
        getattr(user, "last_name", "") or "",
        now_ts(),
    )


def create_lead(
    user,
    *,
    action: str,
    source: str,
    listing_id: str = "",
    area: str = "",
    property_type: str = "",
    budget_min: int | None = None,
    budget_max: int | None = None,
    payload: dict | None = None,
) -> None:
    lead_payload = payload or {}
    raw_message_id = lead_payload.get("channel_message_id", lead_payload.get("message_id"))
    try:
        message_id = int(raw_message_id) if raw_message_id not in (None, "", 0) else None
    except (TypeError, ValueError):
        message_id = None
    caption_variant = _normalize_variant(lead_payload.get("caption_variant")) or ""
    post_token = str(lead_payload.get("post_token") or "").strip()
    agent_id = str(lead_payload.get("agent_id") or "").strip()
    response_at = str(lead_payload.get("response_at") or "").strip()
    try:
        db.create_lead(
            {
                "user_id": user.id,
                "username": getattr(user, "username", "") or "",
                "display_name": user_display_name(user),
                "source": source,
                "action": action,
                "listing_id": listing_id,
                "area": area,
                "property_type": property_type,
                "budget_min": budget_min,
                "budget_max": budget_max,
                "payload": lead_payload,
                "message_id": message_id,
                "post_token": post_token,
                "caption_variant": caption_variant,
                "agent_id": agent_id,
                "response_at": response_at,
                "created_at": now_ts(),
            }
        )
    except Exception:
        logger.exception("写入 leads 失败: action=%s listing=%s", action, listing_id)


def _user_mention_html(user) -> str:
    name = he(user_display_name(user) or str(getattr(user, "id", "")))
    uid = int(getattr(user, "id", 0) or 0)
    if uid > 0:
        return f'<a href="tg://user?id={uid}">{name}</a>'
    return name


def _user_contact_text(user) -> str:
    username = (getattr(user, "username", "") or "").strip()
    if username:
        return f"@{username}"
    uid = int(getattr(user, "id", 0) or 0)
    return f"tg://user?id={uid}" if uid > 0 else "-"


def _is_admin_user(user_id: int | None) -> bool:
    try:
        return int(user_id or 0) in set(ADMIN_IDS or [])
    except (TypeError, ValueError):
        return False


def _budget_text(budget_min: int | None, budget_max: int | None) -> str:
    if budget_min is not None and budget_max is not None:
        return f"{budget_min}-{budget_max} USD/月"
    if budget_min is not None:
        return f">= {budget_min} USD/月"
    if budget_max is not None:
        return f"<= {budget_max} USD/月"
    return "-"


def _parse_date_safe(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _binding_end_date(binding: dict | None) -> str:
    if not binding:
        return ""
    return str(binding.get("contract_end_date") or binding.get("lease_end_date") or "").strip()


def _binding_days_left(binding: dict | None) -> int | None:
    dt = _parse_date_safe(_binding_end_date(binding))
    if dt is None:
        return None
    return max((dt.date() - datetime.now().date()).days, 0)


def _contract_status_text(days_left: int | None) -> str:
    if days_left is None:
        return "资料待补全"
    if days_left <= 3:
        return "临近到期，请优先跟进"
    if days_left <= 7:
        return "本周内建议确认续租/换房"
    if days_left <= 30:
        return "本月内可提前安排"
    return "租约状态稳定"


def _lease_reminder_label(user_id: int | None) -> str:
    enabled = True if user_id is None else db.is_lease_reminder_enabled(user_id)
    return "🔔 到期提醒：已开启" if enabled else "🔕 到期提醒：已关闭"


def _binding_contract_text(binding: dict | None, user_id: int | None = None) -> str:
    if not binding:
        return (
            "📋 <b>我的租约</b>\n\n"
            "当前还没有绑定租约档案。\n"
            "请点「💬 联系顾问」，我们会后台录入房号/交租日/到期日。"
        )
    property_name = str(binding.get("property_name") or "-")
    rent_day = binding.get("rent_day")
    rent_text = f"每月 {int(rent_day)} 号" if isinstance(rent_day, int) else "待确认"
    end_date = _binding_end_date(binding) or "待确认"
    days_left = _binding_days_left(binding)
    day_line = f"{days_left} 天" if days_left is not None else "待确认"
    monthly_rent = binding.get("monthly_rent")
    try:
        rent_value = float(monthly_rent or 0)
    except (TypeError, ValueError):
        rent_value = 0
    rent_line = f"${int(rent_value)}/月" if rent_value > 0 else "待确认"
    deposit_months = binding.get("deposit_months")
    try:
        deposit_line = f"{int(deposit_months)} 个月" if int(deposit_months) > 0 else "待确认"
    except (TypeError, ValueError):
        deposit_line = "待确认"
    reminder_line = _lease_reminder_label(user_id)
    status_line = _contract_status_text(days_left)
    return (
        "📋 <b>我的租约</b>\n\n"
        f"🏠 房号/项目：{he(property_name)}\n"
        f"💰 月租：{he(rent_line)}\n"
        f"🔐 押金：{he(deposit_line)}\n"
        f"📅 交租日：{he(rent_text)}\n"
        f"⏳ 合同到期：{he(end_date)}\n"
        f"🕒 剩余：<b>{he(day_line)}</b>\n"
        f"🧭 状态：<b>{he(status_line)}</b>\n"
        f"{he(reminder_line)}"
    )


def _contract_actions_keyboard(user_id: int | None = None) -> InlineKeyboardMarkup:
    reminder_label = _lease_reminder_label(user_id)
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton("🔄 续租咨询", callback_data="contract:renew"),
            InlineKeyboardButton("🏠 我要换房", callback_data="contract:change"),
        ],
        [
            InlineKeyboardButton("📅 我的预约", callback_data="appointment_menu:list"),
            InlineKeyboardButton("⚡ 入住服务", callback_data="service:hub"),
        ],
        [
            InlineKeyboardButton(reminder_label, callback_data="contract:toggle_reminder"),
            InlineKeyboardButton("💬 联系顾问", callback_data="appointment_menu:contact"),
        ],
        [InlineKeyboardButton("🏠 返回首页", callback_data="home")],
    ]
    return InlineKeyboardMarkup(rows)


def _format_match_line(item: dict) -> str:
    head = (
        f"• <b>{he(str(item.get('listing_id', '') or '-'))}</b> | "
        f"{he(str(item.get('area', '') or '金边'))} | "
        f"{he(_fmt_price(item.get('price')))}"
    )
    detail_parts = []
    if item.get("layout"):
        detail_parts.append(str(item.get("layout")))
    if item.get("size_sqm"):
        detail_parts.append(f"{item.get('size_sqm')}㎡")
    detail = f"\n  {he(' · '.join(detail_parts))}" if detail_parts else ""
    reminder = ""
    if item.get("drawbacks"):
        reminder = f"\n  ⚠️ {he(str(item.get('drawbacks'))[:60])}"
    return f"{head}{detail}{reminder}"


def _format_listing_choice_lines(matches: list[dict]) -> str:
    if not matches:
        return ""
    lines = [listing_match_intro_text()]
    for item in matches[:3]:
        lines.append(_format_match_line(item))
    lines.append(listing_match_footer_text())
    return "\n".join(lines)


async def _notify_admins(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    title: str,
    lines: list[str],
) -> None:
    if not ADMIN_IDS:
        return
    body = "\n".join([line for line in lines if str(line or "").strip()])
    text = f"🔔 <b>{he(title)}</b>\n\n{body}".strip()
    for admin_id in sorted(ADMIN_IDS):
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except Exception:
            logger.exception("发送管理号消息失败: admin_id=%s title=%s", admin_id, title)


def _allow_admin_notify(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    key: str,
    cooldown_seconds: int = 180,
) -> bool:
    """简单节流：避免同一用户短时间重复点击导致管理号刷屏。"""
    box = context.user_data.setdefault("_notify_throttle", {})
    if not isinstance(box, dict):
        box = {}
    now_ts = datetime.now().timestamp()
    last_ts = float(box.get(key) or 0)
    if now_ts - last_ts < max(1, int(cooldown_seconds)):
        return False
    box[key] = now_ts
    context.user_data["_notify_throttle"] = box
    return True


def old_tenant_binding_text(user_id: int) -> tuple[str, dict | None]:
    binding = db.get_active_binding(user_id)
    if not binding:
        return (
            "✅ 已登记老客回流。\n\n"
            "当前还没有绑定到您的租住档案。\n"
            "请点下方「联系顾问」，我们会用后台资料完成绑定。",
            None,
        )
    return ("✅ 已识别侨联老用户档案\n\n" + _binding_contract_text(binding), binding)


def list_recent_appointments(user_id: int) -> str:
    rows = db.list_appointments(user_id, limit=5)
    if not rows:
        return (
            "📅 暂无预约记录。\n\n"
            "看中频道某套房时，直接点帖里的“预约”最快；\n"
            "如果还没定房号，也可以点菜单里的“预约看房”先把时间排起来。"
        )
    parts = ["📅 我的预约"]
    for row in rows:
        mode = APPOINTMENT_MODE_LABELS.get(str(row.get("viewing_mode") or ""), str(row.get("viewing_mode") or "-"))
        time_label = APPOINTMENT_TIME_LABELS.get(str(row.get("appointment_time") or ""), str(row.get("appointment_time") or "-"))
        status = APPOINTMENT_STATUS_LABELS.get(str(row.get("status") or ""), str(row.get("status") or "待确认"))
        parts.append(
            f"• {row.get('listing_id', '未填写')} | {row.get('appointment_date', '')} | {time_label} | {mode} | {status}"
        )
    parts.append("\n如需改时间，请点「💬 联系顾问」由管理号协助处理。")
    return "\n".join(parts)


def list_favorites_text(user_id: int) -> str:
    rows = db.list_favorites(user_id)
    if not rows:
        return (
            "⭐ 暂无收藏房源。\n\n"
            "在频道里点“收藏房源”后，这里会保留清单，方便您回头对比。"
        )
    parts = ["⭐ 您收藏过的房源："]
    for item in rows[:8]:
        detail = []
        if item.get("layout"):
            detail.append(str(item.get("layout")))
        if item.get("size_sqm"):
            detail.append(f"{item.get('size_sqm')}㎡")
        detail_text = f" | {' · '.join(detail)}" if detail else ""
        parts.append(
            f"• {item.get('listing_id', '-')} | {item.get('area', '金边')} | {_fmt_price(item.get('price'))}{detail_text}"
        )
    parts.append("\n需要从收藏里优先挑选，点「💬 联系顾问」即可。")
    return "\n".join(parts)


async def route_start_arg(update: Update, context: ContextTypes.DEFAULT_TYPE, arg: str) -> int | None:
    user = update.effective_user
    message = update.effective_message
    payload = parse_start_arg_payload(arg)
    if payload is None:
        return None

    action = payload["action"]
    raw_target = payload["target"]
    target, target_meta = _split_target_meta(raw_target)
    if not target:
        target = raw_target
    post_token = payload.get("post_token", "")
    channel_message_id = payload.get("channel_message_id")
    source = build_source_label(post_token)
    touch_payload = {
        "start_arg": arg,
        "post_token": post_token,
        "channel_message_id": channel_message_id,
        "first_touch_action": action,
    }
    if target_meta:
        touch_payload["target_meta"] = target_meta

    if action == "appoint":
        listing_id = target
        listing_info = listing_context(listing_id)
        caption_variant = (
            _normalize_variant(target_meta.get("cv"))
            or _normalize_variant(listing_info.get("caption_variant"))
            or "a"
        )
        touch_payload["caption_variant"] = caption_variant
        entry_source = str(target_meta.get("entry") or target_meta.get("src") or "").strip().lower()
        if entry_source:
            touch_payload["entry"] = entry_source
            touch_payload["entry_step"] = str(target_meta.get("step") or "").strip()
        context.user_data["contact_listing_id"] = listing_id
        context.user_data["contact_touch_payload"] = {**touch_payload, "caption_variant": caption_variant}
        _store_active_entry(
            context,
            arg=arg,
            action=action,
            listing_id=listing_id,
            touch_payload={**touch_payload, "caption_variant": caption_variant},
        )
        is_available, availability_reason = listing_is_available(listing_id)
        if not is_available:
            if availability_reason == "missing":
                create_lead(
                    user,
                    action="broken_link",
                    source=source,
                    listing_id=listing_id,
                    payload={**touch_payload, "reason": availability_reason},
                )
            await message.reply_text(
                listing_unavailable_text(),
                reply_markup=listing_unavailable_keyboard(listing_id),
            )
            return MAIN
        initial_mode = str(target_meta.get("mode") or "").strip().lower()
        create_lead(
            user,
            action="appointment_click",
            source=source,
            listing_id=listing_id,
            payload={**touch_payload, "preferred_mode": initial_mode, "caption_variant": caption_variant},
        )
        if entry_source == "discussion" and _allow_admin_notify(
            context,
            key=f"discussion_appoint:{listing_id}:{post_token}:{int(user.id)}",
            cooldown_seconds=180,
        ):
            await _notify_admins(
                context,
                title="讨论区预约点击（首段）",
                lines=[
                    f"用户：{_user_mention_html(user)}",
                    f"联系方式：{he(_user_contact_text(user))}",
                    f"房源：{he(listing_id or '-')}",
                    f"来源：{he(source)}",
                    f"post_token：{he(post_token or '-')}",
                    f"预约方式：{he(initial_mode or '未选择')}",
                ],
            )
        return await start_appointment(
            update,
            context,
            listing_id,
            source=source,
            touch_payload={**touch_payload, "entry": entry_source or ""},
            initial_mode=initial_mode,
        )

    if action == "consult":
        listing_id = target
        is_available, availability_reason = listing_is_available(listing_id)
        if not is_available:
            if availability_reason == "missing":
                create_lead(
                    user,
                    action="broken_link",
                    source=source,
                    listing_id=listing_id,
                    payload={**touch_payload, "reason": availability_reason},
                )
            await message.reply_text(
                listing_unavailable_text(),
                reply_markup=listing_unavailable_keyboard(listing_id),
            )
            return MAIN
        listing_info = listing_context(listing_id)
        caption_variant = (
            _normalize_variant(target_meta.get("cv"))
            or str(listing_info.get("caption_variant") or "a").lower()
        )
        context.user_data["contact_listing_id"] = listing_id
        context.user_data["contact_touch_payload"] = {**touch_payload, "caption_variant": caption_variant}
        _store_active_entry(
            context,
            arg=arg,
            action=action,
            listing_id=listing_id,
            touch_payload={**touch_payload, "caption_variant": caption_variant},
        )
        create_lead(
            user,
            action="consult_click",
            source=source,
            listing_id=listing_id,
            payload={**touch_payload, "caption_variant": caption_variant},
        )
        await _notify_admins(
            context,
            title="咨询点击（频道深链）",
            lines=[
                f"用户：{_user_mention_html(user)}",
                f"联系方式：{he(_user_contact_text(user))}",
                f"房源：{he(listing_id or '-')}",
                f"来源：{he(source)}",
            ],
        )
        await message.reply_text(
            listing_landing_text(listing_id),
            parse_mode=ParseMode.HTML,
            reply_markup=listing_landing_keyboard(listing_id),
        )
        return MAIN

    if action == "index_area":
        context.user_data["search_pref"] = {"source": "channel_index", "goal": "any", "touch_payload": touch_payload}
        await message.reply_text(
            "📍 <b>按区域找房</b>\n\n请选择区域：",
            parse_mode=ParseMode.HTML,
            reply_markup=find_area_keyboard(),
        )
        return FIND_AREA

    if action == "index_budget":
        context.user_data["search_pref"] = {"source": "channel_index", "goal": "any", "touch_payload": touch_payload}
        await message.reply_text(
            "💰 <b>按预算找房</b>\n\n请选择预算区间：",
            parse_mode=ParseMode.HTML,
            reply_markup=find_budget_keyboard("any"),
        )
        return FIND_BUDGET

    if action == "index_layout":
        await message.reply_text(
            "🛏 <b>按户型找房</b>\n\n请选择户型：",
            parse_mode=ParseMode.HTML,
            reply_markup=room_type_keyboard(),
        )
        return MAIN

    if action == "index_latest":
        await message.reply_text(
            _latest_listing_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=latest_listing_keyboard(),
        )
        return MAIN

    if action == "index_video":
        return await start_video_tour_flow(
            update,
            context,
            source="channel_index",
        )

    if action == "index_advisor":
        return await contact_management(update, context, source="channel_index")

    if action == "index_service":
        await message.reply_text(
            service_hub_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=service_hub_keyboard(),
        )
        return MAIN

    if action == "tenant_bind":
        binding_code = target
        binding = db.bind_by_code(user.id, binding_code)
        if not binding:
            create_lead(
                user,
                action="tenant_bind_invalid",
                source="tenant_bind",
                payload={"binding_code": binding_code},
            )
            await message.reply_text(
                "这个绑定链接已失效或已使用过。\n请联系顾问重新获取绑定码，或直接发消息给我们 ↓",
                reply_markup=contact_handoff_keyboard(),
            )
            return MAIN
        create_lead(
            user,
            action="tenant_bind_success",
            source="tenant_bind",
            listing_id=str(binding.get("property_name") or ""),
            payload={"binding_code": binding_code, "binding_id": binding.get("id")},
        )
        _store_active_entry(
            context,
            arg=arg,
            action=action,
            listing_id=str(binding.get("property_name") or ""),
            touch_payload={"binding_code": binding_code, "binding_id": binding.get("id")},
        )
        await message.reply_text(
            (
                f"您好 {he(getattr(user, 'first_name', '') or '您好')}，租后管家已就位 🏠\n\n"
                + _binding_contract_text(binding, user.id)
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=_contract_actions_keyboard(user.id),
        )
        return MAIN

    if action == "channel_topic":
        topic = target
        context.user_data.pop("contact_listing_id", None)
        create_lead(
            user,
            action="channel_topic_click",
            source="channel_topic",
            area=detect_area(topic) if topic else "",
            payload={"topic": topic},
        )
        if topic == "district_guide":
            context.user_data["search_pref"] = {"source": "channel_topic", "goal": "any", "touch_payload": {"topic": topic}}
            await message.reply_text(channel_topic_welcome_text(topic), reply_markup=find_area_keyboard())
            return FIND_AREA
        if topic == "service":
            await message.reply_text(channel_topic_welcome_text(topic), reply_markup=service_hub_keyboard())
            return MAIN
        if topic == "video_tour":
            return await start_video_tour_flow(
                update,
                context,
                source="channel_topic",
            )
        await message.reply_text(channel_topic_welcome_text(topic), reply_markup=main_keyboard())
        return MAIN

    if action == "fav":
        listing_id = target
        db.favorite_listing(user.id, listing_id, now_ts())
        create_lead(
            user,
            action="favorite_click",
            source=source,
            listing_id=listing_id,
            payload=touch_payload,
        )
        await message.reply_text(
            "❤️ 这套先帮您记下了。\n\n" + listing_landing_text(listing_id),
            parse_mode=ParseMode.HTML,
            reply_markup=listing_landing_keyboard(listing_id),
        )
        return MAIN

    if action == "more":
        area, listing_id = _resolve_area_from_target(target)
        create_lead(
            user,
            action="more_area_click",
            source=source,
            area=area,
            listing_id=listing_id,
            payload={**touch_payload, "resolved_area": area, "listing_id": listing_id},
        )
        matches = db.search_listings(
            areas=[area] if area and area != "不限" else None,
            limit=3,
        )
        if matches:
            intro = ""
            if listing_id:
                intro = "🏠 这套房同区域还有这些在架房源，您可以直接继续看：\n\n"
            await message.reply_text(
                intro + _format_listing_choice_lines(matches),
                parse_mode=ParseMode.HTML,
                reply_markup=keyword_followup_keyboard(area=area),
            )
        else:
            await message.reply_text(
                f"当前同区域（{he(area or '金边')}）暂无更多上架房源。\n"
                "已同步管理号继续盯新房；您也可以继续按预算或户型缩小一轮。",
                parse_mode=ParseMode.HTML,
                reply_markup=no_match_followup_keyboard(),
            )
        return MAIN

    if action in {"brand", "about", "want_home", "ask"}:
        action_map = {
            "brand": "brand_click",
            "about": "about_click",
            "want_home": "want_home_click",
            "ask": "ask_click",
        }
        create_lead(
            user,
            action=action_map[action],
            source=source,
            payload={"start_arg": arg, **touch_payload},
        )
        if action == "brand":
            await message.reply_text(
                brand_story_text(),
                parse_mode=ParseMode.HTML,
                reply_markup=main_keyboard(),
            )
        elif action == "about":
            await message.reply_text(
                about_text(),
                parse_mode=ParseMode.HTML,
                reply_markup=main_keyboard(),
            )
        elif action == "want_home":
            context.user_data["pref_select"] = {
                "source": "channel_want_home",
                "selected": [],
            }
            await message.reply_text(
                want_home_prompt_text(),
                parse_mode=ParseMode.HTML,
                reply_markup=precise_filter_keyboard(set()),
            )
        else:
            await _notify_admins(
                context,
                title="咨询入口点击（频道深链）",
                lines=[
                    f"用户：{_user_mention_html(user)}",
                    f"联系方式：{he(_user_contact_text(user))}",
                    f"来源：{he(source)} ask",
                ],
            )
            await message.reply_text(
                advisor_text(),
                parse_mode=ParseMode.HTML,
                reply_markup=contact_handoff_keyboard(),
            )
        return MAIN

    if action == "discussion_entry":
        listing_id = target
        entry_source = str(target_meta.get("entry") or "discussion").strip().lower() or "discussion"
        context.user_data["contact_listing_id"] = listing_id or None
        create_lead(
            user,
            action="discussion_entry_click",
            source="discussion_entry",
            listing_id=listing_id,
            payload={
                "post_token": post_token,
                "listing_id": listing_id,
                "source": "discussion_entry",
                "entry": entry_source,
                **touch_payload,
            },
        )
        await _notify_admins(
            context,
            title="讨论区入口点击",
            lines=[
                f"用户：{_user_mention_html(user)}",
                f"联系方式：{he(_user_contact_text(user))}",
                f"房源：{he(listing_id or '-')}",
                f"post_token：{he(post_token or '-')}",
            ],
        )
        first_name = str(getattr(user, "first_name", "") or "")
        await message.reply_text(
            discussion_entry_welcome_text(first_name=first_name, listing_id=listing_id),
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard(),
        )
        return MAIN

    return None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    upsert_user_profile(user)
    if context.args:
        arg = context.args[0]
        active = context.user_data.get("active_entry") or {}
        if (
            str(active.get("arg") or "") == arg
            and (context.user_data.get("appt") or context.user_data.get("contact_listing_id"))
        ):
            context.user_data["resume_start_arg"] = arg
            await update.effective_message.reply_text(
                "检测到你正在同一条链路中。\n可继续当前流程，或重新开始。",
                reply_markup=_active_entry_resume_keyboard(),
            )
            return MAIN

        clear_session_for_fresh_entry(context)
        context.user_data.pop("resume_start_arg", None)
        state = await route_start_arg(update, context, context.args[0])
        if state is not None:
            return state
        await update.effective_message.reply_text("入口链接已失效，请从频道帖子重新进入。")
        return MAIN

    clear_session_for_fresh_entry(context)
    binding = db.get_active_binding(user.id)
    if binding:
        await update.effective_message.reply_text(
            "✅ <b>已识别您的在租档案</b>\n\n" + _binding_contract_text(binding, user.id),
            parse_mode=ParseMode.HTML,
            reply_markup=_contract_actions_keyboard(user.id),
        )
        return MAIN
    # 首次裸 /start（无深链参数）：展示压缩版首屏欢迎语
    await update.effective_message.reply_text(
        channel_welcome_text(first_name=str(getattr(user, "first_name", "") or "")),
        reply_markup=main_keyboard(),
        parse_mode=ParseMode.HTML,
    )
    return MAIN


def _appointment_date_keyboard() -> InlineKeyboardMarkup:
    btns = []
    for i in range(1, 6):
        date_code = (datetime.now() + timedelta(days=i)).strftime("%m-%d")
        btns.append(InlineKeyboardButton(date_code, callback_data="apdate:" + date_code))
    return InlineKeyboardMarkup(
        [
            btns[0:3],
            btns[3:5],
            [
                InlineKeyboardButton("⬅️ 返回上一步", callback_data="appoint_back_mode"),
                InlineKeyboardButton("🏠 返回首页", callback_data="home"),
            ],
        ]
    )


def _focus_summary_lines(keys: list[str] | set[str] | tuple[str, ...]) -> str:
    picked = [k for k in APPOINTMENT_FOCUS_ORDER if k in set(keys)]
    if not picked:
        return "（未选择）"
    return "\n".join(f"• {he(APPOINTMENT_FOCUS_LABELS[k])}" for k in picked)


def _appointment_focus_keyboard(selected: set[str]) -> InlineKeyboardMarkup:
    btn_rows: list[list[InlineKeyboardButton]] = []
    labels = APPOINTMENT_FOCUS_LABELS
    order = APPOINTMENT_FOCUS_ORDER
    row: list[InlineKeyboardButton] = []
    for idx, key in enumerate(order, start=1):
        checked = "✅" if key in selected else "▫️"
        row.append(
            InlineKeyboardButton(
                f"{checked} {labels[key]}",
                callback_data=f"apfocus:toggle:{key}",
            )
        )
        if idx % 2 == 0:
            btn_rows.append(row)
            row = []
    if row:
        btn_rows.append(row)
    btn_rows.append(
        [
            InlineKeyboardButton("✅ 下一步（选日期）", callback_data="apfocus:next"),
        ]
    )
    btn_rows.append(
        [
            InlineKeyboardButton("⬅️ 返回方式", callback_data="apfocus:back_mode"),
            InlineKeyboardButton("🏠 返回首页", callback_data="home"),
        ]
    )
    return InlineKeyboardMarkup(btn_rows)


def _appointment_focus_prompt(mode: str, listing_id: str, selected: set[str]) -> str:
    mode_label = APPOINTMENT_MODE_LABELS.get(mode, "预约看房")
    safe_lid = listing_id if listing_id and listing_id != "待推荐" else "暂未指定"
    return (
        f"<b>📅 {mode_label}</b>\n"
        f"房源：<code>{he(safe_lid)}</code>\n\n"
        "<b>第二步：请选择您最关注的验房点</b>\n"
        "默认 5 项全选，您只需点 <b>下一步</b> 就能继续。\n\n"
        f"{_focus_summary_lines(selected)}"
    )


async def start_appointment(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    listing_id: str,
    *,
    source: str = "user_bot",
    touch_payload: dict | None = None,
    initial_mode: str = "",
) -> int:
    context.user_data["appt"] = {
        "listing_id": listing_id,
        "source": source,
        "touch_payload": touch_payload or {},
        "focus_keys": list(APPOINTMENT_FOCUS_ORDER),
    }
    if initial_mode in APPOINTMENT_MODE_LABELS:
        context.user_data["appt"]["mode"] = initial_mode
        focus_set = set(context.user_data["appt"].get("focus_keys") or APPOINTMENT_FOCUS_ORDER)
        text = _appointment_focus_prompt(initial_mode, listing_id, focus_set)
        await render_panel(
            update,
            text=text,
            reply_markup=_appointment_focus_keyboard(focus_set),
            parse_mode=ParseMode.HTML,
        )
        return APPT_FOCUS

    text = (
        f"<b>📅 预约看房</b>\n"
        f"房源：<code>{he(listing_id if listing_id and listing_id != '待推荐' else '暂未指定')}</code>\n\n"
        "流程：1) 选方式 2) 选关注点 3) 选日期 4) 选时段 5) 提交\n\n"
        "第一步：请选择看房方式。"
    )
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📅 实地看房", callback_data="apmode:offline"),
                InlineKeyboardButton("🎥 视频代看", callback_data="apmode:video"),
            ],
            [InlineKeyboardButton("🏠 返回首页", callback_data="home")],
        ]
    )
    await render_panel(
        update,
        text=text,
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
        context=context,
    )
    return APPT_MODE


async def show_search_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.pop("awaiting_keyword_find", None)
    context.user_data["search_pref"] = {"source": "user_search", "touch_payload": {}}
    await render_panel(
        update,
        text=search_entry_intro_text(),
        reply_markup=search_entry_keyboard(),
        parse_mode=ParseMode.HTML,
        context=context,
    )
    return MAIN


async def show_precise_filter(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.pop("awaiting_keyword_find", None)
    context.user_data.pop("awaiting_want_home", None)
    context.user_data["pref_select"] = {
        "source": "menu_precise",
        "selected": [],
    }
    await render_panel(
        update,
        text=(
            "<b>📍 条件筛选</b>\n\n"
            "您最在意哪类条件？直接点选即可。\n"
            "选完点 <b>提交条件</b>，我会同步推送管理号人工收窄到 1-3 套。"
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=precise_filter_keyboard(set()),
        context=context,
    )
    return MAIN


async def show_appointment_hub(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    upsert_user_profile(user)
    create_lead(
        user,
        action="appointment_hub_view",
        source="main_menu",
        payload={"from_menu": True},
    )
    await render_panel(
        update,
        text=advisor_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=contact_handoff_keyboard(),
        context=context,
    )
    return MAIN


async def show_service_hub(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await render_panel(
        update,
        text=service_hub_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=service_hub_keyboard(),
        context=context,
    )
    return MAIN


async def show_favorites(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await render_panel(
        update,
        text=list_favorites_text(update.effective_user.id),
        reply_markup=main_keyboard(),
        context=context,
    )
    return MAIN


async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await render_panel(
        update,
        text=help_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=help_repeat_keyboard(),
        context=context,
    )
    return MAIN


async def contact_management(update: Update, context: ContextTypes.DEFAULT_TYPE, *, source: str = "menu") -> int:
    listing_id = ""
    binding = db.get_active_binding(update.effective_user.id)
    if context.user_data.get("contact_listing_id"):
        listing_id = str(context.user_data.get("contact_listing_id") or "")
    create_lead(
        update.effective_user,
        action="consult_menu_click",
        source=source,
        listing_id=listing_id or str((binding or {}).get("property_name") or ""),
        payload={"binding_id": (binding or {}).get("id"), "listing_id": listing_id},
    )
    await _notify_admins(
        context,
        title="咨询顾问请求（按钮）",
        lines=[
            f"用户：{_user_mention_html(update.effective_user)}",
            f"联系方式：{he(_user_contact_text(update.effective_user))}",
            f"来源：{he(source)}",
            f"房源：{he(listing_id or str((binding or {}).get('property_name') or '-'))}",
        ],
    )
    await render_panel(
        update,
        text=advisor_handoff_text(listing_id=listing_id, user_id=update.effective_user.id),
        parse_mode=ParseMode.HTML,
        reply_markup=contact_handoff_keyboard(),
        context=context,
    )
    return MAIN


async def handle_main_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    upsert_user_profile(user)
    text = (update.effective_message.text or "").strip()

    # 文本消息不再承载主导航，主流程全部走 inline 按钮。

    kctx = context.user_data.pop("awaiting_keyword_find", None)
    if kctx is not None:
        if not text:
            await render_panel(
                update,
                text="请发一句关键词需求，例如：BKK1 预算800内 1房 安静。",
                reply_markup=main_keyboard(),
                context=context,
                prefer_edit_anchor=True,
            )
            return MAIN
        budget_min, budget_max = parse_budget_range(text)
        area_raw = detect_area(text)
        area_use = area_raw if area_raw != text[:40] else ""
        property_type = detect_property_type(text)
        areas_arg = [area_use] if area_use and area_use != "不限" else None
        matches, match_mode = search_listings_with_fallback(
            property_type=property_type or None,
            area=area_use,
            budget_min=budget_min,
            budget_max=budget_max,
            text_fragment=text,
            limit=5,
        )
        used_fb = match_mode in {"fuzzy", "fallback_recent"}
        create_lead(
            user,
            action="keyword_find_play",
            source=str(kctx.get("source", "smart_find_play")),
            area=area_use,
            property_type=property_type,
            budget_min=budget_min,
            budget_max=budget_max,
            payload={"message": text, "match_mode": match_mode},
        )
        await _notify_admins(
            context,
            title="趣味关键词找房",
            lines=[
                f"用户：{_user_mention_html(user)}",
                f"联系方式：{he(_user_contact_text(user))}",
                f"模式：{he(match_mode)}",
                f"原文：<code>{he(text[:700])}</code>",
            ],
        )
        if match_mode == "strict":
            head = "✅ <b>已按您的需求先筛出这些房源</b>："
        elif match_mode in {"no_type", "no_area", "budget_only"}:
            head = "🔎 <b>我先帮您放宽一轮条件</b>，这些比较接近："
        elif match_mode == "fuzzy":
            head = "🔎 <b>我先按近似需求给您找一轮</b>："
        else:
            head = "📌 <b>暂时没有完全对上的</b>，先看近期在架房源："
        room_type = detect_room_type(text)
        body_parts = [_keyword_intro_text(area=area_use, room_type=room_type, budget_min=budget_min, budget_max=budget_max), "", head, ""]
        for item in matches:
            body_parts.append(_format_match_line(item))
        body_parts.append(smart_find_play_footer_hint_text(used_fallback=used_fb))
        await render_panel(
            update,
            text="\n".join(body_parts),
            parse_mode=ParseMode.HTML,
            reply_markup=keyword_followup_keyboard(area=area_use, room_type=room_type),
            context=context,
            prefer_edit_anchor=True,
        )
        return MAIN

    normalized_text = text.lower()
    natural_area = detect_area(text)
    area_use = natural_area if natural_area != text[:40] else ""
    room_type = detect_room_type(text)
    budget_min, budget_max = parse_budget_range(text)
    property_type = detect_property_type(text)
    wants_video = any(token in normalized_text for token in ("视频看房", "视频代看", "实拍", "视频"))

    if area_use or room_type or budget_min is not None or budget_max is not None:
        _remember_video_pref(
            context,
            area=area_use or None,
            budget_min=budget_min,
            budget_max=budget_max,
            layout=room_type or property_type or None,
        )

    if wants_video:
        return await start_video_tour_flow(
            update,
            context,
            source="natural_keyword",
            area=area_use,
            budget_min=budget_min,
            budget_max=budget_max,
            layout=room_type or property_type,
        )

    if area_use or room_type or budget_min is not None or budget_max is not None:
        matches, match_mode = search_listings_with_fallback(
            property_type=property_type or None,
            area=area_use,
            budget_min=budget_min,
            budget_max=budget_max,
            text_fragment=f"{text} {room_type}".strip(),
            limit=5,
        )
        create_lead(
            user,
            action="keyword_find_play",
            source="natural_keyword",
            area=area_use,
            property_type=property_type,
            budget_min=budget_min,
            budget_max=budget_max,
            payload={"message": text[:700], "match_mode": match_mode, "room_type": room_type},
        )
        if matches:
            body_parts = [_keyword_intro_text(area=area_use, room_type=room_type, budget_min=budget_min, budget_max=budget_max), "", "我先按这个方向给您筛了一轮：", ""]
            for item in matches:
                body_parts.append(_format_match_line(item))
            body_parts.append("\n如需更准，可以继续补一个区域、预算或户型，我会再收窄一轮。")
            await render_panel(
                update,
                text="\n".join(body_parts),
                parse_mode=ParseMode.HTML,
                reply_markup=keyword_followup_keyboard(area=area_use, room_type=room_type),
                context=context,
                prefer_edit_anchor=True,
            )
        else:
            await render_panel(
                update,
                text=(
                    _keyword_intro_text(area=area_use, room_type=room_type, budget_min=budget_min, budget_max=budget_max)
                    + "\n\n当前还没有完全对上的在架房源，我可以继续帮您缩小条件，或者直接转中文顾问继续筛。"
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=keyword_followup_keyboard(area=area_use, room_type=room_type),
                context=context,
                prefer_edit_anchor=True,
            )
        return MAIN

    if text in {"🏠 返回首页", "🏠 返回首页"}:
        clear_session_for_fresh_entry(context)
        await render_panel(
            update,
            text=welcome_text(),
            reply_markup=main_keyboard(),
            parse_mode=ParseMode.HTML,
            context=context,
            prefer_edit_anchor=True,
        )
        return MAIN

    pref = context.user_data.get("search_pref") or {}
    if pref.get("goal") or pref.get("area"):
        await render_panel(
            update,
            text="当前流程是按钮选择，请直接点上面的区域/预算按钮继续。",
            reply_markup=main_keyboard(),
            context=context,
            prefer_edit_anchor=True,
        )
        return MAIN

    contact_listing_id = str(context.user_data.get("contact_listing_id") or "").strip()
    if contact_listing_id:
        item = listing_context(contact_listing_id)
        contact_touch_payload = context.user_data.get("contact_touch_payload") or {}
        create_lead(
            user,
            action="consult_message",
            source="listing_chat",
            listing_id=contact_listing_id,
            area=str(item.get("area") or ""),
            property_type=str(item.get("property_type") or ""),
            payload={"message": text[:700], **contact_touch_payload},
        )
        await _notify_admins(
            context,
            title="房源咨询留言",
            lines=[
                f"用户：{_user_mention_html(user)}",
                f"联系方式：{he(_user_contact_text(user))}",
                f"房源：{he(contact_listing_id)}",
                f"留言：<code>{he(text[:700])}</code>",
            ],
        )
        await render_panel(
            update,
            text=(
                "已收到您对这套房的留言，顾问会按这条内容继续跟进。\n\n"
                "如果方便，也可以直接点下方按钮预约看房或直连顾问。"
            ),
            reply_markup=contact_handoff_keyboard(),
            parse_mode=ParseMode.HTML,
            context=context,
            prefer_edit_anchor=True,
        )
        return MAIN

    await render_panel(
        update,
        text="我这边先给您走按钮导航，您也可以直接发“BKK1、500以内、一房、视频看房”这类关键词。",
        reply_markup=main_keyboard(),
        context=context,
        prefer_edit_anchor=True,
    )
    return MAIN


async def handle_find_area(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    area = detect_area((update.effective_message.text or "").strip())
    current = context.user_data.get("search_pref") or {}
    goal = current.get("goal") or "any"
    context.user_data["search_pref"] = {
        "area": area,
        "source": current.get("source", "user_search"),
        "goal": goal,
        "touch_payload": current.get("touch_payload") or {},
    }
    _remember_video_pref(context, area=area)
    await render_panel(
        update,
        text=find_area_budget_hint_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=find_budget_keyboard(goal),
        context=context,
        prefer_edit_anchor=True,
    )
    return FIND_BUDGET


async def handle_find_budget(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    text = (update.effective_message.text or "").strip()
    pref = context.user_data.pop("search_pref", {})
    budget_min, budget_max = parse_budget_range(text)
    property_type = detect_property_type(text)
    area = pref.get("area", "")
    goal = str(pref.get("goal") or "")
    room_hint = detect_room_type(text) or ("" if goal in {"", "any", "住宅"} else goal)
    _remember_video_pref(
        context,
        area=area or None,
        budget_min=budget_min,
        budget_max=budget_max,
        layout=room_hint or None,
    )

    create_lead(
        user,
        action="search_pref_submit",
        source=pref.get("source", "user_search"),
        area=area,
        property_type=property_type,
        budget_min=budget_min,
        budget_max=budget_max,
        payload={
            "message": text,
            "area_hint": area,
            "goal": goal,
            **(pref.get("touch_payload") or {}),
        },
    )
    await _notify_admins(
        context,
        title="新找房条件",
        lines=[
            f"用户：{_user_mention_html(user)}",
            f"联系方式：{he(_user_contact_text(user))}",
            f"来源：{he(str(pref.get('source', 'user_search')))}",
            f"类型意向：{he(goal or '-')}",
            f"区域：{he(area or '-')}",
            f"预算：{he(_budget_text(budget_min, budget_max))}",
            f"户型：{he(property_type or '-')}",
            f"条件：<code>{he(text[:700])}</code>",
        ],
    )

    matches, match_mode = search_listings_with_fallback(
        property_type=property_type or None,
        area=area,
        budget_min=budget_min,
        budget_max=budget_max,
        text_fragment=text,
        limit=3,
    )
    if matches:
        lead_hint = ""
        if match_mode in {"no_type", "no_area", "budget_only", "fuzzy", "fallback_recent"}:
            lead_hint = "🔎 已为您放宽筛选条件，先看这几套：\n\n"
        await render_panel(
            update,
            text=f"{lead_hint}{_format_listing_choice_lines(matches)}",
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard(),
            context=context,
            prefer_edit_anchor=True,
        )
    else:
        await render_panel(
            update,
            text=find_no_match_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=no_match_followup_keyboard(),
            context=context,
            prefer_edit_anchor=True,
        )
    return MAIN


async def cmd_find(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    upsert_user_profile(update.effective_user)
    clear_session_for_fresh_entry(context)
    return await show_search_entry(update, context)


async def cmd_favorites(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    upsert_user_profile(update.effective_user)
    clear_session_for_fresh_entry(context)
    return await show_favorites(update, context)


async def cmd_appointments(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    upsert_user_profile(update.effective_user)
    clear_session_for_fresh_entry(context)
    await render_panel(
        update,
        text=list_recent_appointments(update.effective_user.id),
        reply_markup=main_keyboard(),
        context=context,
        prefer_edit_anchor=True,
    )
    return MAIN


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    upsert_user_profile(update.effective_user)
    clear_session_for_fresh_entry(context)
    return await show_help(update, context)


async def cmd_contact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    upsert_user_profile(update.effective_user)
    clear_session_for_fresh_entry(context)
    return await contact_management(update, context, source="command")


async def cmd_deal_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if not _is_admin_user(getattr(user, "id", 0)):
        await update.effective_message.reply_text("❌ 无权限。该命令仅限管理员使用。")
        return MAIN

    args = list(context.args or [])
    if len(args) < 2:
        await update.effective_message.reply_text(
            "用法：\n"
            "/deal_done <user_id> <binding_code> [property_name] [lease_end_date]\n\n"
            "示例：\n"
            "/deal_done 123456789 BIND20260421 QL-001 2026-12-31"
        )
        return MAIN

    try:
        target_user_id = int(args[0])
    except (TypeError, ValueError):
        await update.effective_message.reply_text("❌ user_id 无效，请传数字。")
        return MAIN

    binding_code = str(args[1]).strip()
    if not binding_code:
        await update.effective_message.reply_text("❌ binding_code 不能为空。")
        return MAIN

    remaining = [str(x).strip() for x in args[2:] if str(x).strip()]
    lease_end_date = ""
    if remaining and re.fullmatch(r"\d{4}-\d{2}-\d{2}", remaining[-1]):
        lease_end_date = remaining.pop()
    property_name = " ".join(remaining).strip() or "待补充"

    try:
        binding_id = db.create_binding(
            user_id=target_user_id,
            binding_code=binding_code,
            property_name=property_name,
            lease_end_date=lease_end_date,
            rent_day=None,
            created_at=now_ts(),
            status="pending",
        )
    except sqlite3.IntegrityError:
        await update.effective_message.reply_text(
            f"❌ 绑定码 `{binding_code}` 已存在，请换一个。",
            parse_mode=ParseMode.MARKDOWN,
        )
        return MAIN
    except Exception:
        logger.exception("创建 tenant binding 失败")
        await update.effective_message.reply_text("❌ 创建绑定失败，请稍后重试。")
        return MAIN

    deep_link = _deep_link(f"t_bind_{binding_code}")
    bind_kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔗 绑定租后管家", url=deep_link)]])
    push_ok = True
    try:
        await context.bot.send_message(
            chat_id=target_user_id,
            text=(
                "🎉 恭喜入住！\n\n"
                "入住后如需报修、续租或物业沟通，\n"
                "请点击下方按钮完成租后管家绑定（约 30 秒）。"
            ),
            reply_markup=bind_kb,
        )
    except Exception:
        push_ok = False
        logger.exception("向用户推送 t_bind 链接失败: user_id=%s", target_user_id)

    try:
        db.create_lead(
            {
                "user_id": target_user_id,
                "username": "",
                "display_name": "",
                "source": "admin_cmd",
                "action": "deal_done",
                "listing_id": property_name,
                "area": "",
                "property_type": "",
                "budget_min": None,
                "budget_max": None,
                "payload": {
                    "binding_id": binding_id,
                    "binding_code": binding_code,
                    "lease_end_date": lease_end_date,
                    "created_by_admin_id": getattr(user, "id", 0),
                    "push_ok": push_ok,
                },
                "message_id": None,
                "post_token": "",
                "caption_variant": "",
                "created_at": now_ts(),
            }
        )
    except Exception:
        logger.exception("写入 deal_done leads 失败: user_id=%s", target_user_id)

    await update.effective_message.reply_text(
        (
            "✅ 已创建成交绑定任务\n"
            f"- user_id: `{target_user_id}`\n"
            f"- binding_code: `{binding_code}`\n"
            f"- property: `{property_name}`\n"
            f"- lease_end: `{lease_end_date or '-'}\n"
            f"- push: `{'ok' if push_ok else 'failed'}`\n\n"
            f"用户绑定入口：{deep_link}"
        ),
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
    )
    return MAIN


async def cmd_lead_response(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if not _is_admin_user(getattr(user, "id", 0)):
        await update.effective_message.reply_text("❌ 无权限。该命令仅限管理员使用。")
        return MAIN

    args = list(context.args or [])
    if len(args) < 2:
        await update.effective_message.reply_text(
            "用法：\n"
            "/lead_response <lead_id> <agent_id> [response_at]\n\n"
            "示例：\n"
            "/lead_response 123 agent_zhang 2026-04-22 15:30:00"
        )
        return MAIN

    try:
        lead_id = int(args[0])
    except (TypeError, ValueError):
        await update.effective_message.reply_text("❌ lead_id 无效，请传数字。")
        return MAIN

    agent_id = str(args[1]).strip()
    if not agent_id:
        await update.effective_message.reply_text("❌ agent_id 不能为空。")
        return MAIN

    response_at = " ".join(args[2:]).strip() or now_ts()
    ok = db.mark_lead_responded(lead_id, agent_id=agent_id, response_at=response_at)
    if not ok:
        await update.effective_message.reply_text(f"⚠️ 未找到 lead_id={lead_id}，未更新。")
        return MAIN
    await update.effective_message.reply_text(
        f"✅ 已记录响应：lead_id={lead_id}, agent_id={agent_id}, response_at={response_at}"
    )
    return MAIN


async def cmd_push_local(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if not _is_admin_user(getattr(user, "id", 0)):
        await update.effective_message.reply_text("❌ 无权限。该命令仅限管理员使用。")
        return MAIN

    args = list(context.args or [])
    if len(args) < 2:
        await update.effective_message.reply_text(
            "用法：\n"
            "/push_local <小区关键词> <消息内容>\n\n"
            "示例：\n"
            "/push_local 富力城 【楼下新开】重庆火锅，营业时间 10:00-22:00，欢迎光临！"
        )
        return MAIN

    keyword = args[0].strip()
    message_body = " ".join(args[1:]).strip()
    if not keyword or not message_body:
        await update.effective_message.reply_text("❌ 小区关键词和消息内容不能为空。")
        return MAIN

    try:
        bindings = db.list_active_bindings_by_property(keyword)
    except Exception:
        logger.exception("查询小区租客失败: keyword=%s", keyword)
        await update.effective_message.reply_text("❌ 查询租客失败，请检查日志。")
        return MAIN

    if not bindings:
        await update.effective_message.reply_text(f"⚠️ 未找到小区「{keyword}」的活跃租客，未发送任何消息。")
        return MAIN

    sent = 0
    failed = 0
    for binding in bindings:
        try:
            uid = int(binding.get("user_id") or 0)
        except (TypeError, ValueError):
            uid = 0
        if uid <= 0:
            failed += 1
            continue
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=message_body,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            sent += 1
        except Exception:
            logger.exception("定向推送失败: user_id=%s", uid)
            failed += 1

    await update.effective_message.reply_text(
        f"✅ 定向推送完成\n"
        f"小区：{keyword}\n"
        f"成功：{sent} 人 | 失败/跳过：{failed} 人"
    )
    return MAIN


async def cmd_push_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if not _is_admin_user(getattr(user, "id", 0)):
        await update.effective_message.reply_text("❌ 无权限。该命令仅限管理员使用。")
        return MAIN

    args = list(context.args or [])
    if not args:
        await update.effective_message.reply_text(
            "用法：\n"
            "/push_all <消息内容>\n\n"
            "示例：\n"
            "/push_all 【侨联通知】本月起全面升级租后管家服务，敬请期待。"
        )
        return MAIN

    message_body = " ".join(args).strip()
    if not message_body:
        await update.effective_message.reply_text("❌ 消息内容不能为空。")
        return MAIN

    try:
        bindings = db.list_all_active_bindings()
    except Exception:
        logger.exception("查询全部活跃租客失败")
        await update.effective_message.reply_text("❌ 查询租客失败，请检查日志。")
        return MAIN

    if not bindings:
        await update.effective_message.reply_text("⚠️ 当前无活跃绑定租客，未发送任何消息。")
        return MAIN

    sent = 0
    failed = 0
    for binding in bindings:
        try:
            uid = int(binding.get("user_id") or 0)
        except (TypeError, ValueError):
            uid = 0
        if uid <= 0:
            failed += 1
            continue
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=message_body,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            sent += 1
        except Exception:
            logger.exception("全量推送失败: user_id=%s", uid)
            failed += 1

    await update.effective_message.reply_text(
        f"✅ 全量推送完成\n"
        f"成功：{sent} 人 | 失败/跳过：{failed} 人"
    )
    return MAIN


async def handle_ui_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    user = update.effective_user
    upsert_user_profile(user)

    if data == "home":
        clear_session_for_fresh_entry(context)
        context.user_data.pop("resume_start_arg", None)
        context.user_data.pop("contact_listing_id", None)
        context.user_data.pop("contact_touch_payload", None)
        await render_panel(
            update,
            text=welcome_text(),
            reply_markup=main_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        return MAIN

    if data == "hub:area":
        context.user_data["search_pref"] = {"source": "home_area", "goal": "any", "touch_payload": {}}
        await render_panel(
            update,
            text="📍 <b>按区域找房</b>\n\n先选区域，我再带您进下一步预算筛选。",
            parse_mode=ParseMode.HTML,
            reply_markup=find_area_keyboard(),
        )
        return FIND_AREA

    if data == "hub:budget":
        context.user_data["search_pref"] = {"source": "home_budget", "goal": "any", "area": "", "touch_payload": {}}
        await render_panel(
            update,
            text="💰 <b>按预算找房</b>\n\n先选预算，我再继续帮您缩小到区域或户型。",
            parse_mode=ParseMode.HTML,
            reply_markup=find_budget_keyboard("any"),
        )
        return FIND_BUDGET

    if data == "hub:layout":
        await render_panel(
            update,
            text="🛏 <b>按户型找房</b>\n\n先选想看的户型，我先给您一轮结果。",
            parse_mode=ParseMode.HTML,
            reply_markup=room_type_keyboard(),
        )
        return MAIN

    if data == "hub:latest":
        await render_panel(
            update,
            text=_latest_listing_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=latest_listing_keyboard(),
        )
        return MAIN

    if data == "hub:find":
        return await show_search_entry(update, context)

    if data == "hub:appoint":
        return await show_appointment_hub(update, context)

    if data == "hub:video_tour":
        return await start_video_tour_flow(update, context, source="home_video_button")

    if data == "hub:advisor":
        return await contact_management(update, context, source="hub")

    if data == "hub:precise":
        return await show_precise_filter(update, context)

    if data == "hub:account":
        await render_panel(
            update,
            text=list_favorites_text(user.id) + "\n\n" + list_recent_appointments(user.id),
            reply_markup=main_keyboard(),
        )
        return MAIN

    if data == "hub:favorites":
        return await show_favorites(update, context)

    if data == "hub:appointments":
        await render_panel(update, text=list_recent_appointments(user.id), reply_markup=main_keyboard())
        return MAIN

    if data == "hub:contract":
        binding = db.get_active_binding(user.id)
        await render_panel(
            update,
            text=_binding_contract_text(binding, user.id),
            parse_mode=ParseMode.HTML,
            reply_markup=_contract_actions_keyboard(user.id),
        )
        return MAIN

    if data == "hub:service":
        return await show_service_hub(update, context)

    if data == "hub:promise":
        await render_panel(
            update,
            text=promise_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard(),
        )
        return MAIN

    if data == "hub:help":
        return await show_help(update, context)

    if data.startswith("resume:"):
        action = data.split(":", 1)[1]
        resume_arg = str(context.user_data.get("resume_start_arg") or "").strip()
        if action == "continue":
            appt = context.user_data.get("appt") or {}
            listing_id = str(appt.get("listing_id") or context.user_data.get("contact_listing_id") or "").strip()
            if appt and listing_id:
                return await start_appointment(
                    update,
                    context,
                    listing_id,
                    source=str(appt.get("source") or "channel_deeplink"),
                    touch_payload=appt.get("touch_payload") or {},
                    initial_mode=str(appt.get("mode") or ""),
                )
            if listing_id:
                await render_panel(
                    update,
                    text=listing_landing_text(listing_id),
                    parse_mode=ParseMode.HTML,
                    reply_markup=listing_landing_keyboard(listing_id),
                )
                return MAIN
            await render_panel(
                update,
                text="当前没有可恢复的流程，已返回首页。",
                reply_markup=main_keyboard(),
            )
            return MAIN

        if action == "restart" and resume_arg:
            clear_session_for_fresh_entry(context)
            context.user_data.pop("resume_start_arg", None)
            state = await route_start_arg(update, context, resume_arg)
            if state is not None:
                return state
            await render_panel(update, text="入口链接已失效，请从频道帖子重新进入。")
            return MAIN

    if data == "findmode:play":
        context.user_data["awaiting_keyword_find"] = {"source": "smart_find_play"}
        await query.edit_message_text(
            smart_find_play_prompt_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("📍 改走按类型（稳）", callback_data="findmode:guided")],
                    [InlineKeyboardButton("🏠 返回首页", callback_data="home")],
                ]
            ),
        )
        return MAIN

    if data == "findmode:guided":
        context.user_data.pop("awaiting_keyword_find", None)
        await query.edit_message_text(
            smart_find_guided_header_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=guided_search_keyboard(),
        )
        return MAIN

    if data.startswith("roompick:"):
        room_type = data.split(":", 1)[1]
        _remember_video_pref(context, layout=room_type)
        property_type = detect_property_type(room_type)
        matches, match_mode = search_listings_with_fallback(
            property_type=property_type or None,
            area=None,
            budget_min=None,
            budget_max=None,
            text_fragment=room_type,
            limit=5,
        )
        create_lead(
            user,
            action="keyword_find_play",
            source="home_layout",
            property_type=property_type,
            payload={"message": room_type, "match_mode": match_mode, "room_type": room_type},
        )
        if matches:
            lines = [f"已先按 <b>{he(room_type)}</b> 给您筛一轮：", ""]
            for item in matches:
                lines.append(_format_match_line(item))
            lines.append("\n如需更准，可以再补一个区域或预算。")
            await render_panel(
                update,
                text="\n".join(lines),
                parse_mode=ParseMode.HTML,
                reply_markup=keyword_followup_keyboard(room_type=room_type),
            )
        else:
            await render_panel(
                update,
                text=f"当前没有完全匹配 <b>{he(room_type)}</b> 的在架房源，我可以继续按区域或预算帮您缩小。",
                parse_mode=ParseMode.HTML,
                reply_markup=keyword_followup_keyboard(room_type=room_type),
            )
        return MAIN

    if data == "profile:repeat":
        binding_text, binding = old_tenant_binding_text(user.id)
        create_lead(
            user,
            action="repeat_tenant_opt_in",
            source="help_inline",
            payload={
                "via": "profile:repeat",
                "binding_found": bool(binding),
                "property_name": str((binding or {}).get("property_name") or ""),
                "lease_end_date": str((binding or {}).get("lease_end_date") or ""),
                "rent_day": (binding or {}).get("rent_day"),
            },
        )
        await _notify_admins(
            context,
            title="老客回流登记",
            lines=[
                f"用户：{_user_mention_html(user)}",
                f"联系方式：{he(_user_contact_text(user))}",
                f"房号：{he(str((binding or {}).get('property_name') or '-'))}",
                f"交租日：{he(str((binding or {}).get('rent_day') or '-'))}",
                f"到期：{he(str((binding or {}).get('lease_end_date') or '-'))}",
                "说明：后台可按 user_id 维护房号/交租日/合同到期，作为老客回流入口",
            ],
        )
        await render_panel(
            update,
            text=binding_text,
            parse_mode=ParseMode.HTML,
            reply_markup=old_tenant_followup_keyboard(),
        )
        return MAIN

    if data == "contract:view":
        binding = db.get_active_binding(user.id)
        create_lead(
            user,
            action="contract_view_click",
            source="contract_hub",
            listing_id=str((binding or {}).get("property_name") or ""),
            payload={"binding_id": (binding or {}).get("id")},
        )
        await render_panel(
            update,
            text=_binding_contract_text(binding, user.id),
            parse_mode=ParseMode.HTML,
            reply_markup=_contract_actions_keyboard(user.id),
        )
        return MAIN

    if data == "contract:toggle_reminder":
        sub = db.toggle_lease_reminder(user.id, now_ts())
        enabled = int(sub.get("lease_reminder_enabled", 1) or 1) == 1
        binding = db.get_active_binding(user.id)
        create_lead(
            user,
            action="lease_reminder_enable_click" if enabled else "lease_reminder_disable_click",
            source="contract_hub",
            listing_id=str((binding or {}).get("property_name") or ""),
            payload={"binding_id": (binding or {}).get("id"), "lease_reminder_enabled": enabled},
        )
        prefix = "已开启到期提醒，30/7/3 天节点会自动提醒您。" if enabled else "已关闭到期提醒，后续不再自动推送到期消息。"
        await render_panel(
            update,
            text=f"{prefix}\n\n{_binding_contract_text(binding, user.id)}",
            parse_mode=ParseMode.HTML,
            reply_markup=_contract_actions_keyboard(user.id),
        )
        return MAIN

    if data == "contract:renew":
        binding = db.get_active_binding(user.id)
        if not binding:
            await render_panel(
                update,
                text="当前还没有绑定租约档案。\n请点「💬 联系顾问」，我们先把房号和到期日录入。",
                reply_markup=contact_handoff_keyboard(),
            )
            return MAIN
        days_left = _binding_days_left(binding)
        day_text = f"{days_left} 天" if days_left is not None else "待确认"
        open_tracking = db.get_open_renewal_tracking(
            binding_id=int(binding.get("id") or 0),
            user_id=user.id,
        )
        create_lead(
            user,
            action="renewal_inquiry_click",
            source="contract_hub",
            listing_id=str(binding.get("property_name") or ""),
            payload={
                "binding_id": binding.get("id"),
                "days_left": days_left,
                "open_tracking_id": (open_tracking or {}).get("id"),
            },
        )
        await render_panel(
            update,
            text="🔄 <b>续租咨询</b>\n\n"
            f"🏠 当前房号：{he(str(binding.get('property_name') or '-'))}\n"
            f"📅 到期日：{he(_binding_end_date(binding) or '待确认')}\n"
            f"⏳ 剩余：<b>{he(day_text)}</b>\n\n"
            + (
                "当前已有一张续租工单在跟进，若继续点击确认，我们会沿用原工单继续推进。"
                if open_tracking
                else "您打算继续住这套吗？确认后我们会把工单推给管理号跟进。"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "✅ 确认续租",
                            callback_data=f"contract:renew_yes:{int(binding.get('id') or 0)}",
                        ),
                        InlineKeyboardButton("🏠 我想换房", callback_data="contract:change"),
                    ],
                    [InlineKeyboardButton("💬 联系顾问", callback_data="appointment_menu:contact")],
                ]
            ),
        )
        return MAIN

    if data.startswith("contract:renew_yes:"):
        raw_bid = data.split(":", 2)[2]
        try:
            binding_id = int(raw_bid)
        except (TypeError, ValueError):
            binding_id = 0
        binding = db.get_binding_by_id(binding_id) if binding_id > 0 else None
        if not binding or int(binding.get("user_id") or 0) != int(user.id):
            await render_panel(
                update,
                text="未找到可确认的续租档案。\n请点「🔄 续租咨询」重新发起。",
                reply_markup=_contract_actions_keyboard(user.id),
            )
            return MAIN
        existing = db.get_open_renewal_tracking(binding_id=binding_id, user_id=user.id)
        tracking_id = int(existing.get("id") or 0) if existing else 0
        if tracking_id <= 0:
            tracking_id = db.create_renewal_tracking(
                binding_id=binding_id,
                user_id=user.id,
                listing_id=str(binding.get("property_name") or ""),
                renewal_status="pending",
                user_response="用户确认续租",
                created_at=now_ts(),
            )
        create_lead(
            user,
            action="renewal_confirm_click",
            source="contract_hub",
            listing_id=str(binding.get("property_name") or ""),
            payload={
                "binding_id": binding_id,
                "tracking_id": tracking_id,
                "deduped": bool(existing),
            },
        )
        if not existing:
            await _notify_admins(
                context,
                title="续租意向确认",
                lines=[
                    f"用户：{_user_mention_html(user)}",
                    f"联系方式：{he(_user_contact_text(user))}",
                    f"房号：{he(str(binding.get('property_name') or '-'))}",
                    f"到期：{he(_binding_end_date(binding) or '-')}",
                    f"工单：RT-{he(str(tracking_id))}",
                    "请在 24 小时内联系租客确认续租条款。",
                ],
            )
        await render_panel(
            update,
            text=(
                "⏳ <b>续租工单已在跟进中</b>\n\n"
                "我们沿用之前的工单继续处理，管理号会尽快联系您确认租期与价格。"
                if existing
                else "✅ <b>续租意向已提交</b>\n\n管理号已收到工单，会尽快联系您确认租期与价格。"
            )
            + "\n如有变更，也可以直接点下方联系顾问。",
            parse_mode=ParseMode.HTML,
            reply_markup=_contract_actions_keyboard(user.id),
        )
        return MAIN

    if data == "contract:change":
        binding = db.get_active_binding(user.id)
        create_lead(
            user,
            action="change_house_click",
            source="contract_hub",
            listing_id=str((binding or {}).get("property_name") or ""),
            payload={"binding_id": (binding or {}).get("id")},
        )
        if binding:
            await _notify_admins(
                context,
                title="老客换房意向",
                lines=[
                    f"用户：{_user_mention_html(user)}",
                    f"联系方式：{he(_user_contact_text(user))}",
                    f"当前房号：{he(str(binding.get('property_name') or '-'))}",
                    f"到期：{he(_binding_end_date(binding) or '-')}",
                ],
            )
        await render_panel(
            update,
            text="🏠 <b>换房服务</b>\n\n"
            "我们会按您当前预算/区域重新筛选 1-3 套可决策房源。\n"
            "您可以直接浏览频道，也可以让顾问立刻接手。",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🔍 立即筛房", callback_data="findmode:guided")],
                    [InlineKeyboardButton("💬 联系顾问", callback_data="appointment_menu:contact")],
                    [InlineKeyboardButton("📋 返回租约", callback_data="contract:view")],
                ]
            ),
        )
        return MAIN

    if data.startswith("findtype:"):
        goal = data.split(":", 1)[1]
        context.user_data["search_pref"] = {
            "source": "user_search",
            "goal": goal,
            "touch_payload": {},
        }
        goal_text = "我也说不清，直接帮我找" if goal == "any" else goal
        await query.edit_message_text(
            f"🏠 <b>{he(goal_text)}</b>\n\n"
            "<b>第二步：请选择区域</b>\n"
            "全流程点击完成，不需要手动输入。",
            parse_mode=ParseMode.HTML,
            reply_markup=find_area_keyboard(),
        )
        return FIND_AREA

    if data.startswith("unavail:more:"):
        area = detect_area(data.split(":", 2)[2])
        create_lead(
            user,
            action="unavailable_more_click",
            source="listing_unavailable",
            area=area,
            listing_id=str(context.user_data.get("contact_listing_id") or ""),
        )
        matches = db.search_listings(
            areas=[area] if area and area != "不限" else None,
            limit=3,
        )
        if matches:
            await render_panel(
                update,
                text=_format_listing_choice_lines(matches),
                parse_mode=ParseMode.HTML,
                reply_markup=main_keyboard(),
            )
        else:
            await render_panel(
                update,
                text=f"当前同区域（{he(area or '金边')}）暂无更多上架房源。\n"
                "可以点「继续找房」筛选，或直接联系顾问给你人工推荐。",
                parse_mode=ParseMode.HTML,
                reply_markup=listing_unavailable_keyboard(),
            )
        return MAIN

    if data == "findback:area":
        pref = context.user_data.get("search_pref") or {}
        goal = str(pref.get("goal") or "any")
        goal_text = "我也说不清，直接帮我找" if goal == "any" else goal
        await query.edit_message_text(
            f"🏠 <b>{he(goal_text)}</b>\n\n"
            "<b>第二步：请选择区域</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=find_area_keyboard(),
        )
        return FIND_AREA

    if data.startswith("findarea:"):
        code = data.split(":", 1)[1]
        area = FIND_AREA_CODE_MAP.get(code, "")
        if not area:
            return FIND_AREA
        pref = context.user_data.get("search_pref") or {}
        pref["area"] = area
        context.user_data["search_pref"] = pref
        goal = str(pref.get("goal") or "any")
        goal_text = "我也说不清，直接帮我找" if goal == "any" else goal
        await query.edit_message_text(
            f"🏠 <b>{he(goal_text)}</b>\n"
            f"📍 已选区域：<b>{he(area)}</b>\n\n"
            "<b>第三步：请选择预算区间</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=find_budget_keyboard(goal),
        )
        return FIND_BUDGET

    if data.startswith("findbudget:"):
        code = data.split(":", 1)[1]
        pref = context.user_data.pop("search_pref", {})
        goal = str(pref.get("goal") or "any")
        area = str(pref.get("area") or "")
        budget_label, budget_min, budget_max = _decode_budget_choice(goal, code)

        layout_hint = "" if goal in {"any", "住宅"} else goal
        _remember_video_pref(
            context,
            area=area or None,
            budget_min=budget_min,
            budget_max=budget_max,
            layout=layout_hint or None,
        )

        type_filter = "" if goal in {"any", "住宅"} else goal
        create_lead(
            user,
            action="search_pref_submit",
            source=str(pref.get("source", "user_search")),
            area=area if area != "不限" else "",
            property_type=type_filter,
            budget_min=budget_min,
            budget_max=budget_max,
            payload={
                "message": f"goal={goal}; area={area}; budget={budget_label}",
                "goal": goal,
                "area_hint": area,
                "budget_label": budget_label,
                **(pref.get("touch_payload") or {}),
            },
        )
        await _notify_admins(
            context,
            title="新找房条件（点击提交）",
            lines=[
                f"用户：{_user_mention_html(user)}",
                f"联系方式：{he(_user_contact_text(user))}",
                f"类型意向：{he(goal)}",
                f"区域：{he(area or '-')}",
                f"预算：{he(budget_label)}",
            ],
        )

        matches, match_mode = search_listings_with_fallback(
            property_type=type_filter or None,
            area=area,
            budget_min=budget_min,
            budget_max=budget_max,
            text_fragment=f"{goal} {area} {budget_label}",
            limit=3,
        )
        if matches:
            if match_mode in {"no_type", "no_area", "budget_only", "fuzzy", "fallback_recent"}:
                await query.answer("已自动放宽条件匹配", show_alert=False)
            await render_panel(
                update,
                text=_format_listing_choice_lines(matches),
                parse_mode=ParseMode.HTML,
                reply_markup=main_keyboard(),
            )
        else:
            await render_panel(
                update,
                text=find_no_match_text(),
                parse_mode=ParseMode.HTML,
                reply_markup=no_match_followup_keyboard(),
            )
        return MAIN

    if data == "appointment_menu:list":
        await render_panel(
            update,
            text=list_recent_appointments(user.id),
            reply_markup=main_keyboard(),
            context=context,
        )
        return MAIN

    if data.startswith("appointment_menu:contact"):
        parts = data.split(":", 3)
        scope = parts[2] if len(parts) >= 3 else ""
        ref = parts[3] if len(parts) >= 4 else ""
        listing_id = ""
        binding = db.get_active_binding(user.id)
        source_label = "appointment_hub"
        if scope == "listing":
            listing_id = ref
            context.user_data["contact_listing_id"] = listing_id
            source_label = "listing_landing"
        else:
            listing_id = str(context.user_data.get("contact_listing_id") or "")
        create_lead(
            user,
            action="consult_menu_click",
            source=source_label,
            listing_id=listing_id or str((binding or {}).get("property_name") or ""),
            payload={"binding_id": (binding or {}).get("id"), "listing_id": listing_id},
        )
        await _notify_admins(
            context,
            title="咨询顾问请求（按钮承接）",
            lines=[
                f"用户：{_user_mention_html(user)}",
                f"联系方式：{he(_user_contact_text(user))}",
                f"来源：{he(source_label)}",
                f"房源：{he(listing_id or str((binding or {}).get('property_name') or '-'))}",
            ],
        )
        await render_panel(
            update,
            text=advisor_handoff_text(listing_id=listing_id, user_id=user.id),
            parse_mode=ParseMode.HTML,
            reply_markup=contact_handoff_keyboard(),
        )
        return MAIN

    if data.startswith("appointment_menu:"):
        mode = data.split(":", 1)[1]
        create_lead(
            user,
            action="appointment_click",
            source="menu_appointment",
            payload={"from_menu": True, "preferred_mode": mode},
        )
        return await start_appointment(
            update,
            context,
            "待推荐",
            source="menu_appointment",
            touch_payload={"from_menu": True, "listing_unknown": True},
            initial_mode=mode,
        )

    if data == "lead_capture:phone":
        create_lead(
            user,
            action="lead_capture_phone_request",
            source="lead_capture",
            listing_id=str(context.user_data.get("contact_listing_id") or ""),
            payload={"intent": "phone_share"},
        )
        await render_panel(
            update,
            text=(
                "好的，请直接发送您的手机号码。\n\n"
                "或者，可以发 Telegram 账号 / 微信 ID 也可以，顾问会主动联系您。"
            ),
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🏠 继续看房", callback_data="home")],
                ]
            ),
            context=context,
        )
        return MAIN

    if data == "pref:clear":
        context.user_data["pref_select"] = {"source": "menu_precise", "selected": []}
        await query.edit_message_text(
            "<b>📍 条件筛选</b>\n\n"
            "已清空，继续点选后提交即可。",
            parse_mode=ParseMode.HTML,
            reply_markup=precise_filter_keyboard(set()),
        )
        return MAIN

    if data.startswith("pref:toggle:"):
        key = data.split(":", 2)[2]
        if key not in PREF_CONDITION_LABELS:
            return MAIN
        pref_ctx = context.user_data.setdefault("pref_select", {"source": "menu_precise", "selected": []})
        selected = [str(x) for x in (pref_ctx.get("selected") or []) if str(x) in PREF_CONDITION_LABELS]
        selected_set = set(selected)
        if key in selected_set:
            selected_set.remove(key)
        else:
            selected_set.add(key)
        pref_ctx["selected"] = list(selected_set)
        summary = "、".join(PREF_CONDITION_LABELS[k] for k in pref_ctx["selected"][:6]) or "未选择"
        await query.edit_message_text(
            "<b>📍 条件筛选</b>\n\n"
            f"当前已选：{he(summary)}\n"
            "选完点 <b>提交条件</b>，无需手动打字。",
            parse_mode=ParseMode.HTML,
            reply_markup=precise_filter_keyboard(set(pref_ctx["selected"])),
        )
        return MAIN

    if data == "pref:submit":
        pref_ctx = context.user_data.pop("pref_select", {"source": "menu_precise", "selected": []})
        selected = [str(x) for x in (pref_ctx.get("selected") or []) if str(x) in PREF_CONDITION_LABELS]
        selected_labels = [PREF_CONDITION_LABELS[x] for x in selected]
        summary = "、".join(selected_labels) if selected_labels else "未勾选具体条件"
        create_lead(
            user,
            action="search_pref_submit",
            source=str(pref_ctx.get("source", "menu_precise")),
            payload={
                "condition_keys": selected,
                "condition_labels": selected_labels,
                "message": summary,
            },
        )
        await _notify_admins(
            context,
            title="新条件筛选（点击提交）",
            lines=[
                f"用户：{_user_mention_html(user)}",
                f"联系方式：{he(_user_contact_text(user))}",
                f"条件：{he(summary)}",
                "说明：用户通过按钮提交条件筛选",
            ],
        )
        await render_panel(
            update,
            text="✅ 已收到您的条件，已推送管理号人工筛选。\n"
            "接下来会优先给您 1-3 套可直接做决定的房源。",
            reply_markup=main_keyboard(),
        )
        return MAIN

    if data == "service:hub":
        await query.edit_message_text(
            service_hub_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=service_hub_keyboard(),
        )
        return MAIN

    if data == "service:contact":
        create_lead(user, action="consult_menu_click", source="service_hub")
        await _notify_admins(
            context,
            title="咨询顾问请求（入住服务按钮）",
            lines=[
                f"用户：{_user_mention_html(user)}",
                f"联系方式：{he(_user_contact_text(user))}",
                "来源：service_hub",
            ],
        )
        await render_panel(
            update,
            text=advisor_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=contact_handoff_keyboard(),
        )
        return MAIN

    if data == "service:renew_change":
        create_lead(
            user,
            action="service_renew_change_click",
            source="service_hub",
            listing_id=str(context.user_data.get("contact_listing_id") or ""),
        )
        await query.edit_message_text(
            "🔁 <b>续租 / 换房服务</b>\n\n"
            "如果你准备续租、换房或退租，侨联可以协助：\n"
            "• 先核对当前租约关键条款\n"
            "• 评估续租谈判或换房方案\n"
            "• 对接下一套看房与衔接时间\n\n"
            "请选择你现在更需要哪种协助：",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("🔄 续租咨询", callback_data="contract:renew"),
                        InlineKeyboardButton("🏠 我要换房", callback_data="contract:change"),
                    ],
                    [InlineKeyboardButton("💬 联系顾问", callback_data="service:contact")],
                    [InlineKeyboardButton("⬅️ 返回入住服务", callback_data="service:hub")],
                ]
            ),
        )
        return MAIN

    if data == "service:move":
        await query.edit_message_text(
            "<b>📦 搬家协助</b>\n\n"
            "可协助您对接搬家车辆、人手与时间安排。\n"
            "我们会按您的入住时间给出执行建议。\n\n"
            "请点下方「联系顾问」转人工协助。",
            parse_mode=ParseMode.HTML,
            reply_markup=service_detail_keyboard(),
        )
        return MAIN

    if data == "service:handover":
        await query.edit_message_text(
            "<b>🧾 入住交接留档</b>\n\n"
            "入住前建议把房屋现状、水电表、家具家电状态拍照留档，\n"
            "便于后续退租时对照。\n\n"
            "需要时也可以让我们提醒您现场重点看哪些细节。",
            parse_mode=ParseMode.HTML,
            reply_markup=service_detail_keyboard(),
        )
        return MAIN

    if data == "service:deposit":
        create_lead(
            user,
            action="deposit_inquiry",
            source="service_deposit",
            listing_id=str(context.user_data.get("contact_listing_id") or ""),
            payload={"intent": "price_question"},
        )
        await query.edit_message_text(
            deposit_text() + "\n\n" + lead_capture_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=lead_capture_keyboard(),
        )
        return MAIN

    if data == "service:staging":
        await query.edit_message_text(
            "<b>📹 代拍验房</b>\n\n"
            "没空到现场也没关系。\n"
            "我们可以先过去拍，或和您实时视频连线。\n\n"
            "会优先替您确认：\n"
            "• 空调型号和老旧程度\n"
            "• 冰箱、洗衣机等家电状态\n"
            "• 采光、噪音、楼道与周边情况\n"
            "• 水电网和押付方式\n\n"
            "如果要安排，请点下方「联系顾问」。",
            parse_mode=ParseMode.HTML,
            reply_markup=service_detail_keyboard(),
        )
        return MAIN

    if data == "service:addons":
        await query.edit_message_text(
            "<b>🛋 家具家电补配</b>\n\n"
            "入住前如果需要补床垫、桌椅、窗帘、小家电，\n"
            "请点下方「联系顾问」，我们统一帮您对接和确认。",
            parse_mode=ParseMode.HTML,
            reply_markup=service_detail_keyboard(),
        )
        return MAIN

    if data in ("service:guide", "service:local_life"):
        await query.edit_message_text(
            local_life_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=local_life_keyboard(),
        )
        return MAIN

    if data == "service:checkin_tips":
        await query.edit_message_text(
            "<b>📋 入住注意事项</b>\n\n"
            "给您一份「少踩坑」清单，都是现场容易忘看的点：\n"
            "• 门禁 / 电梯卡几张、押金多少\n"
            "• 水压、热水、地漏、马桶冲水\n"
            "• 窗户密封与隔音、阳台排水\n"
            "• 空调试机 10 分钟、外机噪音\n"
            "• 合同里维修责任与联系人写清\n\n"
            "需要顾问按这套清单帮您走一遍，点下方「联系顾问」。",
            parse_mode=ParseMode.HTML,
            reply_markup=service_detail_keyboard(),
        )
        return MAIN

    if data == "service:repair_hub":
        await query.edit_message_text(
            "<b>🔧 租后管家服务</b>\n\n"
            "遇到问题，找侨联，更省心。\n\n"
            "请选择您现在需要协助的事项：",
            parse_mode=ParseMode.HTML,
            reply_markup=service_repair_keyboard(),
        )
        return MAIN

    if data.startswith("service_request:"):
        issue_key = data.split(":", 1)[1]
        issue_label = SERVICE_REQUEST_LABELS.get(issue_key, issue_key)
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("🚨 今天内安排", callback_data=f"service_slot:{issue_key}:today"),
                    InlineKeyboardButton("🕘 明天上午", callback_data=f"service_slot:{issue_key}:tomorrow_am"),
                ],
                [
                    InlineKeyboardButton("🕒 明天下午", callback_data=f"service_slot:{issue_key}:tomorrow_pm"),
                    InlineKeyboardButton("💬 联系顾问", callback_data="service:contact"),
                ],
                [InlineKeyboardButton("⬅️ 返回入住服务", callback_data="service:hub")],
            ]
        )
        await render_panel(
            update,
            text=f"🔧 <b>{he(issue_label)}</b>\n\n"
            "请选择处理时间，直接点击提交即可。",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return MAIN

    if data.startswith("service_slot:"):
        _, issue_key, slot = data.split(":", 2)
        issue_label = SERVICE_REQUEST_LABELS.get(issue_key, issue_key)
        slot_map = {
            "today": "今天内安排",
            "tomorrow_am": "明天上午",
            "tomorrow_pm": "明天下午",
        }
        slot_label = slot_map.get(slot, slot)
        binding = db.get_active_binding(user.id)
        binding_id = int((binding or {}).get("id") or 0) or None
        ticket_id = db.create_repair_ticket(
            user.id,
            binding_id,
            issue_label,
            f"按钮提交：{slot_label}",
            now_ts(),
        )
        create_lead(
            user,
            action="service_request_submit",
            source="service_hub",
            listing_id=str((binding or {}).get("property_name") or ""),
            payload={
                "issue_key": issue_key,
                "issue_label": issue_label,
                "time_slot": slot,
                "binding_id": binding_id,
            },
        )
        await _notify_admins(
            context,
            title="新入住服务请求（按钮提交）",
            lines=[
                f"用户：{_user_mention_html(user)}",
                f"联系方式：{he(_user_contact_text(user))}",
                f"房号：{he(str((binding or {}).get('property_name') or '-'))}",
                f"事项：{he(issue_label)}",
                f"时间：{he(slot_label)}",
                f"工单：{he(str(ticket_id))}",
            ],
        )
        await render_panel(
            update,
            text=f"✅ 已提交 {issue_label}（{slot_label}），已推送管理号处理。",
            reply_markup=main_keyboard(),
        )
        return MAIN

    if data == "local:rfcity":
        create_lead(
            user,
            action="local_area_click",
            source="local_life",
            area="rfcity",
            payload={"area": "rfcity", "category": "overview"},
        )
        await query.edit_message_text(
            rfcity_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=rfcity_keyboard(),
        )
        return MAIN

    if data.startswith("rfcity:"):
        category = data.split(":", 1)[1]
        create_lead(
            user,
            action="local_category_click",
            source="rfcity",
            area="rfcity",
            payload={"area": "rfcity", "category": category},
        )
        _rfcity_texts = {
            "restaurant": copy_rfcity_restaurant_text,
            "bbq": copy_rfcity_bbq_text,
            "drinks": copy_rfcity_drinks_text,
            "supermarket": copy_rfcity_supermarket_text,
            "hotel": copy_rfcity_hotel_text,
            "recreation": copy_rfcity_recreation_text,
            "logistics": copy_rfcity_logistics_text,
            "property": copy_rfcity_property_text,
        }
        if category == "join":
            await query.edit_message_text(
                copy_merchant_join_text(),
                parse_mode=ParseMode.HTML,
                reply_markup=merchant_join_keyboard(),
            )
        elif category in _rfcity_texts:
            await query.edit_message_text(
                _rfcity_texts[category](),
                parse_mode=ParseMode.HTML,
                reply_markup=rfcity_back_keyboard(),
            )
        else:
            await query.edit_message_text(
                rfcity_text(),
                parse_mode=ParseMode.HTML,
                reply_markup=rfcity_keyboard(),
            )
        return MAIN

    return MAIN


async def lease_reminder_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """租约到期提醒（30/7/3 天）；按日志去重，默认每天早上触发一次。"""
    now = datetime.now()
    for days_before in LEASE_REMINDER_DAYS:
        target_date = (now + timedelta(days=days_before)).strftime("%Y-%m-%d")
        remind_type = f"{days_before}days"
        try:
            bindings = db.list_bindings_expiring_on(target_date)
        except Exception:
            logger.exception("查询到期租约失败: target=%s", target_date)
            continue

        for binding in bindings:
            try:
                user_id = int(binding.get("user_id") or 0)
                binding_id = int(binding.get("id") or 0)
            except (TypeError, ValueError):
                continue
            if user_id <= 0 or binding_id <= 0:
                continue
            if not db.is_lease_reminder_enabled(user_id):
                continue
            if db.has_reminder_sent(binding_id=binding_id, remind_type=remind_type, remind_date=target_date):
                continue

            property_name = str(binding.get("property_name") or "-")
            end_date = _binding_end_date(binding) or target_date
            rent_raw = binding.get("monthly_rent")
            try:
                rent_value = float(rent_raw or 0)
            except (TypeError, ValueError):
                rent_value = 0
            rent_line = f"${int(rent_value)}/月" if rent_value > 0 else "待确认"
            name = str(binding.get("first_name") or "您好")
            text = (
                f"⏰ <b>租约到期提醒</b>\n\n"
                f"{he(name)}，您的租约即将到期：\n"
                f"🏠 房号：{he(property_name)}\n"
                f"💰 月租：{he(rent_line)}\n"
                f"📅 到期日：{he(end_date)}\n"
                f"⚠️ 剩余：<b>{days_before} 天</b>\n\n"
                "如果准备续租或换房，点下面按钮我们马上跟进。"
            )
            keyboard = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "✅ 我要续租",
                            callback_data=f"contract:renew_yes:{binding_id}",
                        ),
                        InlineKeyboardButton("🏠 我想换房", callback_data="contract:change"),
                    ],
                    [InlineKeyboardButton("💬 联系顾问", callback_data="appointment_menu:contact")],
                ]
            )
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard,
                )
                db.log_reminder_sent(
                    binding_id=binding_id,
                    user_id=user_id,
                    lease_end_date=end_date,
                    remind_for_date=target_date,
                    remind_type=remind_type,
                    sent_at=now_ts(),
                )
                await _notify_admins(
                    context,
                    title=f"到期提醒已发送（{days_before}天）",
                    lines=[
                        f"用户ID：{he(str(user_id))}",
                        f"房号：{he(property_name)}",
                        f"到期：{he(end_date)}",
                        f"类型：{he(remind_type)}",
                    ],
                )
            except Exception:
                logger.exception(
                    "发送租约提醒失败: user_id=%s binding_id=%s target=%s",
                    user_id,
                    binding_id,
                    target_date,
                )


async def rent_day_reminder_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """每月交租提醒：在交租日前 7 天早上发送一次，按日志去重。"""
    now = datetime.now()
    target = now + timedelta(days=7)
    rent_day = target.day
    remind_date = target.strftime("%Y-%m-%d")
    remind_type = "rent_7days"
    try:
        bindings = db.list_bindings_with_rent_day(rent_day)
    except Exception:
        logger.exception("查询交租日租约失败: rent_day=%s", rent_day)
        return

    for binding in bindings:
        try:
            user_id = int(binding.get("user_id") or 0)
            binding_id = int(binding.get("id") or 0)
        except (TypeError, ValueError):
            continue
        if user_id <= 0 or binding_id <= 0:
            continue
        if not db.is_lease_reminder_enabled(user_id):
            continue
        if db.has_reminder_sent(binding_id=binding_id, remind_type=remind_type, remind_date=remind_date):
            continue

        property_name = str(binding.get("property_name") or "-")
        rent_raw = binding.get("monthly_rent")
        try:
            rent_value = float(rent_raw or 0)
        except (TypeError, ValueError):
            rent_value = 0
        rent_line = f"${int(rent_value)}/月" if rent_value > 0 else "待确认"
        name = str(binding.get("first_name") or "您好")
        text = (
            f"💰 <b>交租提醒</b>\n\n"
            f"{he(name)}，您在【{he(property_name)}】的租金将于 <b>{he(remind_date)}（{rent_day} 号）</b> 到期。\n\n"
            f"💵 月租：{he(rent_line)}\n\n"
            "请提前安排转账，如有疑问可联系顾问。"
        )
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("💬 联系顾问", callback_data="appointment_menu:contact")],
            ]
        )
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
            db.log_reminder_sent(
                binding_id=binding_id,
                user_id=user_id,
                lease_end_date=_binding_end_date(binding) or "",
                remind_for_date=remind_date,
                remind_type=remind_type,
                sent_at=now_ts(),
            )
            await _notify_admins(
                context,
                title="交租提醒已发送",
                lines=[
                    f"用户ID：{he(str(user_id))}",
                    f"房号：{he(property_name)}",
                    f"交租日：{rent_day} 号（{he(remind_date)}）",
                    f"月租：{he(rent_line)}",
                ],
            )
        except Exception:
            logger.exception(
                "发送交租提醒失败: user_id=%s binding_id=%s remind_date=%s",
                user_id,
                binding_id,
                remind_date,
            )


async def appoint_flow_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    data = query.data
    appt = context.user_data.setdefault("appt", {})

    if data.startswith("apmode:"):
        lid = str(appt.get("listing_id") or "").strip()
        if not lid or lid == "未知":
            context.user_data.pop("appt", None)
            await query.edit_message_text(
                "无法识别这套房源。\n\n"
                "请从频道帖子里的「预约看房」进入；或在首页点「📅 预约看房」后按提示操作。",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 返回首页", callback_data="home")]]),
                parse_mode=ParseMode.HTML,
            )
            return MAIN
        appt["mode"] = data.split(":", 1)[1]
        appt["focus_keys"] = list(APPOINTMENT_FOCUS_ORDER)
        focus_set = set(appt.get("focus_keys") or APPOINTMENT_FOCUS_ORDER)
        text = _appointment_focus_prompt(appt["mode"], appt.get("listing_id", ""), focus_set)
        await query.edit_message_text(
            text,
            reply_markup=_appointment_focus_keyboard(focus_set),
            parse_mode=ParseMode.HTML,
        )
        return APPT_FOCUS

    if data.startswith("apfocus:toggle:"):
        key = data.split(":", 2)[2]
        if key not in APPOINTMENT_FOCUS_LABELS:
            return APPT_FOCUS
        selected = set(str(k) for k in (appt.get("focus_keys") or APPOINTMENT_FOCUS_ORDER))
        if key in selected:
            selected.remove(key)
        else:
            selected.add(key)
        appt["focus_keys"] = [k for k in APPOINTMENT_FOCUS_ORDER if k in selected]
        text = _appointment_focus_prompt(appt.get("mode", "offline"), appt.get("listing_id", ""), selected)
        await query.edit_message_text(
            text,
            reply_markup=_appointment_focus_keyboard(selected),
            parse_mode=ParseMode.HTML,
        )
        return APPT_FOCUS

    if data == "apfocus:back_mode":
        return await start_appointment(
            update,
            context,
            appt.get("listing_id", "未知"),
            source=appt.get("source", "user_bot"),
            touch_payload=appt.get("touch_payload"),
        )

    if data == "apfocus:next":
        selected = set(str(k) for k in (appt.get("focus_keys") or []))
        if not selected:
            await query.answer("至少保留 1 个关注点", show_alert=True)
            return APPT_FOCUS
        text = "第三步：请选择预约日期。"
        await query.edit_message_text(text, reply_markup=_appointment_date_keyboard(), parse_mode=ParseMode.HTML)
        return APPT_DATE

    if data == "appoint_back_mode":
        focus_set = set(str(k) for k in (appt.get("focus_keys") or APPOINTMENT_FOCUS_ORDER))
        await query.edit_message_text(
            _appointment_focus_prompt(appt.get("mode", "offline"), appt.get("listing_id", ""), focus_set),
            reply_markup=_appointment_focus_keyboard(focus_set),
            parse_mode=ParseMode.HTML,
        )
        return APPT_FOCUS

    if data.startswith("apdate:"):
        appt["date"] = data.split(":", 1)[1]
        text = f"第四步：已选日期 {appt['date']}。\n请选择时间段："
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("上午 (9:00-12:00)", callback_data="aptime:am"),
                    InlineKeyboardButton("下午 (14:00-18:00)", callback_data="aptime:pm"),
                ],
                [
                    InlineKeyboardButton("⬅️ 返回上一步", callback_data="appoint_back_date"),
                    InlineKeyboardButton("🏠 返回首页", callback_data="home"),
                ],
            ]
        )
        await query.edit_message_text(text, reply_markup=keyboard)
        return APPT_TIME

    if data == "appoint_back_date":
        query.data = "apmode:" + appt.get("mode", "offline")
        return await appoint_flow_cb(update, context)

    if data.startswith("aptime:"):
        appt["time"] = data.split(":", 1)[1]
        time_str = APPOINTMENT_TIME_LABELS.get(appt["time"], appt["time"])
        mode_str = APPOINTMENT_MODE_LABELS.get(appt.get("mode"), str(appt.get("mode") or "-"))
        focus_lines = _focus_summary_lines(appt.get("focus_keys") or APPOINTMENT_FOCUS_ORDER)
        text = (
            f"第五步：请确认预约信息\n\n"
            f"🏠 房源编号：<code>{he(appt.get('listing_id', ''))}</code>\n"
            f"🛠 看房方式：{mode_str}\n"
            f"📅 预约时间：{appt.get('date', '')} {time_str}\n"
            f"🔎 关注点：\n{focus_lines}\n\n"
            "确认后，中文顾问会尽快联系你。"
        )
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("✅ 确认提交", callback_data="apconfirm:yes")],
                [
                    InlineKeyboardButton("⬅️ 返回上一步", callback_data="appoint_back_time"),
                    InlineKeyboardButton("❌ 取消", callback_data="home"),
                ],
            ]
        )
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        return APPT_CONFIRM

    if data == "appoint_back_time":
        query.data = "apdate:" + appt.get("date", "")
        return await appoint_flow_cb(update, context)

    if data == "apconfirm:yes":
        lid_submit = str(appt.get("listing_id") or "").strip()
        if not appt.get("date") or not appt.get("time"):
            context.user_data.pop("appt", None)
            await query.edit_message_text(
                "预约信息不完整或已过期。\n请从频道「预约看房」或首页「📅 预约看房」重新发起。",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 返回首页", callback_data="home")]]),
                parse_mode=ParseMode.HTML,
            )
            return MAIN
        user = update.effective_user
        time_value = "am" if appt.get("time") == "am" else "pm"
        time_label = APPOINTMENT_TIME_LABELS.get(time_value, time_value)
        mode_label = APPOINTMENT_MODE_LABELS.get(appt.get("mode"), str(appt.get("mode") or "-"))
        focus_keys = [k for k in APPOINTMENT_FOCUS_ORDER if k in set(appt.get("focus_keys") or APPOINTMENT_FOCUS_ORDER)]
        focus_labels = [APPOINTMENT_FOCUS_LABELS[k] for k in focus_keys]
        focus_text = "；".join(focus_labels)
        db.create_appointment(
            {
                "user_id": user.id,
                "username": getattr(user, "username", "") or "",
                "display_name": user_display_name(user),
                "listing_id": appt.get("listing_id", "") if appt.get("listing_id") else "待推荐",
                "viewing_mode": appt.get("mode", ""),
                "appointment_date": appt.get("date", ""),
                "appointment_time": time_value,
                "contact_value": f"@{user.username}" if getattr(user, "username", "") else str(user.id),
                "note": f"关注点：{focus_text}" if focus_text else "",
                "status": "pending",
                "created_at": now_ts(),
            }
        )
        create_lead(
            user,
            action="appointment_submit",
            source=appt.get("source", "user_bot"),
            listing_id=appt.get("listing_id", "") if appt.get("listing_id") else "待推荐",
            payload={
                "viewing_mode": appt.get("mode", ""),
                "appointment_date": appt.get("date", ""),
                "appointment_time": time_value,
                "focus_keys": focus_keys,
                "focus_labels": focus_labels,
                **(appt.get("touch_payload") or {}),
            },
        )

        await _notify_admins(
            context,
            title="新预约提醒",
            lines=[
                f"用户：{_user_mention_html(user)}",
                f"联系方式：{he(_user_contact_text(user))}",
                f"房源：{he(appt.get('listing_id', '') or '-')}",
                f"方式：{he(mode_label)}",
                f"时间：{he(appt.get('date', '') or '-')} {he(time_label)}",
                f"关注点：{he(focus_text or '默认全项')}",
                f"来源：{he(str(appt.get('source', 'user_bot')))}",
            ],
        )

        context.user_data.pop("appt", None)
        await query.edit_message_text(
            "✅ <b>预约已提交</b>\n\n"
            "管理号已收到你的预约，会尽快确认具体时间。\n\n"
            + lead_capture_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=lead_capture_keyboard(),
        )
        return MAIN

    if data == "home":
        clear_session_for_fresh_entry(context)
        await render_panel(
            update,
            text=welcome_text(),
            reply_markup=main_keyboard(),
            parse_mode=ParseMode.HTML,
            context=context,
        )
        return MAIN

    return MAIN


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    clear_session_for_fresh_entry(context)
    await update.effective_message.reply_text("❌ 已取消当前操作。", reply_markup=main_keyboard())
    return MAIN


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("user_bot handler error: %s", context.error)


_MAIN_CB_PATTERN = r"^(home|hub:|resume:|unavail:|findmode:|findtype:|findarea:|findbudget:|findback:area|roompick:|appointment_menu:|service:|service_request:|service_slot:|pref:|profile:|contract:|lead_capture:|local:|rfcity:)"


def build_application() -> Application:
    app = Application.builder().token(USER_BOT_TOKEN).build()
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CommandHandler("find", cmd_find),
            CommandHandler("favorites", cmd_favorites),
            CommandHandler("appointments", cmd_appointments),
            CommandHandler("help", cmd_help),
            CommandHandler("contact", cmd_contact),
            CommandHandler("deal_done", cmd_deal_done),
            CommandHandler("lead_response", cmd_lead_response),
            CommandHandler("push_local", cmd_push_local),
            CommandHandler("push_all", cmd_push_all),
            CallbackQueryHandler(appoint_flow_cb, pattern="^apmode:"),
        ],
        states={
            MAIN: [
                CallbackQueryHandler(handle_ui_callback, pattern=_MAIN_CB_PATTERN),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_main_message),
            ],
            FIND_AREA: [
                CallbackQueryHandler(handle_ui_callback, pattern=_MAIN_CB_PATTERN),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_find_area),
            ],
            FIND_BUDGET: [
                CallbackQueryHandler(handle_ui_callback, pattern=_MAIN_CB_PATTERN),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_find_budget),
            ],
            APPT_MODE: [CallbackQueryHandler(appoint_flow_cb, pattern="^(apmode:|home)")],
            APPT_FOCUS: [CallbackQueryHandler(appoint_flow_cb, pattern="^(apfocus:|home)")],
            APPT_DATE: [
                CallbackQueryHandler(
                    appoint_flow_cb,
                    pattern="^(apdate:|appoint_back_mode|home)",
                )
            ],
            APPT_TIME: [
                CallbackQueryHandler(
                    appoint_flow_cb,
                    pattern="^(aptime:|appoint_back_date|home)",
                )
            ],
            APPT_CONFIRM: [
                CallbackQueryHandler(
                    appoint_flow_cb,
                    pattern="^(apconfirm:|appoint_back_time|home)",
                )
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CommandHandler("start", start),
            CommandHandler("find", cmd_find),
            CommandHandler("favorites", cmd_favorites),
            CommandHandler("appointments", cmd_appointments),
            CommandHandler("help", cmd_help),
            CommandHandler("contact", cmd_contact),
            CommandHandler("deal_done", cmd_deal_done),
            CommandHandler("lead_response", cmd_lead_response),
            CommandHandler("push_local", cmd_push_local),
            CommandHandler("push_all", cmd_push_all),
        ],
        allow_reentry=True,
    )
    app.add_handler(conv_handler)
    app.add_error_handler(error_handler)
    if app.job_queue is not None:
        app.job_queue.run_daily(
            lease_reminder_job,
            time=dt_time(hour=9, minute=5),
            name="lease_reminder_job",
        )
        app.job_queue.run_daily(
            rent_day_reminder_job,
            time=dt_time(hour=9, minute=10),
            name="rent_day_reminder_job",
        )
    else:
        logger.warning("job_queue 不可用：租约到期提醒任务未启动")
    return app


def main() -> None:
    app = build_application()
    logger.info("用户服务 Bot 启动中")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()

"""
侨联地产 · 找房助手 Bot v6.0
功能：找房推荐卡片 / 预约看房分步 / 全按钮化发布 / 讨论组跟帖 / 中介分配
python-telegram-bot >= 20.x (async)
"""
import os
import json
import logging
import asyncio
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

_BOT_DIR = Path(__file__).resolve().parent
load_dotenv(_BOT_DIR / ".env")

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    InputMediaPhoto,
)
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, filters, ContextTypes,
)
try:
    from sheets_handler import save_lead
except Exception:
    def save_lead(lead): pass

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════
# 基础配置
# ══════════════════════════════════════════════
BOT_TOKEN       = os.getenv("USER_BOT_TOKEN", os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE"))
ADMIN_TG_ID     = int(os.getenv("ADMIN_TG_ID", os.getenv("ADMIN_IDS", "0").split(",")[0]))
CHANNEL_URL     = os.getenv("CHANNEL_URL", "https://t.me/Jinbianzufanz")
CHANNEL_ID      = os.getenv("CHANNEL_ID", "@Jinbianzufanz")
BOT_USERNAME    = os.getenv("USER_BOT_USERNAME", os.getenv("BOT_USERNAME", "Meihua666bot"))
_DATA_DIR       = os.getenv("DATA_DIR", "./data")
LEADS_FILE      = os.getenv("LEADS_FILE",    os.path.join(_DATA_DIR, "leads.json"))
SUBS_FILE       = os.getenv("SUBS_FILE",     os.path.join(_DATA_DIR, "subscribers.json"))
LISTINGS_FILE   = os.getenv("LISTINGS_FILE", os.path.join(_DATA_DIR, "listings.json"))
APPOINTS_FILE   = os.path.join(_DATA_DIR, "appointments.json")
CONFIG_FILE     = os.path.join(_DATA_DIR, "bot_config.json")

# 讨论组 chat_id（关联讨论组后填入，留空则不自动跟帖）
DISCUSSION_ID   = os.getenv("DISCUSSION_ID", "")


def bot_tme_username() -> str:
    """用于 t.me 深链的 @ 后用户名，不含 @。"""
    return (BOT_USERNAME or "bot").strip().lstrip("@")


def md_safe(s) -> str:
    """用户输入插入 Markdown 时做简单脱敏，避免 *_ 等打断格式。"""
    if s is None:
        return ""
    return (
        str(s)
        .replace("\\", " ")
        .replace("_", " ")
        .replace("*", " ")
        .replace("[", "(")
        .replace("`", "'")
    )

# ══════════════════════════════════════════════
# 配置管理（品牌名 + 4个中介位置）
# ══════════════════════════════════════════════
DEFAULT_CONFIG = {
    "brand": "侨联地产",
    "agents": [
        {"name": "小彭", "tg": "@pengqingw", "wechat": "pengqingw"},
        {"name": "", "tg": "", "wechat": ""},
        {"name": "", "tg": "", "wechat": ""},
        {"name": "", "tg": "", "wechat": ""},
    ],
    "agent_index": 0,  # 轮询分配指针
}

_config_cache = None  # type: Optional[dict]
_config_mtime = None  # type: Optional[float]

def load_config() -> dict:
    global _config_cache, _config_mtime
    if not os.path.exists(CONFIG_FILE):
        _config_cache = None
        _config_mtime = None
        return DEFAULT_CONFIG.copy()
    try:
        mtime = os.path.getmtime(CONFIG_FILE)
    except OSError:
        return DEFAULT_CONFIG.copy()
    if _config_cache is not None and _config_mtime == mtime:
        return _config_cache
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        for k, v in DEFAULT_CONFIG.items():
            if k not in cfg:
                cfg[k] = v
        _config_cache = cfg
        _config_mtime = mtime
        return cfg
    except Exception:
        pass
    return DEFAULT_CONFIG.copy()

def save_config(cfg: dict):
    global _config_cache, _config_mtime
    _config_cache = None
    _config_mtime = None
    os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

def get_brand() -> str:
    return load_config().get("brand", "侨联地产")

def get_active_agents() -> list:
    """返回已配置的中介列表（name不为空）"""
    agents = load_config().get("agents", DEFAULT_CONFIG["agents"])
    return [a for a in agents if a.get("name")]

def assign_agent() -> dict:
    """轮询分配中介，返回中介信息"""
    cfg = load_config()
    active = [a for a in cfg.get("agents", []) if a.get("name")]
    if not active:
        return {"name": "小彭", "tg": "@pengqingw", "wechat": "pengqingw"}
    idx = cfg.get("agent_index", 0) % len(active)
    agent = active[idx]
    cfg["agent_index"] = (idx + 1) % len(active)
    save_config(cfg)
    return agent

# ══════════════════════════════════════════════
# STATES
# ══════════════════════════════════════════════
(
    MAIN_MENU,
    FIND_AREA,
    FIND_BUDGET,
    MAINT_TYPE,
    NEARBY_AREA,
    NEARBY_CAT,
    # 预约看房
    APPOINT_LISTING,
    APPOINT_DATE,
    APPOINT_TIME,
    APPOINT_CONFIRM,
    # 入住服务子菜单
    SERVICE_MENU,
    # 远程实拍预约流程
    VIDEO_LISTING,
    VIDEO_DATE,
    VIDEO_TIME,
    VIDEO_NOTE,
    VIDEO_CONFIRM,
    # 管理员发布（分步）
    ADMIN_PHOTOS,
    ADMIN_TYPE,
    ADMIN_AREA,
    ADMIN_ROOM,
    ADMIN_PRICE,
    ADMIN_DEPOSIT,
    ADMIN_FEATURES,
    ADMIN_MOVEIN,
    ADMIN_PROS,
    ADMIN_CONS,
    ADMIN_COMMENT,
    ADMIN_CONFIRM,
) = range(28)

APPOINT_MODE = 28  # reserved state (not active)

TEXT = filters.TEXT & ~filters.COMMAND

# ══════════════════════════════════════════════
# 数据工具
# ══════════════════════════════════════════════
def load_json_file(path: str) -> list:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return []

_listings_cache = None  # type: Optional[list]
_listings_mtime = None  # type: Optional[float]


def invalidate_listings_cache():
    global _listings_cache, _listings_mtime
    _listings_cache = None
    _listings_mtime = None


def save_json_file(path: str, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    try:
        if os.path.abspath(path) == os.path.abspath(LISTINGS_FILE):
            invalidate_listings_cache()
    except (OSError, ValueError, TypeError):
        pass

def save_lead_local(lead: dict):
    leads = load_json_file(LEADS_FILE)
    leads.append(lead)
    save_json_file(LEADS_FILE, leads)
    try:
        save_lead(lead)
    except Exception:
        pass


async def _save_lead_in_thread(lead: dict) -> None:
    try:
        await asyncio.to_thread(save_lead_local, lead)
    except Exception:
        logger.exception("后台保存线索失败")


async def _persist_find_lead_notify(
    context: ContextTypes.DEFAULT_TYPE,
    lead: dict,
    user_name: str,
    tg: str,
    find_type: str,
    area: str,
    budget_label: str,
) -> None:
    await _save_lead_in_thread(lead)
    if not ADMIN_TG_ID:
        return
    try:
        await context.bot.send_message(
            chat_id=ADMIN_TG_ID,
            text=(
                f"🔔 *新客户线索*\n\n"
                f"👤 {md_safe(user_name)}  {tg}\n"
                f"🏠 {find_type} | {md_safe(area)} | {budget_label}\n"
                f"🕐 {lead['timestamp']}"
            ),
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"通知管理员失败: {e}")


async def _persist_like_lead_notify(
    context: ContextTypes.DEFAULT_TYPE,
    update: Update,
    lid: str,
) -> None:
    user_name = get_user_name(update)
    tg = get_user_tg(update)
    title = ""
    if lid:
        listings = await asyncio.to_thread(load_listings)
        item = next((x for x in listings if x.get("listing_id") == lid), None)
        if item:
            title = f"{item.get('community', '')} {item.get('room_type', '')}".strip()
    lead = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "name": user_name,
        "tg": tg,
        "user_id": update.effective_user.id,
        "action": "喜欢房源",
        "listing_id": lid or "未知",
        "listing_title": title,
        "source": "TG Bot",
    }
    await _save_lead_in_thread(lead)
    if not ADMIN_TG_ID:
        return
    try:
        await context.bot.send_message(
            chat_id=ADMIN_TG_ID,
            text=(
                f"❤️ *用户喜欢房源*\n\n"
                f"编号：`{md_safe(lid) or '—'}`\n"
                f"房源：{md_safe(title) or '—'}\n"
                f"👤 {md_safe(user_name)}  {tg}\n"
                f"🕐 {lead['timestamp']}"
            ),
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"通知管理员失败: {e}")


async def _save_appt_and_notify_admin(
    context: ContextTypes.DEFAULT_TYPE,
    appt: dict,
    listing: str,
    date_val: str,
    time_val: str,
    contact: str,
    user_name: str,
    tg: str,
) -> None:
    try:
        await asyncio.to_thread(save_appointment, appt)
    except Exception:
        logger.exception("保存预约失败")
    if not ADMIN_TG_ID:
        return
    mode = appt.get("mode", "现场看房")
    mode_icon = "🚗" if mode == "现场看房" else "📹"
    video_note = "\n📹 *视频看房* — 需安排业务员到现场实时带看" if mode == "视频看房" else ""
    try:
        await context.bot.send_message(
            chat_id=ADMIN_TG_ID,
            text=(
                f"📅 *新看房预约*\n\n"
                f"🏠 房源：{md_safe(listing)}\n"
                f"{mode_icon} 方式：{md_safe(mode)}{video_note}\n"
                f"📅 日期：{md_safe(date_val)}  ⏰ 时间：{md_safe(time_val)}\n"
                f"📞 联系：{md_safe(contact)}\n"
                f"👤 {md_safe(user_name)}  {tg}\n"
                f"🕐 {appt['timestamp']}"
            ),
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"通知管理员失败: {e}")


def save_appointment(appt: dict):
    appts = load_json_file(APPOINTS_FILE)
    appts.append(appt)
    save_json_file(APPOINTS_FILE, appts)

def load_listings() -> list:
    global _listings_cache, _listings_mtime
    if not os.path.exists(LISTINGS_FILE):
        _listings_cache = []
        _listings_mtime = 0.0
        return []
    try:
        mtime = os.path.getmtime(LISTINGS_FILE)
    except OSError:
        return list(_listings_cache or [])
    if _listings_cache is not None and _listings_mtime == mtime:
        return _listings_cache
    data = load_json_file(LISTINGS_FILE)
    _listings_cache = data
    _listings_mtime = mtime
    return data

def load_subscribers() -> list:
    return load_json_file(SUBS_FILE)

def save_subscribers(subs: list):
    save_json_file(SUBS_FILE, subs)

def add_subscriber(user_id: int, user_name: str, tg: str):
    subs = load_subscribers()
    if user_id not in [s["user_id"] for s in subs]:
        subs.append({
            "user_id": user_id, "name": user_name, "tg": tg,
            "subscribed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        save_subscribers(subs)
        return True
    return False

def remove_subscriber(user_id: int):
    subs = [s for s in load_subscribers() if s["user_id"] != user_id]
    save_subscribers(subs)

def get_user_tg(update: Update) -> str:
    user = update.effective_user
    return f"@{user.username}" if user.username else f"tg://user?id={user.id}"

def get_user_name(update: Update) -> str:
    user = update.effective_user
    name = (user.first_name or "") + (" " + user.last_name if user.last_name else "")
    return name.strip() or "朋友"

def is_admin(update: Update) -> bool:
    return update.effective_user.id == ADMIN_TG_ID

# ══════════════════════════════════════════════
# 房源匹配（从listings.json读取真实数据）
# ══════════════════════════════════════════════
BUDGET_RANGES = {
    "300":  (0,    300),
    "500":  (300,  500),
    "800":  (500,  800),
    "1200": (800,  1200),
    "high": (1200, 99999),
    "1500": (800,  1500),
    "2500": (1500, 2500),
}

BUDGET_LABELS = {
    "300": "$300以下", "500": "$300-500", "800": "$500-800",
    "1200": "$800-1200", "high": "$1200以上",
    "1500": "$800-1500", "2500": "$1500-2500",
}

def match_listings(find_type: str, area: str, budget_key: str, limit: int = 3) -> list:
    """从listings.json中匹配房源，返回最多limit条"""
    listings = load_listings()
    if not listings:
        return []
    lo, hi = BUDGET_RANGES.get(budget_key, (0, 99999))
    results = []
    for item in listings:
        if item.get("status") not in ("上架", "在租", ""):
            continue
        # 类型匹配
        pt = item.get("prop_type", "住宅")
        if find_type == "别墅" and pt not in ("别墅",):
            continue
        if find_type == "住宅" and pt not in ("住宅", ""):
            continue
        # 区域匹配（模糊）
        item_area = item.get("area", "")
        if area and area != "其他" and area not in item_area and item_area not in area:
            continue
        # 预算匹配
        try:
            price = float(str(item.get("price", "0")).replace(",", ""))
            if not (lo <= price <= hi):
                continue
        except Exception:
            pass
        results.append(item)
    return results[:limit]

def format_listing_card(item: dict, idx: int) -> str:
    """格式化单条房源卡片文字"""
    prop_icons = {"住宅": "🏠", "别墅": "🏡", "办公室": "🏢", "商铺": "🏪"}
    icon = prop_icons.get(item.get("prop_type", "住宅"), "🏠")
    lid = item.get("listing_id", "")
    lines = [
        f"{icon} *{item.get('community', '未命名')}*",
        f"📍 {item.get('area', '')}  💰 *${item.get('price', '')}*/月  🏠 {item.get('room_type', '')}",
    ]
    if item.get("size"):
        lines.append(f"📐 {item['size']}㎡")
    if item.get("features"):
        lines.append(f"✨ {item['features']}")
    if item.get("pros"):
        lines.append(f"✅ {item['pros']}")
    if item.get("cons"):
        lines.append(f"⚠️ {item['cons']}")
    if item.get("comment"):
        lines.append(f"💬 侨联说：{item['comment']}")
    return "\n".join(lines), lid


# ══════════════════════════════════════════════
# 统一底部按钮（所有用户页面共用）
# ══════════════════════════════════════════════
def kb_footer(extra_rows: list = None):
    """返回统一的底部三按钮行，可在前面追加额外按钮行"""
    footer = [
        [
            InlineKeyboardButton("📅 预约办理", callback_data="appoint_"),
            InlineKeyboardButton("💎 咨询顾问", callback_data="menu_human"),
            InlineKeyboardButton("🏠 返回首页", callback_data="back_menu"),
        ]
    ]
    if extra_rows:
        return InlineKeyboardMarkup(extra_rows + footer)
    return InlineKeyboardMarkup(footer)

# ══════════════════════════════════════════════
# 键盘布局
# ══════════════════════════════════════════════
def kb_main():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 智能找房",    callback_data="find_residential"),
         InlineKeyboardButton("📹 远程实拍",    callback_data="menu_video")],
        [InlineKeyboardButton("🔒 服务承诺",    callback_data="menu_promise"),
         InlineKeyboardButton("🏠 租后管家",    callback_data="menu_service")],
        [InlineKeyboardButton("💌 关于侨联",    callback_data="menu_about"),
         InlineKeyboardButton("📞 联系专业顾问", callback_data="menu_human")],
    ])

def kb_res_area():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏙 富力城",    callback_data="area_富力城"),
         InlineKeyboardButton("🌆 炳发城",    callback_data="area_炳发城")],
        [InlineKeyboardButton("🌟 太子/幸福", callback_data="area_太子"),
         InlineKeyboardButton("📍 BKK1",      callback_data="area_BKK1")],
        [InlineKeyboardButton("🗺 TK/7月区",  callback_data="area_TK"),
         InlineKeyboardButton("💎 钻石岛",    callback_data="area_钻石岛")],
        [InlineKeyboardButton("🔍 其他区域",  callback_data="area_其他")],
        [InlineKeyboardButton("« 返回主菜单", callback_data="back_menu")],
    ])

def kb_villa_area():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌆 炳发城/集茂", callback_data="area_炳发城"),
         InlineKeyboardButton("🛣 洪森大道",    callback_data="area_洪森大道")],
        [InlineKeyboardButton("🔍 其他区域",    callback_data="area_其他")],
        [InlineKeyboardButton("« 返回主菜单",   callback_data="back_menu")],
    ])

def kb_budget_res():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("$300 以下",    callback_data="budget_300")],
        [InlineKeyboardButton("$300 – $500",  callback_data="budget_500")],
        [InlineKeyboardButton("$500 – $800",  callback_data="budget_800")],
        [InlineKeyboardButton("$800 – $1200", callback_data="budget_1200")],
        [InlineKeyboardButton("$1200 以上",   callback_data="budget_high")],
        [InlineKeyboardButton("« 返回",       callback_data="back_find_area"),
         InlineKeyboardButton("🏠 主菜单",    callback_data="back_menu")],
    ])

def kb_budget_villa():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("$800 – $1500",  callback_data="budget_1500")],
        [InlineKeyboardButton("$1500 – $2500", callback_data="budget_2500")],
        [InlineKeyboardButton("$2500 以上",    callback_data="budget_high")],
        [InlineKeyboardButton("« 返回",        callback_data="back_find_area"),
         InlineKeyboardButton("🏠 主菜单",     callback_data="back_menu")],
    ])

def kb_listing_card(lid: str):
    """单条房源卡片按钮"""
    return [
        InlineKeyboardButton("📅 预约看房", callback_data=f"appoint_{lid}"),
        InlineKeyboardButton("❤️ 喜欢",    callback_data=f"like_{lid}"),
    ]

def kb_after_listings():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 去频道看更多", url=CHANNEL_URL)],
        [InlineKeyboardButton("🔄 重新找房",     callback_data="back_menu"),
         InlineKeyboardButton("🏠 主菜单",       callback_data="back_menu")],
    ])

def kb_maint_type():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❄️ 空调不制冷", callback_data="maint_空调"),
         InlineKeyboardButton("🚿 漏水/堵水",  callback_data="maint_水管")],
        [InlineKeyboardButton("💡 电路问题",   callback_data="maint_电路"),
         InlineKeyboardButton("🚪 门窗问题",   callback_data="maint_门窗")],
        [InlineKeyboardButton("📦 家具损坏",   callback_data="maint_家具"),
         InlineKeyboardButton("🔧 其他问题",   callback_data="maint_其他")],
        [InlineKeyboardButton("« 返回主菜单",  callback_data="back_menu")],
    ])

def kb_nearby_area():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏙️ 富力城",    callback_data="nearby_富力城")],
        [InlineKeyboardButton("🌆 炳发城",    callback_data="nearby_炳发城"),
         InlineKeyboardButton("🌟 太子/幸福", callback_data="nearby_太子")],
        [InlineKeyboardButton("📍 BKK1",      callback_data="nearby_BKK1"),
         InlineKeyboardButton("🔍 其他区域",  callback_data="nearby_其他")],
        [InlineKeyboardButton("« 返回主菜单", callback_data="back_menu")],
    ])

def kb_nearby_cat():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🍴 吃饭",   callback_data="ncat_餐厅小吃"),
         InlineKeyboardButton("🛒 购物",   callback_data="ncat_超市便利")],
        [InlineKeyboardButton("🥤 饮品",   callback_data="ncat_奶茶饮品"),
         InlineKeyboardButton("🚛 快递",   callback_data="ncat_快递")],
        [InlineKeyboardButton("🔥 烧烤",   callback_data="ncat_烧烤"),
         InlineKeyboardButton("💆 休闲",   callback_data="ncat_休闲娱乐")],
        [InlineKeyboardButton("« 返回区域", callback_data="menu_nearby"),
         InlineKeyboardButton("🏠 主菜单",  callback_data="back_menu")],
    ])

# ══════════════════════════════════════════════
# 商家数据
# ══════════════════════════════════════════════
COMMUNITY_DATA = {
    "餐厅小吃": [
        "小明菜煎饼 @XMCaiJianBing", "金饭碗融合食 @JFW_8888", "木森快餐 @hei32567",
        "邻居家盒饭 @linjujia8899", "麻了个面 @fq666520", "兰州拉面 @LZLM_RF",
        "重庆小面 @CY_ccxm", "味之道重庆小面 @WZD8889", "川妹子餐厅 @cuan_meizi",
        "太二酸菜鱼 @taiersuancaiyu", "幺妹麻辣烫 @Ruilin585", "沙县小吃 @cheng1149",
        "猪事顺杀猪粉 @zssflzd", "麦德仕汉堡炸鸡 @MDS0188838388",
        "小仙女手工凉皮 @ba521520", "鼎阁重庆老火锅 @xh918888", "麻小姬·麻椒鸡 @Wwen52025",
    ],
    "烧烤": [
        "留一手烤鱼 @clgxyxy", "东北吉林烧烤 @Jinniu99998888",
        "江湖烧烤 @jianghushaokao", "A8烤鹅翅 @FUAN68899",
    ],
    "奶茶饮品": [
        "麦诺咖啡 @mnppsc", "霸王茶姬 @Jolyne777", "A8ManMan糖水饮品 @manmanC3121",
    ],
    "超市便利": [
        "喜来优品超市 @xilai1818", "够意思超市 @gouyisi", "文轩888便利店 @WENXUAN188",
        "富田生鲜超市 @FUTIAN668899", "糖巢省钱超市 @WGTC99",
        "叮当猫百货 @yuna666666", "如意烟酒 @w1025", "1919商行 @FL191919",
        "庆丰优选超市 @gtffgfffdff", "B11世纪超市 @b11shijichaoshi",
    ],
    "休闲娱乐": [
        "富力体育会所 @Sportcity1098", "泰自然按摩 @taiziran01",
        "茜茜美容SPA @d11631876", "安妮奢侈品回收 @anne168777",
        "宠物之家 @motopet188", "奢依阁男装 @SYG666888",
    ],
    "快递": [
        "YA速递富力站 @yaexpres", "CE速递 @CECS006", "中通快递 +85566666280",
    ],
}

# ══════════════════════════════════════════════
# 主菜单 / start
# ══════════════════════════════════════════════
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_name = get_user_name(update)
    brand = get_brand()
    text = (
        f"💌 *{brand}｜金边华人资产管家*\n\n"
        f"您好！欢迎来到{brand}智能服务中心。\n\n"
        f"我们在金边深耕 6 年，不只是房产中介，\n"
        f"更是您在柬生活的可靠后盾。\n\n"
        f"为您提供专业、透明、高效的一站式服务：\n"
        f"🏠 *租赁* · 🏡 *置业* · 🔒 *资产托管*\n\n"
        f"──────────────\n"
        f"✨ *请选择您需要的服务*"
    )
    if update.message:
        await update.message.reply_text(text, reply_markup=kb_main(), parse_mode="Markdown")
    else:
        await update.callback_query.edit_message_text(text, reply_markup=kb_main(), parse_mode="Markdown")
    return MAIN_MENU

async def main_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    return await start(update, context)


async def menu_help_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    brand = get_brand()
    text = (
        f"*{brand} · 使用说明*\n\n"
        "• 「我要找房」→ 选区域 + 预算，自动匹配房源\n"
        "• 看到心仪的房源可点 ❤️ 收藏，顾问会跟进\n"
        "• 「预约看房」→ 选日期时间，顾问主动联系您\n"
        "• 没时间去现场？可要求顾问上门拍视频\n"
        "• `/cancel` 可中断当前步骤\n\n"
        f"📢 房源频道：{CHANNEL_URL}\n\n"
        f"🏢 *{brand}*\n"
        f"📍 您在金边的自己人"
    )
    await q.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("« 返回主菜单", callback_data="back_menu")],
        ]),
        parse_mode="Markdown",
        disable_web_page_preview=True,
    )
    return MAIN_MENU


async def menu_subscribe_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "📰 *订阅资讯*\n\n"
        "在下方输入框发送：\n`/subscribe`\n\n"
        "取消订阅发送：\n`/unsubscribe`\n\n"
        "（订阅后管理员群发时你会收到消息）",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("« 返回主菜单", callback_data="back_menu")],
        ]),
        parse_mode="Markdown",
    )
    return MAIN_MENU


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """任意对话步骤中中断，回到主菜单。"""
    context.user_data.clear()
    await update.message.reply_text(
        "✅ 已取消当前操作。\n\n"
        "下面重新打开主菜单，或直接发 /start",
        reply_markup=kb_main(),
        parse_mode="Markdown",
    )
    return MAIN_MENU

# ══════════════════════════════════════════════
# 找房流程
# ══════════════════════════════════════════════
async def find_residential(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data["find_type"] = "住宅"
    await q.edit_message_text(
        "🏠 *条件找房 · 公寓*\n\n请选区域，再选预算（按上架数据匹配）：",
        reply_markup=kb_res_area(), parse_mode="Markdown"
    )
    return FIND_AREA

async def find_villa(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data["find_type"] = "别墅"
    await q.edit_message_text(
        "🏡 *条件找房 · 别墅*\n\n请选区域，再选预算：",
        reply_markup=kb_villa_area(), parse_mode="Markdown"
    )
    return FIND_AREA

async def find_area_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    area = q.data.replace("area_", "")
    context.user_data["area"] = area
    find_type = context.user_data.get("find_type", "住宅")
    kb = kb_budget_villa() if find_type == "别墅" else kb_budget_res()
    await q.edit_message_text(
        f"📍 区域：*{md_safe(area)}*\n\n预算范围（美元/月）：",
        reply_markup=kb, parse_mode="Markdown"
    )
    return FIND_BUDGET

async def back_find_area(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    find_type = context.user_data.get("find_type", "住宅")
    if find_type == "别墅":
        await q.edit_message_text(
            "🏡 *条件找房 · 别墅*\n\n请选区域，再选预算：",
            reply_markup=kb_villa_area(), parse_mode="Markdown",
        )
    else:
        await q.edit_message_text(
            "🏠 *条件找房 · 公寓*\n\n请选区域，再选预算：",
            reply_markup=kb_res_area(), parse_mode="Markdown",
        )
    return FIND_AREA

async def find_budget_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    budget_key = q.data.replace("budget_", "")
    context.user_data["budget"] = budget_key

    find_type = context.user_data.get("find_type", "住宅")
    area      = context.user_data.get("area", "其他")
    budget_label = BUDGET_LABELS.get(budget_key, budget_key)
    tg        = get_user_tg(update)
    user_name = get_user_name(update)
    cfg       = load_config()
    brand     = cfg.get("brand", "侨联地产")
    active_agents = [a for a in cfg.get("agents", []) if a.get("name")]

    # 从listings.json匹配真实房源
    matched = match_listings(find_type, area, budget_key, limit=3)

    if matched:
        # 把所有房源合并成一条消息，避免多次API调用超时
        prop_icons = {"住宅": "🏠", "别墅": "🏡", "排屋": "🏘", "独栋": "🏠",
                      "办公室": "🏢", "商铺": "🏪"}
        lines = [f"✅ *为你找到 {len(matched)} 套匹配房源*",
                 f"📍 {md_safe(area)}  💰 {budget_label}/月", ""]
        lids = []
        for i, item in enumerate(matched):
            icon = prop_icons.get(item.get("prop_type", "住宅"), "🏠")
            lid = item.get("listing_id", f"item{i}")
            lids.append(lid)
            lines.append(f"{icon} *{item.get('community', '未命名')}*")
            lines.append(f"📍 {item.get('area','')}  💰 *${item.get('price','')}*/月  🏠 {item.get('room_type','')}")
            if item.get('size'):
                lines.append(f"📐 {item['size']}㎡")
            if item.get('features'):
                lines.append(f"✨ {item['features']}")
            if item.get('pros'):
                lines.append(f"✅ {item['pros']}")
            if item.get('cons'):
                lines.append(f"⚠️ {item['cons']}")
            if item.get('comment'):
                lines.append(f"💬 侨联说：{item['comment']}")
            lines.append("")
        lines.append(f"──────────\n🏢 {brand}")
        lines.append(f"📢 更多房源：{CHANNEL_URL}")

        # 构建按钮：每套房源一行
        kb_rows = []
        for i, (item, lid) in enumerate(zip(matched, lids)):
            name = item.get('community', f'房源{i+1}')[:8]
            kb_rows.append([
                InlineKeyboardButton(f"📅 预约 {name}", callback_data=f"appoint_{lid}"),
                InlineKeyboardButton(f"❤️", callback_data=f"like_{lid}"),
            ])
        kb_rows.append([InlineKeyboardButton("📢 去频道看更多", url=CHANNEL_URL)])
        kb_rows.append([InlineKeyboardButton("🔄 重新找房", callback_data="back_menu")])

        await q.edit_message_text(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(kb_rows),
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
    else:
        # 没有匹配房源，引导去频道
        agent_lines = ""
        for a in active_agents:
            if a.get("tg"):
                agent_lines += f"👤 {a['name']}  {a['tg']}\n"
        msg = (
            f"✅ *收到，正在帮你留意。*\n\n"
            f"📍 区域：{md_safe(area)}  💰 {budget_label}/月\n\n"
            f"目前该区间暂无上架房源，我们会第一时间通知你。\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"📢 频道每日更新房源：{CHANNEL_URL}\n\n"
            f"🏢 {brand}\n"
            f"{agent_lines}"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 去频道看更多", url=CHANNEL_URL)],
            [InlineKeyboardButton("📅 预约看房",    callback_data="appoint_"),
             InlineKeyboardButton("🏠 主菜单",      callback_data="back_menu")],
        ])
        await q.edit_message_text(msg, reply_markup=kb, parse_mode="Markdown",
                                   disable_web_page_preview=True)

    # 保存线索 + 通知管理员放到后台，避免卡住界面与其它用户
    lead = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "name": user_name, "tg": tg,
        "user_id": update.effective_user.id,
        "type": find_type, "area": area,
        "budget": budget_label, "source": "TG Bot",
    }
    asyncio.create_task(
        _persist_find_lead_notify(
            context, lead, user_name, tg, find_type, area, budget_label,
        ),
        name="find_lead_persist",
    )
    return MAIN_MENU

async def like_listing_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    lid = q.data.replace("like_", "").strip()
    await q.answer("❤️ 已记下！顾问会帮你跟进这套房～", show_alert=False)
    asyncio.create_task(
        _persist_like_lead_notify(context, update, lid),
        name="like_lead_persist",
    )
    return MAIN_MENU

# ══════════════════════════════════════════════
# 预约看房流程
# ══════════════════════════════════════════════
async def appoint_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """触发预约：可从按钮或命令进入"""
    if update.callback_query:
        q = update.callback_query
        await q.answer()
        lid = q.data.replace("appoint_", "")
        # 如果有lid，预填房源
        if lid:
            listings = load_listings()
            item = next((x for x in listings if x.get("listing_id") == lid), None)
            if item:
                context.user_data["appoint_listing"] = f"{item.get('community','')} {item.get('room_type','')}"
        send = q.message.reply_text
    else:
        send = update.message.reply_text

    pre_listing = context.user_data.get("appoint_listing", "")
    if pre_listing:
        context.user_data["appoint_listing"] = pre_listing
        await send(
            f"📅 *预约看房*\n\n"
            f"已选房源：*{md_safe(pre_listing)}*\n\n"
            f"请选择看房日期：\n"
            f"（视频看房请在留联系方式时备注「视频」；发 /cancel 取消）",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("今天",   callback_data="apdate_今天"),
                 InlineKeyboardButton("明天",   callback_data="apdate_明天"),
                 InlineKeyboardButton("后天",   callback_data="apdate_后天")],
                [InlineKeyboardButton("其他日期（请输入）", callback_data="apdate_other")],
                [InlineKeyboardButton("« 取消", callback_data="back_menu")],
            ]),
            parse_mode="Markdown",
        )
        return APPOINT_DATE
    else:
        await send(
            "📅 *预约看房*\n\n请告诉我您想看的房源名称或地址\n"
            "（例如：富力城华府 2房）。\n\n"
            "支持实地或视频看房；若要 *视频*，下一步留联系方式时请备注「视频」。",
            parse_mode="Markdown",
        )
        return APPOINT_LISTING

async def appoint_listing_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["appoint_listing"] = update.message.text.strip()
    await update.message.reply_text(
        f"📍 房源：*{md_safe(context.user_data['appoint_listing'])}*\n\n请选择看房日期：\n（发 /cancel 可取消）",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("今天",   callback_data="apdate_今天"),
             InlineKeyboardButton("明天",   callback_data="apdate_明天"),
             InlineKeyboardButton("后天",   callback_data="apdate_后天")],
            [InlineKeyboardButton("其他日期（请输入）", callback_data="apdate_other")],
            [InlineKeyboardButton("« 取消", callback_data="back_menu")],
        ]),
        parse_mode="Markdown",
    )
    return APPOINT_DATE

async def appoint_back_to_date_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """选时间段页点「返回」→ 重新选日期。"""
    q = update.callback_query
    await q.answer()
    pre_listing = context.user_data.get("appoint_listing", "")
    if not pre_listing:
        await q.edit_message_text(
            "📅 *预约看房*\n\n请告诉我您想看的房源名称或地址\n"
            "（例如：富力城华府 2房）。\n\n"
            "支持实地或视频；留联系方式时可备注「视频」。\n\n发 /cancel 可取消",
            parse_mode="Markdown",
        )
        return APPOINT_LISTING
    await q.edit_message_text(
        f"📅 *预约看房*\n\n已选房源：*{md_safe(pre_listing)}*\n\n请选择看房日期：",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("今天",   callback_data="apdate_今天"),
             InlineKeyboardButton("明天",   callback_data="apdate_明天"),
             InlineKeyboardButton("后天",   callback_data="apdate_后天")],
            [InlineKeyboardButton("其他日期（请输入）", callback_data="apdate_other")],
            [InlineKeyboardButton("« 取消", callback_data="back_menu")],
        ]),
        parse_mode="Markdown",
    )
    return APPOINT_DATE


async def appoint_date_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    date_val = q.data.replace("apdate_", "")
    if date_val == "other":
        await q.edit_message_text(
            "请输入看房日期（例如：4月15日）：\n\n发 /cancel 可取消",
        )
        return APPOINT_DATE
    context.user_data["appoint_date"] = date_val
    await q.edit_message_text(
        f"📅 日期：*{md_safe(date_val)}*\n\n请选择时间段：\n（点「返回改日期」或发 /cancel）",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("上午 9:00-12:00",  callback_data="aptime_上午"),
             InlineKeyboardButton("下午 14:00-17:00", callback_data="aptime_下午")],
            [InlineKeyboardButton("傍晚 17:00-19:00", callback_data="aptime_傍晚"),
             InlineKeyboardButton("其他时间",         callback_data="aptime_其他")],
            [InlineKeyboardButton("« 返回改日期",     callback_data="back_apdate")],
        ]),
        parse_mode="Markdown",
    )
    return APPOINT_TIME

async def appoint_date_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["appoint_date"] = update.message.text.strip()
    await update.message.reply_text(
        f"📅 日期：*{md_safe(context.user_data['appoint_date'])}*\n\n请选择时间段：",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("上午 9:00-12:00",  callback_data="aptime_上午"),
             InlineKeyboardButton("下午 14:00-17:00", callback_data="aptime_下午")],
            [InlineKeyboardButton("傍晚 17:00-19:00", callback_data="aptime_傍晚"),
             InlineKeyboardButton("其他时间",         callback_data="aptime_其他")],
            [InlineKeyboardButton("« 返回改日期",     callback_data="back_apdate")],
        ]),
        parse_mode="Markdown",
    )
    return APPOINT_TIME

async def appoint_time_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    time_val = q.data.replace("aptime_", "")
    context.user_data["appoint_time"] = time_val
    # 直接进入确认，不再要求手动输入联系方式（TG自动识别）
    d = context.user_data
    listing  = d.get("appoint_listing", "未指定")
    date_val = d.get("appoint_date", "")
    brand    = get_brand()
    user_name = get_user_name(update)
    tg_id    = get_user_tg(update)
    await q.edit_message_text(
        f"📋 *请确认预约信息*\n\n"
        f"🏠 房源：{md_safe(listing)}\n"
        f"📅 日期：{md_safe(date_val)}\n"
        f"⏰ 时间：{md_safe(time_val)}\n"
        f"👤 联系：{md_safe(user_name)}  {tg_id}\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"🏢 *{brand}* · 您在金边的自己人",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ 确认提交", callback_data="apconfirm_yes")],
            [InlineKeyboardButton("« 返回改时间", callback_data="back_aptime"),
             InlineKeyboardButton("🏠 主菜单", callback_data="back_menu")],
        ]),
        parse_mode="Markdown",
    )
    return APPOINT_CONFIRM


async def appoint_back_to_time_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """确认页点「返回改时间」→ 重选时间段。"""
    q = update.callback_query
    await q.answer()
    date_val = context.user_data.get("appoint_date", "")
    await q.edit_message_text(
        f"📅 日期：*{md_safe(date_val)}*\n\n请选择时间段：\n（点「返回改日期」或发 /cancel）",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("上午 9:00-12:00",  callback_data="aptime_上午"),
             InlineKeyboardButton("下午 14:00-17:00", callback_data="aptime_下午")],
            [InlineKeyboardButton("傍晚 17:00-19:00", callback_data="aptime_傍晚"),
             InlineKeyboardButton("其他时间",         callback_data="aptime_其他")],
            [InlineKeyboardButton("« 返回改日期",     callback_data="back_apdate"),
             InlineKeyboardButton("🏠 主菜单",         callback_data="back_menu")],
        ]),
        parse_mode="Markdown",
    )
    return APPOINT_TIME


async def appoint_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    # 确认提交
    d = context.user_data
    user_name = get_user_name(update)
    tg        = get_user_tg(update)
    listing   = d.get("appoint_listing", "未指定")
    date_val  = d.get("appoint_date", "")
    time_val  = d.get("appoint_time", "")
    brand     = get_brand()

    appt = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "user_name": user_name, "tg": tg,
        "user_id": update.effective_user.id,
        "listing": listing, "date": date_val,
        "time": time_val, "contact": tg,  # 自动用TG身份
    }

    menu_url = f"https://t.me/{bot_tme_username()}?start=start"
    await q.edit_message_text(
        f"✅ *看房预约已提交！*\n\n"
        f"🏠 {md_safe(listing)}\n"
        f"📅 {md_safe(date_val)}  ⏰ {md_safe(time_val)}\n"
        f"👤 {md_safe(user_name)}  {tg}\n\n"
        f"正在为您分配专属顾问，稍后会主动联系您。\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"🏢 *{brand}* · 您在金边的自己人",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 返回主菜单", url=menu_url)],
            [InlineKeyboardButton("📢 去看频道房源", url=CHANNEL_URL)],
        ]),
    )

    asyncio.create_task(
        _save_appt_and_notify_admin(
            context, appt, listing, date_val, time_val, tg, user_name, tg,
        ),
        name="appt_persist",
    )
    return ConversationHandler.END

# ══════════════════════════════════════════════
# 周边配套
# ══════════════════════════════════════════════
async def nearby_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        q = update.callback_query
        await q.answer()
        await q.edit_message_text("📍 *周边配套*\n\n你在哪个小区附近？",
                                   reply_markup=kb_nearby_area(), parse_mode="Markdown")
    return NEARBY_AREA

async def nearby_area_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    area = q.data.replace("nearby_", "")
    if area == "富力城":
        await q.edit_message_text(
            f"🏙️ *富力城 · 周边配套*\n\n🔥 热门分类：",
            reply_markup=kb_nearby_cat(), parse_mode="Markdown"
        )
        return NEARBY_CAT
    else:
        agents = get_active_agents()
        agent_line = agents[0]["tg"] if agents else "@pengqingw"
        await q.edit_message_text(
            f"该区域商家正在整理中。\n有具体想找的店，直接联系我们，人工帮你查。\n\n"
            f"💬 {agent_line}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💬 联系我们", url=f"https://t.me/{agent_line.lstrip('@')}")],
                [InlineKeyboardButton("« 返回",      callback_data="menu_nearby"),
                 InlineKeyboardButton("🏠 主菜单",   callback_data="back_menu")],
            ]),
        )
        return NEARBY_AREA

async def nearby_cat_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    cat = q.data.replace("ncat_", "")
    items = COMMUNITY_DATA.get(cat, [])
    if not items:
        await q.answer("暂无数据", show_alert=True)
        return NEARBY_CAT
    text = f"*{cat}*\n\n" + "\n".join(f"• {x}" for x in items)
    text += "\n\n💡 点击 @ 用户名直接联系商家"
    await q.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("« 返回分类", callback_data="nearby_富力城"),
             InlineKeyboardButton("🏠 主菜单",  callback_data="back_menu")],
        ]),
        parse_mode="Markdown",
    )
    return NEARBY_CAT

# ══════════════════════════════# ══════════════════════════════
# 入住服务（子菜单）
# ══════════════════════════════
async def service_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """5星入住服务主菜单"""
    if update.callback_query:
        q = update.callback_query
        await q.answer()
    brand = get_brand()
    text = (
        f"⭐ *入住服务*\n\n"
        f"入住才是{brand}服务的开始。\n"
        f"我们在整个租期都陪着您。\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"🏢 *{brand}*\n"
        f"📍 您在金边的自己人"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔧 报修与维护", callback_data="menu_maintenance"),
         InlineKeyboardButton("🏢 物业沟通", callback_data="svc_property")],
        [InlineKeyboardButton("🗺️ 生活指南包", callback_data="svc_guide"),
         InlineKeyboardButton("🔁 续租/换房", callback_data="svc_renew")],
        [InlineKeyboardButton("« 返回主菜单", callback_data="back_menu")],
    ])
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=kb, parse_mode="Markdown")
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="Markdown")
    return SERVICE_MENU

async def maintenance_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """报修子菜单"""
    if update.callback_query:
        q = update.callback_query
        await q.answer()
        await q.edit_message_text(
            "🔧 *报修服务*\n\n房子遇到什么问题了？点选类型，我们承关1小时内响应。",
            reply_markup=kb_maint_type(), parse_mode="Markdown"
        )
    return MAINT_TYPE

async def maint_type_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    mtype = q.data.replace("maint_", "")
    agents = get_active_agents()
    agent_line = agents[0]["tg"] if agents else "@pengqingw"
    brand = get_brand()
    user_name = get_user_name(update)
    tg_id = get_user_tg(update)
    await q.edit_message_text(
        f"✅ *报修已收到！*\n\n"
        f"问题：{mtype}\n"
        f"👤 {md_safe(user_name)}  {tg_id}\n\n"
        f"⏱️ 承关1小时内响应\n"
        f"📞 很急的话，直接联系：{agent_line}\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"🏢 *{brand}* · 您在金边的自己人",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💬 直接联系顾问", url=f"https://t.me/{agent_line.lstrip('@')}")],
            [InlineKeyboardButton("« 返回入住服务", callback_data="menu_service"),
             InlineKeyboardButton("🏠 主菜单", callback_data="back_menu")],
        ]),
        parse_mode="Markdown",
    )
    if ADMIN_TG_ID:
        try:
            await context.bot.send_message(
                chat_id=ADMIN_TG_ID,
                text=f"🔧 *报修通知*\n\n问题：{mtype}\n👤 {user_name}  {tg_id}",
                parse_mode="Markdown",
            )
        except Exception:
            pass
    return MAIN_MENU

async def svc_property_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """\u7269\u4e1a\u6c9f\u901a"""
    q = update.callback_query
    await q.answer()
    agents = get_active_agents()
    agent_line = agents[0]["tg"] if agents else "@pengqingw"
    brand = get_brand()
    await q.edit_message_text(
        f"🏢 *物业沟通*\n\n"
        f"物业那边的事让我们来，不用您自己去跑。\n\n"
        f"✔ 帮您传达报修/投诉\n"
        f"✔ 关键情况我们会提前通知您\n"
        f"✔ 我们和物业关系打得牢靠，好办事\n\n"
        f"请直接联系顾问，描述您需要与物业沟通的具体问题。\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"🏢 *{brand}* · 您在金边的自己人",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📞 联系顾问跟进", url=f"https://t.me/{agent_line.lstrip('@')}")],
            [InlineKeyboardButton("« 返回入住服务", callback_data="menu_service"),
             InlineKeyboardButton("🏠 主菜单", callback_data="back_menu")],
        ]),
        parse_mode="Markdown",
    )
    return SERVICE_MENU

async def svc_guide_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """\u751f\u6d3b\u6307\u5357\u5305"""
    q = update.callback_query
    await q.answer()
    brand = get_brand()
    agents = get_active_agents()
    agent_line = agents[0]["tg"] if agents else "@pengqingw"
    await q.edit_message_text(
        f"🗺️ *金边生活指南*\n\n"
        f"刚到金边不知道怎么办？这里有你需要的。\n\n"
        f"💱 汇率参考（人民币/美元）\n"
        f"📑 签证办理指南\n"
        f"🚕 出行：Grab / PassApp 使用指南\n"
        f"📦 快递：中文快递地址列表\n"
        f"🍽️ 周边吃饯推荐\n"
        f"📞 紧急电话：实用号码整理\n\n"
        f"具体问题可直接问顾问，人工解答。\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"🏢 *{brand}* · 您在金边的自己人",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💬 问顾问", url=f"https://t.me/{agent_line.lstrip('@')}")],
            [InlineKeyboardButton("« 返回入住服务", callback_data="menu_service"),
             InlineKeyboardButton("🏠 主菜单", callback_data="back_menu")],
        ]),
        parse_mode="Markdown",
    )
    return SERVICE_MENU

async def svc_renew_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """\u7eed\u79df/\u6362\u623f"""
    q = update.callback_query
    await q.answer()
    brand = get_brand()
    agents = get_active_agents()
    agent_line = agents[0]["tg"] if agents else "@pengqingw"
    user_name = get_user_name(update)
    tg_id = get_user_tg(update)
    await q.edit_message_text(
        f"🔁 *续租 / 换房*\n\n"
        f"快到期了？或者想换个环境？\n"
        f"老客户续租我们优先安排，换房也可以帮您对比。\n\n"
        f"✔ 到期前30天提醒\n"
        f"✔ 续租谈判我们代劳\n"
        f"✔ 换房免中介费\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"🏢 *{brand}* · 您在金边的自己人",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📞 联系顾问", url=f"https://t.me/{agent_line.lstrip('@')}")],
            [InlineKeyboardButton("« 返回入住服务", callback_data="menu_service"),
             InlineKeyboardButton("🏠 主菜单", callback_data="back_menu")],
        ]),
        parse_mode="Markdown",
    )
    if ADMIN_TG_ID:
        try:
            await context.bot.send_message(
                chat_id=ADMIN_TG_ID,
                text=f"🔁 *续租/换房意向*\n\n👤 {user_name}  {tg_id}",
                parse_mode="Markdown",
            )
        except Exception:
            pass
    return SERVICE_MENU


# ══════════════════════════════════════════════
# 远程实拍预约流程（完整5步）
# ══════════════════════════════════════════════
async def menu_video_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 0: 入口 → 让用户输入房源名称"""
    q = update.callback_query
    await q.answer()
    context.user_data.clear()
    context.user_data["video_flow"] = True
    text = (
        "📹 *远程实时视频看房*\n\n"
        "没时间亲自到现场也没关系。\n\n"
        "我们会安排业务员前往房源现场，\n"
        "与您进行 *实时视频通话*，\n"
        "边走边看、实时解答您的问题。\n\n"
        "━━━━━━━━━━━━━━\n"
        "请告诉我您想看的房源\n"
        "（可直接复制房源名称）：\n\n"
        "_例如：_\n"
        "• 王府·观邸\n"
        "• 幸福广场大平层\n"
        "• 炳发城集茂独栋\n"
        "• L003"
    )
    kb = kb_footer()
    await q.edit_message_text(text, reply_markup=kb, parse_mode="Markdown")
    return VIDEO_LISTING


async def video_listing_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 1: 收到房源名 → 选日期"""
    listing = update.message.text.strip()
    context.user_data["video_listing"] = listing
    from datetime import date, timedelta
    today = date.today()
    dates = [today + timedelta(days=i) for i in range(7)]
    weekday_cn = ["周一","周二","周三","周四","周五","周六","周日"]
    def fmt(d):
        label = "今天" if d == today else ("明天" if d == today + timedelta(1) else
                ("后天" if d == today + timedelta(2) else
                 f"{d.month}/{d.day} {weekday_cn[d.weekday()]}"))
        return label, d.strftime("%Y-%m-%d")
    btns = []
    row = []
    for d in dates[:4]:
        lbl, val = fmt(d)
        row.append(InlineKeyboardButton(lbl, callback_data=f"vdate_{val}"))
    btns.append(row)
    row2 = []
    for d in dates[4:]:
        lbl, val = fmt(d)
        row2.append(InlineKeyboardButton(lbl, callback_data=f"vdate_{val}"))
    btns.append(row2)
    btns.append([InlineKeyboardButton("« 返回", callback_data="menu_video"),
                 InlineKeyboardButton("🏠 主菜单", callback_data="back_menu")])
    await update.message.reply_text(
        f"✅ *已收到您想看的房源：*\n\n"
        f"🏠 *{md_safe(listing)}*\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"请选择您方便的视频看房日期：",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
    )
    return VIDEO_DATE


async def video_date_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 2: 选完日期 → 选时间段"""
    q = update.callback_query
    await q.answer()
    date_val = q.data.replace("vdate_", "")
    context.user_data["video_date"] = date_val
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🌅 上午 09:00-12:00", callback_data="vtime_上午09:00-12:00")],
        [InlineKeyboardButton("☀️ 下午 14:00-17:00", callback_data="vtime_下午14:00-17:00")],
        [InlineKeyboardButton("🌆 傍晚 17:00-19:00", callback_data="vtime_傍晚17:00-19:00")],
        [InlineKeyboardButton("« 返回", callback_data="vback_date"),
         InlineKeyboardButton("🏠 主菜单", callback_data="back_menu")],
    ])
    await q.edit_message_text(
        f"📅 日期：*{date_val}*\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"请选择大致时间段\n"
        f"（业务员将尽量匹配）：",
        reply_markup=kb,
        parse_mode="Markdown",
    )
    return VIDEO_TIME


async def video_time_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 3: 选完时间 → 填备注（可选）"""
    q = update.callback_query
    await q.answer()
    time_val = q.data.replace("vtime_", "")
    context.user_data["video_time"] = time_val
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭️ 跳过，直接确认", callback_data="vnote_skip")],
        [InlineKeyboardButton("« 返回", callback_data="vback_time"),
         InlineKeyboardButton("🏠 主菜单", callback_data="back_menu")],
    ])
    await q.edit_message_text(
        f"📅 日期：*{context.user_data['video_date']}*\n"
        f"⏰ 时间：*{time_val}*\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"您可以在这里备注最想重点关注的细节\n"
        f"（可选，直接发文字即可）：\n\n"
        f"_例如：想重点看家具成色和厨房设施_",
        reply_markup=kb,
        parse_mode="Markdown",
    )
    return VIDEO_NOTE


async def video_note_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 3b: 用户输入备注文字"""
    context.user_data["video_note"] = update.message.text.strip()
    return await _video_show_confirm(update, context, is_callback=False)


async def video_note_skip_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 3b: 用户跳过备注"""
    q = update.callback_query
    await q.answer()
    context.user_data["video_note"] = ""
    return await _video_show_confirm(update, context, is_callback=True)


async def _video_show_confirm(update, context, is_callback=False):
    """显示确认页"""
    d = context.user_data
    note_line = f"\n📋 备注：{md_safe(d.get('video_note'))}" if d.get("video_note") else ""
    text = (
        f"📋 *请确认您的远程实拍预约*\n\n"
        f"🏠 房源：*{md_safe(d.get('video_listing',''))}*\n"
        f"📅 日期：{d.get('video_date','')}\n"
        f"⏰ 时间段：{d.get('video_time','')}"
        f"{note_line}\n\n"
        f"业务员将准时到现场与您视频连线，\n"
        f"实时带看并回答问题。\n\n"
        f"━━━━━━━━━━━━━━"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ 确认提交预约",  callback_data="vconfirm_yes")],
        [InlineKeyboardButton("✏️ 重新填写",      callback_data="menu_video")],
        [InlineKeyboardButton("🏠 主菜单",         callback_data="back_menu")],
    ])
    if is_callback:
        await update.callback_query.edit_message_text(text, reply_markup=kb, parse_mode="Markdown")
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="Markdown")
    return VIDEO_CONFIRM


async def video_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 4: 提交预约，生成编号，通知管理员"""
    q = update.callback_query
    await q.answer()
    from datetime import datetime
    import random, string
    d = context.user_data
    brand = get_brand()
    agents = get_active_agents()
    agent_line = agents[0]["tg"] if agents else "@pengqingw"
    user_name = get_user_name(update)
    tg_id = get_user_tg(update)
    # 生成预约编号
    now = datetime.now()
    seq = "".join(random.choices(string.digits, k=3))
    booking_id = f"RP-{now.strftime('%Y%m%d')}-{seq}"
    note_line = f"\n📋 备注：{md_safe(d.get('video_note'))}" if d.get("video_note") else ""
    # 通知管理员
    if ADMIN_TG_ID:
        try:
            await context.bot.send_message(
                chat_id=ADMIN_TG_ID,
                text=(
                    f"📹 *远程实拍预约*\n\n"
                    f"编号：`{booking_id}`\n"
                    f"🏠 房源：{md_safe(d.get('video_listing',''))}\n"
                    f"📅 日期：{d.get('video_date','')}\n"
                    f"⏰ 时间段：{d.get('video_time','')}"
                    f"{note_line}\n\n"
                    f"👤 {md_safe(user_name)}  {tg_id}\n"
                    f"⚡ 请在30分钟内联系用户确认"
                ),
                parse_mode="Markdown",
            )
        except Exception:
            pass
    await q.edit_message_text(
        f"✅ *远程实拍预约已成功提交！*\n\n"
        f"预约编号：`{booking_id}`\n\n"
        f"我们将在 *30分钟内* 确认最终准确时间，\n"
        f"并通过 Telegram 与您视频连线。\n\n"
        f"届时业务员会：\n"
        f"• 带您实时查看房源\n"
        f"• 检查您关心的每一个细节\n"
        f"• 客观回答所有问题\n\n"
        f"有任何变动请随时告诉我。\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"🏢 *{brand}* · 您在金边的自己人",
        reply_markup=kb_footer(),
        parse_mode="Markdown",
    )
    return MAIN_MENU


async def video_back_date_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """从时间段页返回日期页"""
    q = update.callback_query
    await q.answer()
    listing = context.user_data.get("video_listing", "")
    from datetime import date, timedelta
    today = date.today()
    dates = [today + timedelta(days=i) for i in range(7)]
    weekday_cn = ["周一","周二","周三","周四","周五","周六","周日"]
    def fmt(d):
        label = "今天" if d == today else ("明天" if d == today + timedelta(1) else
                ("后天" if d == today + timedelta(2) else
                 f"{d.month}/{d.day} {weekday_cn[d.weekday()]}"))
        return label, d.strftime("%Y-%m-%d")
    btns = []
    row = []
    for d in dates[:4]:
        lbl, val = fmt(d)
        row.append(InlineKeyboardButton(lbl, callback_data=f"vdate_{val}"))
    btns.append(row)
    row2 = []
    for d in dates[4:]:
        lbl, val = fmt(d)
        row2.append(InlineKeyboardButton(lbl, callback_data=f"vdate_{val}"))
    btns.append(row2)
    btns.append([InlineKeyboardButton("« 返回主菜单", callback_data="back_menu")])
    await q.edit_message_text(
        f"🏠 房源：*{md_safe(listing)}*\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"请选择您方便的视频看房日期：",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
    )
    return VIDEO_DATE


async def video_back_time_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """从备注页返回时间段页"""
    q = update.callback_query
    await q.answer()
    date_val = context.user_data.get("video_date", "")
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🌅 上午 09:00-12:00", callback_data="vtime_上午09:00-12:00")],
        [InlineKeyboardButton("☀️ 下午 14:00-17:00", callback_data="vtime_下午14:00-17:00")],
        [InlineKeyboardButton("🌆 傍晚 17:00-19:00", callback_data="vtime_傍晚17:00-19:00")],
        [InlineKeyboardButton("« 返回", callback_data="vback_date"),
         InlineKeyboardButton("🏠 主菜单", callback_data="back_menu")],
    ])
    await q.edit_message_text(
        f"📅 日期：*{date_val}*\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"请选择大致时间段：",
        reply_markup=kb,
        parse_mode="Markdown",
    )
    return VIDEO_TIME


# ══════════════════════════════════════════════
# 服务承诺页
# ══════════════════════════════════════════════
async def menu_promise_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    brand = get_brand()
    text = (
        f"🔒 *{brand} · 三大核心保障*\n\n"
        f"*1️⃣ 资金安全保障*\n"
        f"• 入住前全程视频留底，确保存档\n"
        f"• 押金争议先行垫付：针对合理争议，\n"
        f"  侨联承诺先行垫付（最高1个月租金）\n\n"
        f"*2️⃣ 看房避坑保障*\n"
        f"• 拒绝「照骗」，所有房源均经实地核实\n"
        f"• 提供深度实拍视频，足不出户看清细节\n\n"
        f"*3️⃣ 成本透明保障*\n"
        f"• 拒绝隐性收费：水电、物业、网费\n"
        f"  全部明码标价\n"
        f"• 现场查验电器品牌与年份，物有所值\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"🏢 *{brand}* · 您在金边的自己人"
    )
    kb = kb_footer([
        [InlineKeyboardButton("📹 预约远程实拍", callback_data="menu_video")],
    ])
    await q.edit_message_text(text, reply_markup=kb, parse_mode="Markdown")
    return MAIN_MENU


# ══════════════════════════════════════════════
# 关于侨联页
# ══════════════════════════════════════════════
async def menu_about_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    brand = get_brand()
    text = (
        f"💌 *关于{brand}*\n\n"
        f"侨联地产，是一群在金边打拼多年的华人，\n"
        f"因为深知「在异乡找到一个放心的家」有多重要，\n"
        f"才决定做这件事。\n\n"
        f"我们不是最大的中介，\n"
        f"但我们是最在乎你的那一个。\n\n"
        f"*我们的承诺：*\n"
        f"✔ 每套房源亲自实勘，不发「照骗」\n"
        f"✔ 隐性费用全部告知，不留坑\n"
        f"✔ 入住后继续跟进，不消失\n"
        f"✔ 押金纠纷，我们站你这边\n\n"
        f"深耕金边 6 年，服务数百位华人租客。\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"🏢 *{brand}*\n"
        f"📍 *您在金边的自己人*"
    )
    kb = kb_footer([
        [InlineKeyboardButton("📢 关注房源频道", url=CHANNEL_URL)],
    ])
    await q.edit_message_text(text, reply_markup=kb, parse_mode="Markdown",
                               disable_web_page_preview=True)
    return MAIN_MENU


# ══════════════════════════════════════════════
# 联系我们
# ══════════════════════════════════════════════
async def human_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        q = update.callback_query
        await q.answer()
    brand = get_brand()
    agents = get_active_agents()
    agent_lines = ""
    for a in agents:
        if a.get("tg"):
            agent_lines += f"👤 {a['name']}  {a['tg']}"
            if a.get("wechat"):
                agent_lines += f"  💬 微信：{a['wechat']}"
            agent_lines += "\n"
    text = (
        f"👩‍💼 *专业的服务团队，随时为您待命*\n\n"
        f"在金边，不管是找房、报修、问路、\n"
        f"找商家还是生活问题，都可以找我们。\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"{agent_lines}"
        f"━━━━━━━━━━━━━━\n"
        f"🕐 工作时间：9:00 – 21:00\n"
        f"📢 房源频道：{CHANNEL_URL}\n\n"
        f"🏢 *{brand}* · 您在金边的自己人"
    )
    # 为每个顾问生成独立按钮
    agent_btns = []
    for a in agents:
        if a.get("tg"):
            label = f"💬 {a['name']}"
            agent_btns.append([InlineKeyboardButton(label, url=f"https://t.me/{a['tg'].lstrip('@')}")])
    kb = InlineKeyboardMarkup(agent_btns + [
        [InlineKeyboardButton("📢 去频道看房源", url=CHANNEL_URL)],
        [
            InlineKeyboardButton("📅 预约办理", callback_data="appoint_"),
            InlineKeyboardButton("💎 咨询顾问", callback_data="menu_human"),
            InlineKeyboardButton("🏠 返回首页", callback_data="back_menu"),
        ],
    ])
    if update.callback_query:
        await update.callback_query.edit_message_text(
            text, reply_markup=kb, parse_mode="Markdown",
            disable_web_page_preview=True
        )
    else:
        await update.message.reply_text(
            text, reply_markup=kb, parse_mode="Markdown",
            disable_web_page_preview=True
        )
    return MAIN_MENU

# ══════════════════════════════════════════════
# 订阅 / 取消
# ══════════════════════════════════════════════
async def subscribe_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    added = add_subscriber(user.id, get_user_name(update), get_user_tg(update))
    if added:
        text = (
            "✅ *订阅成功！*\n\n"
            "表示你愿意接收本 Bot 的 *管理员群发*（有新消息时才会发，"
            "例如房源提醒、活动通知等，具体以实际发送为准）。\n\n"
            "本 Bot *不会*自动每天推送汇率/签证；若需要可私信顾问。\n\n"
            "随时回复 /unsubscribe 取消。"
        )
    else:
        text = "你已经订阅了，无需重复操作。"
    await update.message.reply_text(text, parse_mode="Markdown")

async def unsubscribe_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remove_subscriber(update.effective_user.id)
    await update.message.reply_text("❌ 已取消订阅。\n需要时随时回复 /subscribe 重新订阅。")

# ══════════════════════════════════════════════
# /help
# ══════════════════════════════════════════════
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    brand = get_brand()
    agents = get_active_agents()
    agent_line = agents[0]["tg"] if agents else "@pengqingw"
    await update.message.reply_text(
        f"📞 *{brand} · 帮助*\n\n"
        f"✈️ 联系我们：{agent_line}\n\n"
        f"📢 房源频道：{CHANNEL_URL}\n\n"
        f"📅 预约看房：发送 /appoint\n"
        f"📰 订阅资讯：发送 /subscribe\n"
        f"↩️ 中断当前步骤：发送 /cancel\n"
        f"🏠 返回主菜单：发送 /start\n\n"
        f"🏢 {brand} · 你在金边的自己人",
        parse_mode="Markdown",
        disable_web_page_preview=True,
    )

# ══════════════════════════════════════════════
# 管理员：发布新房源（全按钮化）
# ══════════════════════════════════════════════
async def admin_publish_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return ConversationHandler.END
    context.user_data.clear()
    context.user_data["admin_photos"] = []
    await update.message.reply_text(
        "📸 *发布新房源*\n\n"
        "第1步：发送房源图片（可多张，第一张作封面）\n"
        "发完后发送 /done",
        parse_mode="Markdown"
    )
    return ADMIN_PHOTOS

async def admin_photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return ADMIN_PHOTOS
    photos = context.user_data.setdefault("admin_photos", [])
    if update.message.photo:
        photos.append(update.message.photo[-1].file_id)
        await update.message.reply_text(f"✅ 已收到第 {len(photos)} 张，继续发或 /done 完成")
    return ADMIN_PHOTOS

async def admin_photos_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return ADMIN_PHOTOS
    photos = context.user_data.get("admin_photos", [])
    if not photos:
        await update.message.reply_text("❌ 请先发送至少1张图片")
        return ADMIN_PHOTOS
    await update.message.reply_text(
        f"✅ 已收到 {len(photos)} 张图片\n\n第2步：选择房源类型：",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 公寓",   callback_data="atype_住宅"),
             InlineKeyboardButton("🏡 别墅",   callback_data="atype_别墅")],
            [InlineKeyboardButton("🏘 排屋",   callback_data="atype_排屋"),
             InlineKeyboardButton("🏠 独栋",   callback_data="atype_独栋")],
            [InlineKeyboardButton("🏢 写字楼", callback_data="atype_办公室"),
             InlineKeyboardButton("🏪 商铺",   callback_data="atype_商铺")],
        ]),
    )
    return ADMIN_TYPE

async def admin_type_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data["admin_prop_type"] = q.data.replace("atype_", "")
    await q.edit_message_text(
        "第3步：选择区域：",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🏙 富力城",   callback_data="aarea_富力城"),
             InlineKeyboardButton("🌆 炳发城",   callback_data="aarea_炳发城")],
            [InlineKeyboardButton("🌟 太子/幸福", callback_data="aarea_太子"),
             InlineKeyboardButton("📍 BKK1",     callback_data="aarea_BKK1")],
            [InlineKeyboardButton("🗺 TK/7月区", callback_data="aarea_TK"),
             InlineKeyboardButton("💎 钻石岛",   callback_data="aarea_钻石岛")],
            [InlineKeyboardButton("🛣 洪森大道", callback_data="aarea_洪森大道"),
             InlineKeyboardButton("🔍 其他",     callback_data="aarea_其他")],
        ]),
    )
    return ADMIN_AREA

async def admin_area_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data["admin_area"] = q.data.replace("aarea_", "")
    await q.edit_message_text(
        "第4步：选择户型：",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Studio",  callback_data="aroom_Studio"),
             InlineKeyboardButton("1房1卫",  callback_data="aroom_1房1卫")],
            [InlineKeyboardButton("2房1卫",  callback_data="aroom_2房1卫"),
             InlineKeyboardButton("2房2卫",  callback_data="aroom_2房2卫")],
            [InlineKeyboardButton("3房2卫",  callback_data="aroom_3房2卫"),
             InlineKeyboardButton("3房3卫",  callback_data="aroom_3房3卫")],
            [InlineKeyboardButton("4房+",    callback_data="aroom_4房+"),
             InlineKeyboardButton("整层/整栋", callback_data="aroom_整层")],
        ]),
    )
    return ADMIN_ROOM

async def admin_room_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data["admin_room_type"] = q.data.replace("aroom_", "")
    await q.edit_message_text(
        "第5步：请输入租金（数字，美元/月）和面积（㎡）\n\n"
        "格式：租金 面积\n例如：650 85"
    )
    return ADMIN_PRICE

async def admin_price_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return ADMIN_PRICE
    parts = update.message.text.strip().split()
    if len(parts) < 2:
        await update.message.reply_text(
            "❌ 请一次输入两个数字：租金（美元/月）和面积（㎡）\n"
            "示例：`650 85`",
            parse_mode="Markdown",
        )
        return ADMIN_PRICE
    try:
        float(str(parts[0]).replace(",", ""))
        float(str(parts[1]).replace(",", ""))
    except ValueError:
        await update.message.reply_text(
            "❌ 租金和面积需要是有效数字，例如：650 85",
        )
        return ADMIN_PRICE
    context.user_data["admin_price"] = parts[0]
    context.user_data["admin_size"] = parts[1]
    await update.message.reply_text(
        "第6步：选择押金方式：",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("押1付1", callback_data="adepo_押1付1"),
             InlineKeyboardButton("押2付1", callback_data="adepo_押2付1")],
            [InlineKeyboardButton("押1付3", callback_data="adepo_押1付3"),
             InlineKeyboardButton("押1付6", callback_data="adepo_押1付6")],
            [InlineKeyboardButton("其他",   callback_data="adepo_其他")],
        ]),
    )
    return ADMIN_DEPOSIT

async def admin_deposit_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data["admin_deposit"] = q.data.replace("adepo_", "")
    await q.edit_message_text(
        "第7步：选择配套（可多选，选完发 /next）：",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🏊 泳池",   callback_data="afeat_泳池"),
             InlineKeyboardButton("💪 健身房", callback_data="afeat_健身房")],
            [InlineKeyboardButton("🛋 全家具", callback_data="afeat_全套家具"),
             InlineKeyboardButton("🚗 停车位", callback_data="afeat_停车位")],
            [InlineKeyboardButton("🌊 河景",   callback_data="afeat_河景"),
             InlineKeyboardButton("🛗 电梯",   callback_data="afeat_电梯")],
            [InlineKeyboardButton("🌿 花园",   callback_data="afeat_花园"),
             InlineKeyboardButton("🔒 门禁",   callback_data="afeat_门禁")],
            [InlineKeyboardButton("✅ 完成选配套", callback_data="afeat_done")],
        ]),
    )
    context.user_data.setdefault("admin_features_list", [])
    return ADMIN_FEATURES

async def admin_features_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    feat = q.data.replace("afeat_", "")
    if feat == "done":
        feats = context.user_data.get("admin_features_list", [])
        context.user_data["admin_features"] = "、".join(feats) if feats else "基本配套"
        await q.edit_message_text(
            "第8步：选择可入住时间：",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("随时入住",  callback_data="amove_随时"),
                 InlineKeyboardButton("本月底",    callback_data="amove_本月底")],
                [InlineKeyboardButton("下个月",    callback_data="amove_下个月"),
                 InlineKeyboardButton("具体日期",  callback_data="amove_other")],
            ]),
        )
        return ADMIN_MOVEIN
    else:
        feats = context.user_data.setdefault("admin_features_list", [])
        if feat not in feats:
            feats.append(feat)
        await q.answer(f"✅ 已选：{feat}", show_alert=False)
        return ADMIN_FEATURES

async def admin_features_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return ADMIN_FEATURES
    feats = context.user_data.get("admin_features_list", [])
    context.user_data["admin_features"] = "、".join(feats) if feats else "基本配套"
    await update.message.reply_text(
        "第8步：选择可入住时间：",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("随时入住",  callback_data="amove_随时"),
             InlineKeyboardButton("本月底",    callback_data="amove_本月底")],
            [InlineKeyboardButton("下个月",    callback_data="amove_下个月"),
             InlineKeyboardButton("具体日期",  callback_data="amove_other")],
        ]),
    )
    return ADMIN_MOVEIN

async def admin_movein_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    val = q.data.replace("amove_", "")
    if val == "other":
        await q.edit_message_text("请输入可入住日期（例如：5月1日）：")
        return ADMIN_MOVEIN
    context.user_data["admin_movein"] = val
    await q.edit_message_text(
        "第9步：请写出这套房的优势\n（每行一个，最多3条，写完发 /next）\n\n"
        "例如：\n小区有泳池和健身房\n步行3分钟到超市\n房东好说话可议价"
    )
    return ADMIN_PROS

async def admin_movein_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return ADMIN_MOVEIN
    context.user_data["admin_movein"] = update.message.text.strip()
    await update.message.reply_text(
        "第9步：请写出这套房的优势\n（每行一个，最多3条，写完发 /next）\n\n"
        "例如：\n小区有泳池和健身房\n步行3分钟到超市\n房东好说话可议价"
    )
    return ADMIN_PROS

async def admin_pros_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return ADMIN_PROS
    lines = [l.strip() for l in update.message.text.strip().split("\n") if l.strip()]
    context.user_data["admin_pros"] = lines[:3]
    await update.message.reply_text(
        "第10步：可接受的缺点\n（每行一个，最多2条，写完发 /next）\n\n"
        "例如：\n靠马路有点吵\n没有阳台"
    )
    return ADMIN_CONS

async def admin_pros_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return ADMIN_PROS
    await update.message.reply_text(
        "第10步：可接受的缺点\n（每行一个，最多2条，写完发 /next）\n\n"
        "例如：\n靠马路有点吵\n没有阳台"
    )
    return ADMIN_CONS

async def admin_cons_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return ADMIN_CONS
    lines = [l.strip() for l in update.message.text.strip().split("\n") if l.strip()]
    context.user_data["admin_cons"] = lines[:2]
    await update.message.reply_text(
        "第11步：写一段「侨联说」\n（客观点评，2-3句话）\n\n"
        "例如：\n这套我们实地看过三次。优点是家具很新，房东是中国人好沟通；"
        "缺点是卧室朝西，下午有点晒。适合预算有限但想要新家具的单身或情侣。"
    )
    return ADMIN_COMMENT

async def admin_cons_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return ADMIN_CONS
    await update.message.reply_text(
        "第11步：写一段「侨联说」\n（客观点评，2-3句话）\n\n"
        "例如：\n这套我们实地看过三次。优点是家具很新，房东是中国人好沟通；"
        "缺点是卧室朝西，下午有点晒。适合预算有限但想要新家具的单身或情侣。"
    )
    return ADMIN_COMMENT

async def admin_comment_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return ADMIN_COMMENT
    context.user_data["admin_comment"] = update.message.text.strip()
    await _show_publish_preview(update, context)
    return ADMIN_CONFIRM

async def _show_publish_preview(update, context):
    d = context.user_data
    brand = get_brand()
    prop_icons = {"住宅": "🏠", "别墅": "🏡", "排屋": "🏘", "独栋": "🏠",
                  "办公室": "🏢", "商铺": "🏪"}
    icon = prop_icons.get(d.get("admin_prop_type", "住宅"), "🏠")
    pros_text = "\n".join(f"• {p}" for p in d.get("admin_pros", []))
    cons_text = "\n".join(f"• {c}" for c in d.get("admin_cons", []))
    photos = d.get("admin_photos", [])

    preview = (
        f"📋 *预览房源帖子*\n\n"
        f"{icon} *{d.get('admin_community','（小区名）')}*  🆕 新上\n"
        f"📍 位置：{d.get('admin_area','')}\n"
        f"💰 租金：*${d.get('admin_price','')}*/月\n"
        f"🏠 户型：{d.get('admin_room_type','')}\n"
        f"📐 面积：{d.get('admin_size','')}㎡\n"
        f"✅ 配套：{d.get('admin_features','')}\n"
        f"🔑 押金：{d.get('admin_deposit','')}\n"
        f"📅 可入住：{d.get('admin_movein','随时')}\n\n"
        f"✨ 优势：\n{pros_text}\n\n"
        f"⚠️ 缺点：\n{cons_text}\n\n"
        f"💬 侨联说：\n{d.get('admin_comment','')}\n\n"
        f"──────────\n"
        f"👉 找房助手 @{d.get('bot_username', BOT_USERNAME)}\n"
        f"📢 更多房源：{CHANNEL_URL}\n\n"
        f"📸 图片：{len(photos)} 张"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ 确认发布到频道", callback_data="apub_confirm")],
        [InlineKeyboardButton("✏️ 修改小区名",    callback_data="apub_edit_community")],
        [InlineKeyboardButton("❌ 取消",           callback_data="apub_cancel")],
    ])
    if update.message:
        await update.message.reply_text(preview, reply_markup=kb, parse_mode="Markdown")
    else:
        await update.callback_query.edit_message_text(preview, reply_markup=kb, parse_mode="Markdown")

async def admin_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "apub_cancel":
        await q.edit_message_text("❌ 已取消发布")
        return ConversationHandler.END
    if q.data == "apub_edit_community":
        await q.edit_message_text("请输入小区名称：")
        return ADMIN_CONFIRM

    # 确认发布
    d = context.user_data
    brand = get_brand()
    prop_icons = {"住宅": "🏠", "别墅": "🏡", "排屋": "🏘", "独栋": "🏠",
                  "办公室": "🏢", "商铺": "🏪"}
    icon = prop_icons.get(d.get("admin_prop_type", "住宅"), "🏠")
    pros_text = "\n".join(f"• {p}" for p in d.get("admin_pros", []))
    cons_text = "\n".join(f"• {c}" for c in d.get("admin_cons", []))
    photos = d.get("admin_photos", [])
    area_tag = d.get("admin_area", "").replace(" ", "")
    room_tag = d.get("admin_room_type", "").replace(" ", "")

    # 主帖：精简信息 + slogan + hashtag
    caption = (
        f"{icon} *{d.get('admin_community', d.get('admin_area',''))}*  🆕 新上\n\n"
        f"📍 区域：{d.get('admin_area','')}\n"
        f"💰 租金：*${d.get('admin_price','')}*/月\n"
        f"🏠 户型：{d.get('admin_room_type','')}\n"
        f"📐 面积：{d.get('admin_size','')}㎡\n"
        f"✅ 配套：{d.get('admin_features','')}\n"
        f"🔑 押金：{d.get('admin_deposit','')}\n"
        f"📅 可入住：{d.get('admin_movein','随时')}\n\n"
        f"#金边租房 #{brand} #{area_tag} #{room_tag}\n\n"
        f"🏢 *{brand}* · 您在金边的自己人"
    )

    # 主帖4个按钮
    caption_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🏠 智能找房", url=f"https://t.me/{BOT_USERNAME}?start=start"),
         InlineKeyboardButton("📋 查看详情", url=f"https://t.me/{BOT_USERNAME}?start=start")],
        [InlineKeyboardButton("📅 预约看房", url=f"https://t.me/{BOT_USERNAME}?start=appoint"),
         InlineKeyboardButton("📞 联系顾问", url=f"https://t.me/{BOT_USERNAME}?start=start")],
    ])

    # 讨论组跟帖内容：优势/缺点/侨联说
    detail_text = (
        f"✨ *优势：*\n{pros_text}\n\n"
        f"⚠️ *缺点：*\n{cons_text}\n\n"
        f"💬 *侨联说：*\n{d.get('admin_comment','')}\n\n"
        f"──────────\n"
        f"🏢 *{brand}* · 您在金边的自己人"
    )
    detail_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 预约看房", url=f"https://t.me/{BOT_USERNAME}?start=appoint"),
         InlineKeyboardButton("🏠 智能找房", url=f"https://t.me/{BOT_USERNAME}?start=start")],
    ])

    try:
        sent_msg = None
        if len(photos) == 1:
            sent_msg = await context.bot.send_photo(
                chat_id=CHANNEL_ID, photo=photos[0],
                caption=caption, parse_mode="Markdown",
                reply_markup=caption_kb
            )
        elif len(photos) > 1:
            media = [InputMediaPhoto(p) for p in photos]
            media[0] = InputMediaPhoto(photos[0], caption=caption, parse_mode="Markdown")
            msgs = await context.bot.send_media_group(chat_id=CHANNEL_ID, media=media)
            sent_msg = msgs[0] if msgs else None
            # media_group 不支持内联按钮，单独发一条按钮消息
            if sent_msg:
                try:
                    await context.bot.send_message(
                        chat_id=CHANNEL_ID,
                        text=f"🏢 *{brand}* · 您在金边的自己人",
                        reply_markup=caption_kb,
                        parse_mode="Markdown",
                        reply_to_message_id=sent_msg.message_id if hasattr(sent_msg, "message_id") else None,
                    )
                except Exception:
                    pass

        # 讨论组跟帖：优势/缺点/侨联说 + 按钮
        if DISCUSSION_ID and sent_msg:
            try:
                reply_id = sent_msg.message_id if hasattr(sent_msg, "message_id") else None
                await context.bot.send_message(
                    chat_id=DISCUSSION_ID,
                    text=detail_text,
                    reply_markup=detail_kb,
                    parse_mode="Markdown",
                    reply_to_message_id=reply_id,
                )
            except Exception as e:
                logger.warning(f"讨论组跟帖失败: {e}")

        # 保存到listings.json
        import random, string
        lid = "QL" + "".join(random.choices(string.digits, k=4))
        listing_item = {
            "listing_id": lid,
            "prop_type": d.get("admin_prop_type", "住宅"),
            "community": d.get("admin_community", d.get("admin_area", "")),
            "area": d.get("admin_area", ""),
            "price": d.get("admin_price", ""),
            "room_type": d.get("admin_room_type", ""),
            "size": d.get("admin_size", ""),
            "deposit": d.get("admin_deposit", ""),
            "features": d.get("admin_features", ""),
            "movein": d.get("admin_movein", "随时"),
            "pros": " / ".join(d.get("admin_pros", [])),
            "cons": " / ".join(d.get("admin_cons", [])),
            "comment": d.get("admin_comment", ""),
            "photos": photos,
            "status": "上架",
            "badge": "new",
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        listings = load_listings()
        listings.append(listing_item)
        save_json_file(LISTINGS_FILE, listings)

        await q.edit_message_text(
            f"✅ *已成功发布到频道！*\n房源编号：{lid}",
            parse_mode="Markdown"
        )
    except Exception as e:
        await q.edit_message_text(f"❌ 发布失败：{e}\n\n请确认 Bot 已被设为频道管理员")
    return ConversationHandler.END

async def admin_community_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """在ADMIN_CONFIRM状态下输入小区名称"""
    if not is_admin(update):
        return ADMIN_CONFIRM
    context.user_data["admin_community"] = update.message.text.strip()
    await _show_publish_preview(update, context)
    return ADMIN_CONFIRM

# ══════════════════════════════════════════════
# 管理员：统计 / 群发 / setinfo
# ══════════════════════════════════════════════
async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    leads   = load_json_file(LEADS_FILE)
    appts   = load_json_file(APPOINTS_FILE)
    subs    = load_subscribers()
    listings = load_listings()
    today   = datetime.now().strftime("%Y-%m-%d")
    today_leads = [l for l in leads if l.get("timestamp", "").startswith(today)]
    await update.message.reply_text(
        f"📊 *线索统计*\n\n"
        f"总线索：{len(leads)} 条\n"
        f"今日新增：{len(today_leads)} 条\n"
        f"预约看房：{len(appts)} 条\n"
        f"订阅用户：{len(subs)} 人\n"
        f"上架房源：{len([x for x in listings if x.get('status')=='上架'])} 套",
        parse_mode="Markdown",
    )

async def admin_setinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    args = context.args or []
    cfg  = load_config()
    if not args:
        agents = cfg.get("agents", [])
        lines = [f"🏢 品牌：{cfg.get('brand', '侨联地产')}"]
        for i, a in enumerate(agents):
            name = a.get("name") or "（空）"
            tg   = a.get("tg") or ""
            lines.append(f"👤 中介{i+1}：{name} {tg}")
        lines.append("\n用法：")
        lines.append("/setinfo brand 侨联地产")
        lines.append("/setinfo agent1 小彭 @pengqingw pengqingw")
        lines.append("/setinfo discussion -1001234567890")
        await update.message.reply_text("\n".join(lines))
        return
    key = args[0].lower()
    if key == "brand" and len(args) >= 2:
        cfg["brand"] = args[1]
        save_config(cfg)
        await update.message.reply_text(f"✅ 品牌名已更新：{args[1]}")
    elif key.startswith("agent") and key[5:].isdigit():
        idx = int(key[5:]) - 1
        if 0 <= idx < 4:
            agents = cfg.setdefault("agents", DEFAULT_CONFIG["agents"].copy())
            while len(agents) < 4:
                agents.append({"name": "", "tg": "", "wechat": ""})
            agents[idx] = {
                "name":   args[1] if len(args) > 1 else "",
                "tg":     args[2] if len(args) > 2 else "",
                "wechat": args[3] if len(args) > 3 else "",
            }
            save_config(cfg)
            await update.message.reply_text(f"✅ 中介{idx+1}已更新：{agents[idx]}")
    elif key == "discussion" and len(args) >= 2:
        # 更新.env中的DISCUSSION_ID（写入config）
        cfg["discussion_id"] = args[1]
        save_config(cfg)
        global DISCUSSION_ID
        DISCUSSION_ID = args[1]
        await update.message.reply_text(f"✅ 讨论组ID已更新：{args[1]}")
    else:
        await update.message.reply_text("❌ 参数格式不对，发 /setinfo 查看用法")

async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    text = update.message.text.replace("/broadcast", "").strip()
    if not text:
        await update.message.reply_text("用法：/broadcast 消息内容")
        return
    subs = load_subscribers()
    ok = fail = 0
    for i, s in enumerate(subs):
        try:
            await context.bot.send_message(chat_id=s["user_id"], text=text)
            ok += 1
        except Exception:
            fail += 1
        if i + 1 < len(subs):
            await asyncio.sleep(0.05)
    await update.message.reply_text(f"✅ 已发送给 {ok}/{ok+fail} 位用户")

# ══════════════════════════════════════════════
# /appoint 命令入口
# ══════════════════════════════════════════════
async def appoint_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("appoint_listing", None)
    await update.message.reply_text(
        "📅 *预约看房*\n\n请告诉我您想看的房源名称或地址\n"
        "（例如：富力城华府 2房）。\n\n"
        "支持实地或视频看房；若要 *视频*，留联系方式时请备注「视频」。",
        parse_mode="Markdown",
    )
    return APPOINT_LISTING

# ══════════════════════════════════════════════
# main
# ══════════════════════════════════════════════
def main():
    # 从config加载讨论组ID
    global DISCUSSION_ID
    cfg = load_config()
    if cfg.get("discussion_id"):
        DISCUSSION_ID = cfg["discussion_id"]

    request = HTTPXRequest(
        connect_timeout=20.0,
        read_timeout=60.0,
        write_timeout=45.0,
        pool_timeout=45.0,
    )
    # 默认走反代：部分机房无法直连 Telegram；能直连时在 .env 设 TG_API_BASE=https://api.telegram.org/bot
    tg_base = os.getenv(
        "TG_API_BASE",
        "https://tg-proxy.njkjajjj-0c1.workers.dev/bot",
    ).rstrip("/")
    app = Application.builder().token(BOT_TOKEN).base_url(
        tg_base
    ).request(request).build()

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start",   start),
            CommandHandler("appoint", appoint_cmd),
            CallbackQueryHandler(main_menu_cb,  pattern="^back_menu$"),
            CallbackQueryHandler(appoint_start, pattern="^appoint_"),
        ],
        states={
            MAIN_MENU: [
                CallbackQueryHandler(find_residential, pattern="^find_residential$"),
                CallbackQueryHandler(find_villa,       pattern="^find_villa$"),
                CallbackQueryHandler(menu_video_cb,    pattern="^menu_video$"),
                CallbackQueryHandler(menu_promise_cb,  pattern="^menu_promise$"),
                CallbackQueryHandler(menu_about_cb,    pattern="^menu_about$"),
                CallbackQueryHandler(service_menu,     pattern="^menu_service$"),
                CallbackQueryHandler(nearby_menu,      pattern="^menu_nearby$"),
                CallbackQueryHandler(maintenance_menu, pattern="^menu_maintenance$"),
                CallbackQueryHandler(human_menu,       pattern="^menu_human$"),
                CallbackQueryHandler(menu_help_cb,     pattern="^menu_help$"),
                CallbackQueryHandler(menu_subscribe_cb, pattern="^menu_subscribe$"),
                CallbackQueryHandler(nearby_area_cb,   pattern="^nearby_"),
                CallbackQueryHandler(nearby_cat_cb,    pattern="^ncat_"),
                CallbackQueryHandler(maint_type_cb,    pattern="^maint_"),
                CallbackQueryHandler(appoint_start,    pattern="^appoint_"),
                CallbackQueryHandler(like_listing_cb,  pattern="^like_"),
            ],
            FIND_AREA: [
                CallbackQueryHandler(find_area_cb,   pattern="^area_"),
                CallbackQueryHandler(main_menu_cb,   pattern="^back_menu$"),
            ],
            FIND_BUDGET: [
                CallbackQueryHandler(find_budget_cb,  pattern="^budget_"),
                CallbackQueryHandler(back_find_area,  pattern="^back_find_area$"),
                CallbackQueryHandler(main_menu_cb,    pattern="^back_menu$"),
            ],
            MAINT_TYPE: [
                CallbackQueryHandler(maint_type_cb, pattern="^maint_"),
                CallbackQueryHandler(main_menu_cb,  pattern="^back_menu$"),
            ],
            NEARBY_AREA: [
                CallbackQueryHandler(nearby_area_cb, pattern="^nearby_"),
                CallbackQueryHandler(nearby_menu,    pattern="^menu_nearby$"),
                CallbackQueryHandler(main_menu_cb,   pattern="^back_menu$"),
            ],
            NEARBY_CAT: [
                CallbackQueryHandler(nearby_cat_cb,  pattern="^ncat_"),
                CallbackQueryHandler(nearby_area_cb, pattern="^nearby_"),
                CallbackQueryHandler(nearby_menu,    pattern="^menu_nearby$"),
                CallbackQueryHandler(main_menu_cb,   pattern="^back_menu$"),
            ],
            # 预约看房
            APPOINT_LISTING: [
                MessageHandler(TEXT, appoint_listing_input),
                CallbackQueryHandler(main_menu_cb, pattern="^back_menu$"),
            ],
            APPOINT_DATE: [
                CallbackQueryHandler(appoint_date_cb,  pattern="^apdate_"),
                MessageHandler(TEXT, appoint_date_text),
                CallbackQueryHandler(main_menu_cb,     pattern="^back_menu$"),
            ],
            APPOINT_TIME: [
                CallbackQueryHandler(appoint_time_cb, pattern="^aptime_"),
                CallbackQueryHandler(appoint_back_to_date_cb, pattern="^back_apdate$"),
                CallbackQueryHandler(main_menu_cb,    pattern="^back_menu$"),
            ],
            APPOINT_CONFIRM: [
                CallbackQueryHandler(appoint_confirm_cb,      pattern="^apconfirm_"),
                CallbackQueryHandler(appoint_back_to_time_cb, pattern="^back_aptime$"),
                CallbackQueryHandler(main_menu_cb,            pattern="^back_menu$"),
            ],
            # 远程实拍预约流程
            VIDEO_LISTING: [
                MessageHandler(TEXT, video_listing_input),
                CallbackQueryHandler(main_menu_cb,  pattern="^back_menu$"),
            ],
            VIDEO_DATE: [
                CallbackQueryHandler(video_date_cb,  pattern="^vdate_"),
                CallbackQueryHandler(main_menu_cb,   pattern="^back_menu$"),
            ],
            VIDEO_TIME: [
                CallbackQueryHandler(video_time_cb,      pattern="^vtime_"),
                CallbackQueryHandler(video_back_date_cb, pattern="^vback_date$"),
                CallbackQueryHandler(main_menu_cb,       pattern="^back_menu$"),
            ],
            VIDEO_NOTE: [
                MessageHandler(TEXT, video_note_input),
                CallbackQueryHandler(video_note_skip_cb,  pattern="^vnote_skip$"),
                CallbackQueryHandler(video_back_time_cb,  pattern="^vback_time$"),
                CallbackQueryHandler(main_menu_cb,        pattern="^back_menu$"),
            ],
            VIDEO_CONFIRM: [
                CallbackQueryHandler(video_confirm_cb,  pattern="^vconfirm_yes$"),
                CallbackQueryHandler(menu_video_cb,     pattern="^menu_video$"),
                CallbackQueryHandler(main_menu_cb,      pattern="^back_menu$"),
            ],
            # 管理员发布
            ADMIN_PHOTOS: [
                MessageHandler(filters.PHOTO | filters.Document.IMAGE, admin_photo_handler),
                CommandHandler("done", admin_photos_done),
            ],
            ADMIN_TYPE: [
                CallbackQueryHandler(admin_type_cb, pattern="^atype_"),
            ],
            ADMIN_AREA: [
                CallbackQueryHandler(admin_area_cb, pattern="^aarea_"),
            ],
            ADMIN_ROOM: [
                CallbackQueryHandler(admin_room_cb, pattern="^aroom_"),
            ],
            ADMIN_PRICE: [
                MessageHandler(TEXT, admin_price_input),
            ],
            ADMIN_DEPOSIT: [
                CallbackQueryHandler(admin_deposit_cb, pattern="^adepo_"),
            ],
            ADMIN_FEATURES: [
                CallbackQueryHandler(admin_features_cb, pattern="^afeat_"),
                CommandHandler("next", admin_features_next),
            ],
            ADMIN_MOVEIN: [
                CallbackQueryHandler(admin_movein_cb,  pattern="^amove_"),
                MessageHandler(TEXT, admin_movein_text),
            ],
            ADMIN_PROS: [
                MessageHandler(TEXT, admin_pros_input),
                CommandHandler("next", admin_pros_next),
            ],
            ADMIN_CONS: [
                MessageHandler(TEXT, admin_cons_input),
                CommandHandler("next", admin_cons_next),
            ],
            ADMIN_COMMENT: [
                MessageHandler(TEXT, admin_comment_input),
            ],
            ADMIN_CONFIRM: [
                CallbackQueryHandler(admin_confirm_cb,    pattern="^apub_"),
                MessageHandler(TEXT, admin_community_input),
            ],
        },
        fallbacks=[
            CommandHandler("start", start),
            CommandHandler("cancel", cancel_cmd),
            CommandHandler("help", help_cmd),
        ],
        allow_reentry=True,
        name="qiaolian_conv_v6",
        persistent=False,
    )

    app.add_handler(conv)
    app.add_handler(CommandHandler("stats",       admin_stats))
    app.add_handler(CommandHandler("setinfo",     admin_setinfo))
    app.add_handler(CommandHandler("publish",     admin_publish_start))
    app.add_handler(CommandHandler("broadcast",   admin_broadcast))
    app.add_handler(CommandHandler("subscribe",   subscribe_cmd))
    app.add_handler(CommandHandler("unsubscribe", unsubscribe_cmd))
    app.add_handler(CommandHandler("help",        help_cmd))

    # ── 注册命令菜单（用户可见 + 管理员专属）──────────────────────────────
    from telegram import BotCommand, BotCommandScopeDefault, BotCommandScopeChat
    async def _register_commands(app):
        user_cmds = [
            BotCommand("start",       "🏠 打开主菜单"),
            BotCommand("appoint",     "📅 预约看房"),
            BotCommand("subscribe",   "🔔 订阅新房源提醒"),
            BotCommand("unsubscribe", "🔕 取消订阅"),
            BotCommand("help",        "❓ 使用帮助"),
        ]
        await app.bot.set_my_commands(user_cmds, scope=BotCommandScopeDefault())
        if ADMIN_TG_ID:
            admin_cmds = user_cmds + [
                BotCommand("publish",   "📤 发布新房源"),
                BotCommand("stats",     "📊 查看数据统计"),
                BotCommand("broadcast", "📢 群发消息"),
                BotCommand("setinfo",   "⚙️ 修改品牌/中介信息"),
            ]
            try:
                await app.bot.set_my_commands(
                    admin_cmds,
                    scope=BotCommandScopeChat(chat_id=ADMIN_TG_ID),
                )
            except Exception as e:
                logger.warning(f"管理员命令菜单注册失败: {e}")
    app.post_init = _register_commands

    logger.info("🤖 侨联地产 找房助手 v6.0 启动中… TG_API=%s", tg_base.rstrip("/")[:40])
    app.run_polling(drop_pending_updates=True, poll_interval=0.0)

if __name__ == "__main__":
    main()

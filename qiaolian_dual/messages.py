from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from .config import ADVISOR_PHONE, ADVISOR_TG, ADVISOR_WECHAT, BRAND_NAME, CHANNEL_URL
from .utils import compact_join, e


def listing_summary(item: dict) -> str:
    tags = compact_join(item.get("tags", []), " / ")
    lines = [
        f"🏠 <b>{e(item.get('title'))}</b>",
        f"💰 ${e(item.get('price'))}/月",
        f"📍 {e(item.get('area'))} · {e(item.get('community'))}",
    ]
    if item.get("layout"):
        lines.append(f"🛏 {e(item.get('layout'))}")
    if item.get("size_sqm"):
        lines.append(f"📐 {e(item.get('size_sqm'))}㎡")
    if tags:
        lines.append(f"✨ {e(tags)}")
    return "\n".join(lines)


def listing_detail(item: dict) -> str:
    tags = compact_join(item.get("tags", []), " / ")
    lines = [
        f"🏠 <b>{e(item.get('title'))}</b>",
        f"编号：<code>{e(item.get('listing_id'))}</code>",
        f"💰 月租：<b>${e(item.get('price'))}</b>",
        f"📍 区域：{e(item.get('area'))} · {e(item.get('community'))}",
    ]
    if item.get("layout"):
        lines.append(f"🛏 户型：{e(item.get('layout'))}")
    if item.get("size_sqm"):
        lines.append(f"📐 面积：{e(item.get('size_sqm'))}㎡")
    if item.get("deposit_rule"):
        lines.append(f"🔑 押金：{e(item.get('deposit_rule'))}")
    if item.get("available_date"):
        lines.append(f"📅 可入住：{e(item.get('available_date'))}")
    if tags:
        lines.append(f"✨ 标签：{e(tags)}")
    if item.get("highlights"):
        lines.append(f"\n<b>亮点</b>\n{e(item.get('highlights'))}")
    if item.get("hidden_costs"):
        lines.append(f"\n<b>费用说明</b>\n{e(item.get('hidden_costs'))}")
    if item.get("drawbacks"):
        lines.append(f"\n<b>顾问提醒</b>\n{e(item.get('drawbacks'))}")
    lines.append(f"\n💬 看中这套，点下面按钮继续。")
    return "\n".join(lines)


def home_text() -> str:
    return (
        f"🏠 <b>{BRAND_NAME}找房助手</b>\n\n"
        "点下方按钮找房、预约或咨询顾问。"
    )


def channel_welcome_text(first_name: str = "") -> str:
    """首屏 /start 欢迎语（无深链参数）：精简版，保留核心动作按钮。"""
    name_part = f" {e(first_name)}" if first_name else ""
    return (
        f"👋 你好{name_part}\n\n"
        f"我是{BRAND_NAME}找房助手\n\n"
        "可以帮你：\n"
        "🏠 找房\n"
        "📅 预约看房\n"
        "📹 视频看房\n"
        "📍 按区域找\n\n"
        "👇 选一个开始"
    )


def lead_capture_text() -> str:
    """留资触发节点文案：在预约/视频看房/费用咨询/联系顾问后触发。"""
    return (
        "好的，方便留一个联系方式吗？\n\n"
        "顾问确认房源和看房时间会更快一点。\n\n"
        "可以发 Telegram / 微信 / 电话。"
    )


def advisor_text() -> str:
    return (
        "💬 <b>中文顾问已接入</b>\n\n"
        "您可以直接问：房源、片区、价格范围、押付、签约、入住安排或租后问题。\n"
        "如果您是从某套房源点进来的，顾问会按当前房源继续跟进，不用重复说明。\n\n"
        f"📱 Telegram：{e(ADVISOR_TG)}\n"
        f"💬 微信：{e(ADVISOR_WECHAT)}\n"
        f"📞 电话：{e(ADVISOR_PHONE)}\n"
        f"📢 频道：{e(CHANNEL_URL)}"
    )


def deposit_text() -> str:
    return (
        f"🔒 <b>{BRAND_NAME} 押金与费用说明</b>\n\n"
        "带看与签约前，我们会把押付、起租、水电（<b>按表 / 包月 / 公摊</b>）、"
        "网络是否需自装、<b>物业与停车</b>、常见隐性项先对齐。\n"
        "入住前建议把全屋现状、<b>水电表读数</b>、家具家电状态留档，退租时更好对照。\n\n"
        "<b>押金保障（方向性）：</b>在侨联经手的单子，退租时我们尽量作为<b>第三方协调与见证</b>，"
        "推动押金按约定合理结算（具体边界以书面为准）。\n\n"
        "<i>涉及具体权利义务、是否可提供见证工时与材料清单等，以在侨联签约时书面约定为准；"
        "在侨联签约客户适用条款见合同附录。</i>"
    )


def brand_text() -> str:
    return (
        f"📖 <b>{BRAND_NAME} 品牌故事</b>\n\n"
        f"<b>我们是谁</b>\n"
        f"{BRAND_NAME}扎根金边，专注华人租房市场，核心使命是：\n"
        "<b>让你更快看对房、签约更稳、入住更顺</b>。\n\n"
        "<b>我们的三个坚持</b>\n"
        "• <b>实拍先行</b>：每套房源都尽量先拍真实在租状态，你在帖里看到的就是真实情况\n"
        "• <b>费用透明</b>：水电按表还是包月、押付方式、物业费、网络安装——签前全部说清楚\n"
        "• <b>中文全程</b>：从第一次咨询、带看、合同确认、入住交接，到报修和退租，中文顾问一路跟着\n\n"
        "<b>品牌口号</b>\n"
        "您在金边的自己人\n"
        "看对房 · 签约稳 · 入住顺\n\n"
        "如果您刚开始看房，建议先点「智能找房」；如果已经看中了某套，直接点帖内「预约看房」最快。"
    )


def about_text() -> str:
    return (
        f"🏢 <b>关于{BRAND_NAME}</b>\n\n"
        f"{BRAND_NAME}是金边华人社区专业租房平台，核心定位是<b>您在金边的在地伙伴</b>。\n\n"
        "<b>我们的工作方式</b>\n"
        "• <b>房源先筛选</b>：按预算 + 区域 + 户型先收窄到 1–3 套，减少无效看房\n"
        "• <b>费用先对齐</b>：押付、水电、物业、网络等关键项在看房前尽量摊开说\n"
        "• <b>过程可追踪</b>：预约、咨询、入住、售后，统一由管理号持续跟进\n"
        "• <b>实拍可验证</b>：所有帖子均含实拍编号，入住后与帖内状态可对照\n\n"
        "您可以先从「智能找房」或「预约看房」开始，我们会一步步协助。"
    )


def want_home_text() -> str:
    return (
        "<b>📍 条件筛选</b>（高意向入口）\n\n"
        "这里默认走 <b>点击选择</b>，不让您反复打字。\n"
        "勾选完条件后点「提交条件」，系统会同步推送管理号，人工收窄到 1-3 套。\n\n"
        "可选条件包括：预算、区域、民水民电、停车、安静、采光、宠物、拎包、电梯/泳池等。"
    )
def service_promise_text() -> str:
    return (
        "<b>🛡️ 服务承诺（公开口径）</b>\n\n"
        "<b>1）看房无忧</b>\n"
        "免费安排看房是自然动作；没空到场，优先安排<b>实时视频代看</b>，您指到哪我们镜头跟到哪。\n\n"
        "<b>2）隐性成本摊开说</b>\n"
        "水电按表还是包、物业费谁出、网络是否需要自己拉、空调保养与停车等，"
        "我们在售前尽量给您<b>说清楚 + 写入材料</b>，减少后续沟通成本。\n\n"
        "<b>3）押金与留档</b>\n"
        "为后续<b>可执行的协调/见证</b>做准备：入住留档模板（全屋、表数、家电清单）我们建议标配。\n\n"
        "<b>4）管理号不断档</b>\n"
        "咨询、预约、条件筛选、入住后报修与物业沟通，一律同步推送管理号。\n\n"
        f"{deposit_text()}"
    )


def appointment_hub_text() -> str:
    return (
        "<b>📅 预约实拍 / 视频看房</b>\n\n"
        "如果您已经看中某套房，点帖子里的预约按钮会直接带上房源。\n"
        "如果您还没定房，也可以先约视频看房或实地看房，我来帮您继续收窄。\n\n"
        "流程保持最短：\n"
        "1）选方式\n"
        "2）选关注点\n"
        "3）选日期和时段\n"
        "4）提交给顾问跟进"
    )
def service_hub_text() -> str:
    return (
        "<b>🧰 租后服务说明</b>\n\n"
        "这里承接签约、入住、报修、水电网、物业沟通、续租 / 退租等事项。\n"
        "如果您已经在侨联租住，顾问会按您的当前租约继续跟进。"
    )


def help_text() -> str:
    return (
        "<b>📘 使用说明</b>\n\n"
        "<b>常用命令</b>\n"
        "<code>/start</code> — 回到首页\n"
        "<code>/find</code> — 快速找房（按钮向导）\n"
        "<code>/favorites</code> — 我的收藏\n"
        "<code>/appointments</code> — 我的预约\n"
        "<code>/contact</code> — 联系顾问\n"
        "<code>/help</code> — 本页\n\n"
        "<b>使用方式</b>\n"
        "• 建议优先使用页面按钮导航\n"
        "• 只有「🎲 一句话关键词找房」需要手动输入\n"
        "• 从频道帖子进入时，咨询与预约会自动绑定对应房源\n\n"
        "<b>补充说明</b>\n"
        "• 合同、押付、入住时间等问题，可直接转顾问人工确认\n"
        "• 看中频道房源，直接点帖内「咨询 / 预约」跟进最快\n"
        "• 老客可点下面入口登记，便于换房、续租与售后衔接"
    )


def help_repeat_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton("🏠 我以前在侨联租过（登记）", callback_data="profile:repeat")],
    ]
    ch = (CHANNEL_URL or "").strip()
    if ch:
        rows.append([InlineKeyboardButton("📢 频道实拍上新", url=ch)])
    return InlineKeyboardMarkup(rows)


def search_entry_intro_text() -> str:
    return (
        "<b>🏠 开始找房</b>\n\n"
        "您可以直接点按钮，也可以发一句话给我。\n"
        "例如：<code>BKK1 500以内 一房</code>、<code>钻石岛 两房</code>、<code>视频看房</code>\n\n"
        "如果您想更稳一点，就走按钮筛选；如果您想快一点，就直接发关键词。"
    )


def smart_find_play_prompt_text() -> str:
    return (
        "<b>🎲 一句话关键词</b>\n\n"
        "请直接发一句需求，无需固定格式。\n"
        "例如：<code>BKK1 预算800内 1房</code>\n"
        "也可以只发：<code>钻石岛</code>、<code>500以内</code>、<code>两房</code>、<code>视频看房</code>\n\n"
        "我会先按您说的方向筛一轮，再引导您继续缩小范围。"
    )


def smart_find_guided_header_text() -> str:
    return (
        "<b>📍 按类型找</b>\n\n"
        "按 <b>类型 → 区域 → 预算</b> 三步筛选。\n"
        "直接点下方按钮即可，无需手动输入。"
    )


def smart_find_play_footer_hint_text(*, used_fallback: bool) -> str:
    if used_fallback:
        return (
            "\n\n<i>以上先按近期在架房源推荐；可继续补充区域/预算/户型，我会再收窄结果。</i>"
        )
    return (
        "\n\n<i>如需更精准，可点菜单「🔍 智能找房」并走「按类型找」按钮流程。</i>"
    )


def repeat_tenant_ack_text() -> str:
    ch = (CHANNEL_URL or "").strip()
    ch_line = f"\n\n📢 实拍频道：<a href=\"{e(ch)}\">点这里关注上新</a>" if ch else ""
    return (
        "✅ <b>已登记为侨联老客回流</b>\n\n"
        "后台会记一条线索，顾问侧换房 / 续租 / 升级户型会优先衔接。"
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
    return "✅ <b>已为您筛出更匹配的房源</b>（优先展示可快速决策的少量选项）"


def listing_match_footer_text() -> str:
    return (
        "\n\n<b>下一步</b>：点菜单 <b>📅 预约看房</b> 直接约到场，或 <b>💎 咨询顾问</b> 让管理号帮你对比决策。"
    )


def find_no_match_text() -> str:
    return (
        "这个条件暂时没有完全匹配的在架房源。\n\n"
        "✅ <b>已通知顾问</b>，会优先为您盯新上的房\n\n"
        "💡 同时您可以：\n"
        "• 点「💬 直接咨询顾问」，人工帮您扩一圈推荐\n"
        "• 点「🎯 重新筛选」调整预算或区域，通常可以多出不少选项\n\n"
        "<i>您的需求已同步管理号，有新房上架第一时间跟进。</i>"
    )


def want_home_ack_text() -> str:
    return (
        "✅ <b>已收到您的找房条件</b>\n\n"
        "顾问会<b>人工收窄</b>到 1–3 套，并提前标注每套的关键费用项，方便您对比决策。\n\n"
        "💡 想加快：再补一下预算硬上限、是否需要民水民电、电梯/泳池需求，或者直接发截图也行。"
    )

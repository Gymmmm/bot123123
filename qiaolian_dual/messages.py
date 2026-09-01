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

    # 清理所有显示字段
    title = remove_test_markers(item.get('title', ''))
    listing_id = item.get('listing_id', '')
    area_raw = item.get('area', '')
    area = get_display_location(area_raw)
    community = remove_test_markers(item.get('community', ''))
    highlights = clean_telegram_text(item.get('highlights', ''))
    hidden_costs = clean_telegram_text(item.get('hidden_costs', ''))
    drawbacks = clean_telegram_text(item.get('drawbacks', ''))
    available_date = fix_duplicate_words(item.get('available_date', ''))

    # 同一楼盘在 area/community 中重复时，详情页只显示一次，避免“Vila Town · Vila Town”。
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
    """房源详情与预约流程共用的成交交付保障短模块。"""
    return (
        "\n🛡️ <b>看房与交付保障</b>\n"
        "看中后：费用逐项核对。\n"
        "入住时：验房、水电表和钥匙/门卡确认留档。"
    )


def discussion_entry_welcome_text(first_name: str = "", listing_id: str = "") -> str:
    """讨论区深链进入文案 - 精简版"""
    from .text_utils import remove_test_markers

    # 深链落地页已废弃，深链现在直达动作
    return ""


def lead_capture_text() -> str:
    """关键行为完成后的 Telegram 承接文案。"""
    return (
        "侨联顾问会直接通过 Telegram 接手，"
        "你不用重复填写联系方式或重新说明需求。"
    )


def advisor_text() -> str:
    """联系我们页面"""
    return (
        "✅ <b>顾问已收到</b>\n\n"
        "顾问会通过 Telegram 联系你。\n"
        "这套房的信息已带上，不用重复说明。"
    )


def advisor_contact_supplement_text() -> str:
    return (
        "顾问会直接通过 Telegram 联系你，无需另外填写手机号或微信。"
    )


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
    """首页只说明客户能做什么；具体房态放在各套房源里。"""
    return (
        "🏠 <b>侨联找房助手｜小彭</b>\n\n"
        "金边租房，先选你现在要做的事。"
    )


def channel_welcome_text(first_name: str = "") -> str:
    """带称呼的统一首页文案；不依赖易失效的字符串替换。"""
    if not first_name:
        return home_text()
    return f"👋 你好 <b>{e(first_name)}</b>。\n\n{home_text()}"


def about_text() -> str:
    """关于侨联地产"""
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
    """关于侨联"""
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
    """服务承诺"""
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
        "咨询：点“咨询这套”，房源信息会自动带上。\n"
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
    return (
        "🏠 <b>想找什么房？</b>\n\n选一个大概类型就行。"
    )


def smart_find_play_prompt_text() -> str:
    return (
        "🎲 <b>一句话关键词</b>\n\n"
        "请直接发一句需求，无需固定格式。\n"
        "例如：<code>BKK1 预算800内 1房</code>\n"
        "也可以只发：<code>钻石岛</code>、<code>500以内</code>、<code>两房</code>、<code>视频看房</code>\n\n"
        "我会先按你说的方向筛一轮，再引导你继续缩小范围。"
    )


def smart_find_guided_header_text() -> str:
    return (
        "🏠 <b>想找什么房？</b>\n\n选一个大概类型就行。"
    )


def smart_find_play_footer_hint_text(*, used_fallback: bool) -> str:
    if used_fallback:
        return "\n\n先给你看接近的房源，还可以继续调整位置或预算。"
    return "\n\n如需更精准，可点菜单「🔍 智能找房」并走「按类型找」按钮流程。"


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
        "\n\n<b>下一步</b>：点「📅 预约看房」安排到场，或点「💬 联系顾问」帮你对比选择。"
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


# ─── 周边生活 / 富力周边便民导航 ────────────────────────────────────────────

_RFCITY_FOOTER = (
    "\n\n💡 点击用户名即可直接联系商家\n"
    "✍️ 有好店想补充，可以提交给侨联\n\n"
    "信息会持续更新，具体价格和服务以商家实际回复为准。"
)


def local_life_text() -> str:
    return (
        "🧭 <b>周边服务</b>\n\n"
        "为方便已入住客户，我们会逐步整理各区域常用生活信息：\n\n"
        "• 餐饮 / 超市 / 快递\n"
        "• 物业 / 搬家 / 维修\n"
        "• 医院 / 药店 / 日常服务\n\n"
        "具体价格和服务以商家实际回复为准；需要时也可直接联系顾问。"
    )


def rfcity_text() -> str:
    return (
        "🏙 <b>R&amp;F City 便民导航</b>\n\n"
        "富力生活常用服务都在这里：\n"
        "吃饭、超市、快递、物业、酒店、休闲。\n\n"
        "先看房，也可以先看生活是否方便。"
    )


def rfcity_restaurant_text() -> str:
    return (
        "🍴 <b>富力餐厅 · 小吃</b>\n\n"
        "小明菜煎饼：@XMCaiJianBing\n"
        "金饭碗融合食：@JFW_8888\n"
        "木森快餐：@hei32567\n"
        "邻居家盒饭：@linjujia8899\n"
        "麻了个面：@fq666520\n"
        "兰州拉面：@LZLM_RF\n"
        "云南老妈米线：+855962510133\n"
        "重庆小面：@CY_ccxm\n"
        "味之道重庆小面：@WZD8889\n"
        "川妹子餐厅：@cuan_meizi\n"
        "A4沙县小吃：+855964039606\n"
        "太二酸菜鱼：@taiersuancaiyu\n"
        "幺妹麻辣烫：@Ruilin585\n"
        "沙县小吃(正门)：@cheng1149\n"
        "猪事顺杀猪粉：@zssflzd\n"
        "广州海鲜城富力店：+855016248811\n"
        "麦德仕汉堡炸鸡：@MDS0188838388\n"
        "川遇菜馆：+8550969794108\n"
        "小仙女手工凉皮：@ba521520\n"
        "鼎阁重庆老火锅：@xh918888\n"
        "四海食府：待补充\n"
        "羊汤一品：+8550883019759\n"
        "麻小姬·麻椒鸡：@Wwen52025"
        + _RFCITY_FOOTER
    )


def rfcity_bbq_text() -> str:
    return (
        "🔥 <b>富力烧烤 · 夜宵</b>\n\n"
        "留一手烤鱼：@clgxyxy\n"
        "东北吉林烧烤：@Jinniu99998888\n"
        "江湖烧烤：@jianghushaokao\n"
        "A8烤鹅翅：@FUAN68899"
        + _RFCITY_FOOTER
    )


def rfcity_drinks_text() -> str:
    return (
        "🥤 <b>富力奶茶 · 饮品</b>\n\n"
        "麦诺咖啡：@mnppsc\n"
        "霸王茶姬：@Jolyne777\n"
        "A8 ManMan 糖水饮品：@manmanC3121"
        + _RFCITY_FOOTER
    )


def rfcity_supermarket_text() -> str:
    return (
        "🛒 <b>富力超市 · 便利店</b>\n\n"
        "喜来优品超市：@xilai1818\n"
        "够意思超市：@gouyisi\n"
        "文轩888便利店：@WENXUAN188\n"
        "富田生鲜超市：@FUTIAN668899\n"
        "中柬易购生活超市：@Yin_zhuochao\n"
        "糖巢省钱超市：@WGTC99\n"
        "叮当猫百货伟哥数码：@yuna666666\n"
        "如意烟酒：@w1025\n"
        "1919商行(烟酒茶)：@FL191919\n"
        "庆丰优选超市：@gtffgfffdff\n"
        "B11世纪超市：@b11shijichaoshi"
        + _RFCITY_FOOTER
    )


def rfcity_hotel_text() -> str:
    return (
        "🏨 <b>富力酒店 · 租房</b>\n\n"
        "橙乐酒店：@FlMinsu2025\n"
        "富力酒店：@RF_Hotel\n"
        "美辰地产富力店：@pengqingw"
        + _RFCITY_FOOTER
    )


def rfcity_recreation_text() -> str:
    return (
        "🏋️ <b>富力运动 · 休闲生活</b>\n\n"
        "富力体育会所：@Sportcity1098\n"
        "泰自然按摩店：@taiziran01\n"
        "茜茜美容SPA：@d11631876\n"
        "东方贵足：+855965840694\n"
        "高棉城市按摩24小时：+855089355788\n"
        "A4理发店：+855968455609\n"
        "A7理发店：待补充\n"
        "理享美容美发沙龙：+855963781029\n"
        "A5美甲店：+85593626126\n"
        "安妮奢侈品回收典当：@anne168777\n"
        "宠物之家：@motopet188\n"
        "奢依阁男装：@SYG666888"
        + _RFCITY_FOOTER
    )


def rfcity_logistics_text() -> str:
    return (
        "🚛 <b>富力快递 · 物流</b>\n\n"
        "YA速递富力站：@yaexpres\n"
        "CE速递：@CECS006\n"
        "中通快递：+85566666280"
        + _RFCITY_FOOTER
    )


def rfcity_property_text() -> str:
    return (
        "👨‍💻 <b>富力物业</b>\n\n"
        "👨‍💻 富力物业24小时：@rfservice24\n"
        "🏢 富力会客厅：+85569927771"
        + _RFCITY_FOOTER
    )


def merchant_join_text() -> str:
    return (
        "🤝 <b>富力商家信息补充</b>\n\n"
        "如果你在富力周边提供餐饮、超市、维修、搬家、快递、酒店或其他日常服务，"
        "可以联系侨联补充信息。\n\n"
        "我们会优先整理对已入住客户有实际帮助的服务。\n\n"
        "请提供：\n"
        "• 店名和服务类别\n"
        "• Telegram / 电话\n"
        "• 详细位置和营业时间\n"
        "• 服务说明\n\n"
        "信息核实后会逐步更新到周边导航。"
    )


def smart_search_text() -> str:
    """智能找房页文案。"""
    return (
        "🏠 <b>想找什么房？</b>\n\n选一个大概类型就行。"
    )


def consult_submit_ok_text() -> str:
    """咨询提交成功文案。"""
    return (
        "✅ 已收到你的咨询\n"
        "顾问会尽快通过 Telegram 回复你。\n"
        "也可点下方直接预约看房。"
    )


def appoint_entry_text() -> str:
    """预约流程第一步文案。"""
    return (
        "📅 已为你进入预约流程\n"
        "先选看房方式："
    )


def appoint_success_text() -> str:
    """预约提交后的统一标题/跟进说明。"""
    return (
        "✅ <b>预约申请已提交</b>\n\n"
        "顾问确认房态和时间后，\n"
        "会通过 Telegram 联系你。"
    )


def advisor_notify_ok_text() -> str:
    """手动呼叫顾问成功提示。"""
    return (
        "✅ 已通知顾问\n"
        "顾问会尽快在 Telegram 联系你，请留意消息。"
    )


def handoff_find_ok_text() -> str:
    """让顾问帮我找成功提示。"""
    return (
        "✅ 已收到你的找房需求\n"
        "发区域、预算或户型中的任意一项即可，顾问会继续帮你缩小范围。"
    )


def repair_progress_text(issue_label: str, stage: str, note: str = "") -> str:
    """报修进度通知：让客户知道事情到哪一步，也知道下一步怎么做。"""
    stage_lines = {
        "accepted": "已经有顾问接手，正在确认处理安排。",
        "scheduled": "已经安排处理。时间有变化，我们会第一时间告诉你。",
        "in_progress": "正在处理。处理完我们会再跟你说一声。",
        "done": "已经处理完成。方便的话，帮忙确认一下现在是否正常。",
        "need_info": "还需要你补充一点信息，顾问会在 Telegram 找你确认。",
    }
    detail = str(note or "").strip()
    lines = ["🔧 <b>报修进度更新</b>", "", e(issue_label or "报修事项"), stage_lines.get(stage, "顾问正在跟进处理。")]
    if detail:
        lines.extend(["", f"补充：{e(detail)}"])
    lines.extend(["", "有新的情况，直接回复这条消息告诉我们就行。"])
    return "\n".join(lines)


def advisor_response_notice_text() -> str:
    """顾问接手预约后的统一提醒。"""
    return (
        "💬 <b>顾问已接手</b>\n\n"
        "你的预约和房源信息已经一起发给顾问。\n"
        "顾问会通过 Telegram 联系你确认。\n\n"
        "时间或需求有变化，\n"
        "直接回复这条消息就可以。"
    )


def legacy_callback_degraded_text() -> str:
    """历史回调降级提示。"""
    return "该入口已升级，请从首页重新选择找房方式。"

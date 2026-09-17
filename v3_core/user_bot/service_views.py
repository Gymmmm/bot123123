"""Locked-copy V3 views for current tenant-service navigation."""
from __future__ import annotations

from dataclasses import dataclass
from html import escape as he

from .service_flow import ServiceRequestDraft


@dataclass(frozen=True)
class ServiceChoice:
    label: str
    callback_data: str


@dataclass(frozen=True)
class ServiceView:
    kind: str
    text: str
    rows: tuple[tuple[ServiceChoice, ...], ...]


def service_home_view() -> ServiceView:
    return ServiceView(
        kind="service_home",
        text="🛠 <b>入住服务</b>\n\n已入住后的住房问题，可以从这里继续处理。",
        rows=(
            (ServiceChoice("🔧 报修与维护", "v3u:service:repair"), ServiceChoice("🏢 物业沟通", "v3u:service:property")),
            (ServiceChoice("🚚 搬家协助", "v3u:assure:moving"), ServiceChoice("🧭 周边服务", "v3u:service:local")),
            (ServiceChoice("💬 中文顾问", "v3u:home:contact"), ServiceChoice("🏠 返回首页", "v3u:t:home")),
        ),
    )


def repair_home_view() -> ServiceView:
    return ServiceView(
        kind="repair_home",
        text="🔧 <b>报修与维护</b>\n\n请选择问题类型。下一步请发送文字说明问题。",
        rows=(
            (ServiceChoice("❄️ 空调", "v3u:service:issue:repair_ac"), ServiceChoice("🚿 热水 / 漏水", "v3u:service:issue:repair_water")),
            (ServiceChoice("💡 灯具 / 电路", "v3u:service:issue:repair_power"), ServiceChoice("🔐 门锁 / 门禁", "v3u:service:issue:repair_door")),
            (ServiceChoice("🧺 洗衣机", "v3u:service:issue:repair_washer"), ServiceChoice("🧊 冰箱", "v3u:service:issue:repair_fridge")),
            (ServiceChoice("📶 网络", "v3u:service:issue:repair_network"), ServiceChoice("🪑 家具损坏", "v3u:service:issue:repair_furniture")),
            (ServiceChoice("🔧 其他问题", "v3u:service:issue:repair_other"),),
            (ServiceChoice("⬅️ 返回入住服务", "v3u:home:service"),),
        ),
    )


def property_view() -> ServiceView:
    return ServiceView(
        kind="property",
        text=(
            "🏢 <b>物业沟通</b>\n\n"
            "噪音、停车、门禁、公共区域、垃圾处理等需要物业协调的问题，可以直接联系中文顾问。\n\n"
            "说明 <b>发生了什么 + 大概时间 + 是否已经联系过物业</b>，我们会协助整理并跟进。"
        ),
        rows=((ServiceChoice("💬 中文顾问", "v3u:home:contact"),), (ServiceChoice("⬅️ 返回入住服务", "v3u:home:service"),)),
    )


def issue_prompt_view(draft: ServiceRequestDraft) -> ServiceView:
    urgent = draft.issue_key in {"repair_water", "repair_power", "repair_door"}
    note = "\n\n如涉及持续漏水、断电或无法正常进出，请同时直接联系中文顾问。" if urgent else ""
    return ServiceView(
        kind="repair_issue",
        text=(
            f"🔧 <b>{he(draft.issue_label)}</b>\n\n"
            "请直接发送文字说明问题。\n"
            f"例如：<code>空调可以启动，但一直不制冷。</code>{note}"
        ),
        rows=((ServiceChoice("💬 中文顾问", "v3u:home:contact"),), (ServiceChoice("⬅️ 重新选择问题", "v3u:service:repair"),)),
    )


def slot_view(draft: ServiceRequestDraft) -> ServiceView:
    return ServiceView(
        kind="repair_slot",
        text=f"✅ <b>问题已记录</b>\n\n{he(draft.detail[:500])}\n\n请选择方便处理的时间：",
        rows=(
            (ServiceChoice("今天内", "v3u:service:slot:today"), ServiceChoice("明天上午", "v3u:service:slot:tomorrow_am")),
            (ServiceChoice("明天下午", "v3u:service:slot:tomorrow_pm"),),
            (ServiceChoice("⬅️ 返回入住服务", "v3u:home:service"),),
        ),
    )


def repair_success_view(*, urgent: bool) -> ServiceView:
    urgent_note = "\n\n如情况仍在扩大，请直接联系中文顾问。" if urgent else ""
    return ServiceView(
        kind="repair_success",
        text="✅ <b>报修已提交</b>\n\n顾问会根据您提交的问题和时间安排后续处理。" + urgent_note,
        rows=((ServiceChoice("💬 中文顾问", "v3u:home:contact"),), (ServiceChoice("⬅️ 返回入住服务", "v3u:home:service"),)),
    )


def general_prompt_view(*, nearby: bool = False) -> ServiceView:
    if nearby:
        return ServiceView(
            kind="nearby_prompt",
            text="📍 <b>其他区域需求</b>\n\n发送区域或地标，再告诉我们需要找什么，例如餐饮、超市、交通或其他生活服务。",
            rows=((ServiceChoice("⬅️ 返回周边推荐", "v3u:service:nearby"),),),
        )
    return ServiceView(
        kind="general_prompt",
        text="💬 <b>其他需求</b>\n\n直接发送需要处理的事情，顾问会按您这条内容继续跟进。",
        rows=((ServiceChoice("💬 中文顾问", "v3u:home:contact"),), (ServiceChoice("⬅️ 返回入住服务", "v3u:home:service"),)),
    )


def general_success_view(*, nearby: bool = False) -> ServiceView:
    text = "✅ <b>周边需求已收到</b>\n\n顾问会根据您提交的区域和需求回复。" if nearby else "✅ <b>需求已收到</b>\n\n顾问会根据您刚才提交的内容继续跟进。"
    return ServiceView(kind="general_success", text=text, rows=((ServiceChoice("⬅️ 返回入住服务", "v3u:home:service"),),))


def local_life_view() -> ServiceView:
    return ServiceView(
        kind="local_life",
        text=(
            "🧭 <b>周边服务</b>\n\n"
            "入住后常用的生活服务可以从这里找：\n\n"
            "• 餐饮 / 超市 / 快递\n"
            "• 保洁 / 搬家 / 维修\n"
            "• 医院 / 药店 / 其他日常服务\n\n"
            "商家信息会持续更新，价格和实际服务以商家回复为准。"
        ),
        rows=(
            (ServiceChoice("🧹 保洁家政", "v3u:home:contact"), ServiceChoice("🚚 搬家协助", "v3u:assure:moving")),
            (ServiceChoice("🗺 周边推荐", "v3u:service:nearby"), ServiceChoice("💬 其他需求", "v3u:service:general")),
            (ServiceChoice("⬅️ 返回入住服务", "v3u:home:service"),),
        ),
    )


def nearby_view() -> ServiceView:
    return ServiceView(
        kind="nearby",
        text="🗺 <b>周边推荐</b>\n\n目前可直接查看富力城已整理的生活商家；其他区域可以提交位置和需求。",
        rows=(
            (ServiceChoice("🏙 富力城导航", "v3u:service:rfcity"),),
            (ServiceChoice("📍 提交其他区域", "v3u:service:nearby_other"),),
            (ServiceChoice("⬅️ 返回周边服务", "v3u:service:local"),),
        ),
    )


_RFCITY_FOOTER = (
    "\n\n💡 点击用户名即可直接联系商家\n"
    "✍️ 有好店想补充，可以提交给侨联\n\n"
    "信息会持续更新，具体价格和服务以商家实际回复为准。"
)

_RFCITY_TEXTS = {
    "restaurant": "🍴 <b>富力餐厅 · 小吃</b>\n\n小明菜煎饼：@XMCaiJianBing\n金饭碗融合食：@JFW_8888\n木森快餐：@hei32567\n邻居家盒饭：@linjujia8899\n麻了个面：@fq666520\n兰州拉面：@LZLM_RF\n云南老妈米线：+855962510133\n重庆小面：@CY_ccxm\n味之道重庆小面：@WZD8889\n川妹子餐厅：@cuan_meizi\nA4沙县小吃：+855964039606\n太二酸菜鱼：@taiersuancaiyu\n幺妹麻辣烫：@Ruilin585\n沙县小吃(正门)：@cheng1149\n猪事顺杀猪粉：@zssflzd\n广州海鲜城富力店：+855016248811\n麦德仕汉堡炸鸡：@MDS0188838388\n川遇菜馆：+8550969794108\n小仙女手工凉皮：@ba521520\n鼎阁重庆老火锅：@xh918888\n四海食府：待补充\n羊汤一品：+8550883019759\n麻小姬·麻椒鸡：@Wwen52025",
    "bbq": "🔥 <b>富力烧烤 · 夜宵</b>\n\n留一手烤鱼：@clgxyxy\n东北吉林烧烤：@Jinniu99998888\n江湖烧烤：@jianghushaokao\nA8烤鹅翅：@FUAN68899",
    "drinks": "🥤 <b>富力奶茶 · 饮品</b>\n\n麦诺咖啡：@mnppsc\n霸王茶姬：@Jolyne777\nA8 ManMan 糖水饮品：@manmanC3121",
    "supermarket": "🛒 <b>富力超市 · 便利店</b>\n\n喜来优品超市：@xilai1818\n够意思超市：@gouyisi\n文轩888便利店：@WENXUAN188\n富田生鲜超市：@FUTIAN668899\n中柬易购生活超市：@Yin_zhuochao\n糖巢省钱超市：@WGTC99\n钉当猫百货伟哥数码：@yuna666666\n如意烟酒：@w1025\n1919商行(烟酒茶)：@FL191919\n庆丰优选超市：@gtffgfffdff\nB11世纪超市：@b11shijichaoshi",
    "hotel": "🏨 <b>富力酒店 · 租房</b>\n\n橙乐酒店：@FlMinsu2025\n富力酒店：@RF_Hotel\n美辰地产富力店：@pengqingw",
    "recreation": "🏋️ <b>富力运动 · 休闲生活</b>\n\n富力体育会所：@Sportcity1098\n泰自然按摩店：@taiziran01\n茵茵美容SPA：@d11631876\n东方贵足：+855965840694\n高棉城市按摩24小时：+855089355788\nA4理发店：+855968455609\nA7理发店：待补充\n理享美容美发沙龙：+855963781029\nA5美甲店：+85593626126\n安妮奢侈品回收典当：@anne168777\n宠物之家：@motopet188\n奢依阁男装：@SYG666888",
    "logistics": "🚚 <b>富力快递 · 物流</b>\n\nYA速递富力站：@yaexpres\nCE速递：@CECS006\n中通快递：+85566666280",
    "property": "👨‍💻 <b>富力物业</b>\n\n👨‍💻 富力物业24小时：@rfservice24\n🏢 富力会客厅：+85569927771",
}


def rfcity_home_view() -> ServiceView:
    return ServiceView(
        kind="rfcity",
        text="🏙 <b>富力城生活导航</b>\n\n选择分类查看已整理的商家联系方式。",
        rows=(
            (ServiceChoice("🍴 餐厅小吃", "v3u:service:rfcity:restaurant"), ServiceChoice("🔥 烧烤夜宵", "v3u:service:rfcity:bbq")),
            (ServiceChoice("🥤 奶茶饮品", "v3u:service:rfcity:drinks"), ServiceChoice("🛒 超市便利", "v3u:service:rfcity:supermarket")),
            (ServiceChoice("🏨 酒店租房", "v3u:service:rfcity:hotel"), ServiceChoice("🏋️ 运动休闲", "v3u:service:rfcity:recreation")),
            (ServiceChoice("🚚 快递物流", "v3u:service:rfcity:logistics"), ServiceChoice("👨‍💻 物业", "v3u:service:rfcity:property")),
            (ServiceChoice("⬅️ 返回周边推荐", "v3u:service:nearby"),),
        ),
    )


def rfcity_category_view(category: str) -> ServiceView:
    clean = str(category or "").strip().lower()
    text = _RFCITY_TEXTS.get(clean)
    if text is None:
        raise ValueError("unsupported_rfcity_category")
    return ServiceView(kind=f"rfcity_{clean}", text=text + _RFCITY_FOOTER, rows=())


__all__ = [
    "ServiceChoice", "ServiceView", "general_prompt_view", "general_success_view", "issue_prompt_view",
    "local_life_view", "nearby_view", "property_view", "repair_home_view", "repair_success_view",
    "rfcity_category_view", "rfcity_home_view", "service_home_view", "slot_view",
]

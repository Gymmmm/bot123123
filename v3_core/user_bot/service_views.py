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
        text=(
            "🛠 <b>入住服务</b>\n\n"
            "已经租下来的事，从这里找我们。\n"
            "报修、物业、搬家，点一项就行。"
        ),
        rows=(
            (
                ServiceChoice("🔧 报修", "v3u:service:repair"),
                ServiceChoice("🏢 找物业", "v3u:service:property"),
            ),
            (
                ServiceChoice("🚚 搬家", "v3u:assure:moving"),
                ServiceChoice("📍 周边", "v3u:service:local"),
            ),
            (
                ServiceChoice("💬 联系我们", "v3u:home:contact"),
                ServiceChoice("🏠 返回首页", "v3u:t:home"),
            ),
        ),
    )


def repair_home_view() -> ServiceView:
    return ServiceView(
        kind="repair_home",
        text="🔧 <b>设备报修</b>\n\n请选择出现问题的设备。",
        rows=(
            (
                ServiceChoice("❄️ 空调", "v3u:service:issue:repair_ac"),
                ServiceChoice("🚿 热水器", "v3u:service:issue:repair_water"),
            ),
            (
                ServiceChoice("🧺 洗衣机", "v3u:service:issue:repair_washer"),
                ServiceChoice("🧊 冰箱", "v3u:service:issue:repair_fridge"),
            ),
            (
                ServiceChoice("📶 网络", "v3u:service:issue:repair_network"),
                ServiceChoice("🔐 门锁/门禁", "v3u:service:issue:repair_door"),
            ),
            (ServiceChoice("🔧 其他设备", "v3u:service:issue:repair_other"),),
            (ServiceChoice("⬅️ 返回", "v3u:home:service"),),
        ),
    )


def property_view() -> ServiceView:
    return ServiceView(
        kind="property",
        text=(
            "🏢 <b>物业协调</b>\n\n"
            "如遇噪音、停车、门禁、公共区域或垃圾处理等问题，可以直接说明具体情况。\n\n"
            "建议包括：\n"
            "• 发生了什么\n"
            "• 大概从什么时候开始\n"
            "• 是否已经与物业沟通过\n\n"
            "我们会协助整理沟通重点，并根据实际情况对接物业。"
        ),
        rows=(
            (ServiceChoice("💬 联系我们", "v3u:home:contact"),),
            (ServiceChoice("⬅️ 返回入住服务", "v3u:home:service"),),
        ),
    )


def issue_prompt_view(draft: ServiceRequestDraft) -> ServiceView:
    urgent = draft.issue_key in {"repair_water", "repair_power", "repair_door"}
    note = "\n\n如果情况紧急，请直接联系我们。" if urgent else ""
    return ServiceView(
        kind="repair_issue",
        text=(
            f"🔧 <b>{he(draft.issue_label)}</b>\n\n"
            "请发送问题照片或短视频，并简单说明异常情况。\n"
            f"例如：<code>空调可以启动，但一直不制冷。</code>{note}"
        ),
        rows=(
            (ServiceChoice("💬 联系我们", "v3u:home:contact"),),
            (ServiceChoice("⬅️ 返回", "v3u:service:repair"),),
        ),
    )


def slot_view(draft: ServiceRequestDraft) -> ServiceView:
    return ServiceView(
        kind="repair_slot",
        text=f"✅ 已记录：{he(draft.detail[:500])}\n\n选希望处理的时间：",
        rows=(
            (
                ServiceChoice("🚨 今天内", "v3u:service:slot:today"),
                ServiceChoice("🔨 明天上午", "v3u:service:slot:tomorrow_am"),
            ),
            (ServiceChoice("🕔 明天下午", "v3u:service:slot:tomorrow_pm"),),
            (ServiceChoice("⬅️ 返回入住服务", "v3u:home:service"),),
        ),
    )


def repair_success_view(*, urgent: bool) -> ServiceView:
    urgent_note = "\n\n如果情况紧急，请直接联系我们。" if urgent else ""
    return ServiceView(
        kind="repair_success",
        text=(
            "✅ <b>报修信息已收到</b>\n\n"
            "我们会根据您提供的情况进行整理，并协助对接后续处理。"
            f"{urgent_note}"
        ),
        rows=(
            (ServiceChoice("💬 联系我们", "v3u:home:contact"),),
            (ServiceChoice("⬅️ 返回设备报修", "v3u:service:repair"),),
        ),
    )


def general_prompt_view(*, nearby: bool = False) -> ServiceView:
    if nearby:
        return ServiceView(
            kind="nearby_prompt",
            text=(
                "📍 <b>其他区域需求提交</b>\n\n"
                "请发送区域或地标，以及需要的餐饮、超市、交通或生活服务。提交后，我们会在1个工作日内回复。"
            ),
            rows=((ServiceChoice("⬅️ 返回周边推荐", "v3u:service:nearby"),),),
        )
    return ServiceView(
        kind="general_prompt",
        text="💬 <b>通用服务咨询</b>\n\n请直接说需要什么帮助，我们会按服务需求跟进。",
        rows=(
            (ServiceChoice("💬 联系我们", "v3u:home:contact"),),
            (ServiceChoice("⬅️ 返回入住服务", "v3u:home:service"),),
        ),
    )


def general_success_view(*, nearby: bool = False) -> ServiceView:
    text = (
        "✅ 已收到您的周边需求\n\n我们会在1个工作日内回复。"
        if nearby
        else "✅ 已收到您的需求\n\n顾问会根据这条内容联系您。"
    )
    return ServiceView(
        kind="general_success",
        text=text,
        rows=((ServiceChoice("⬅️ 返回入住服务", "v3u:home:service"),),),
    )


def local_life_view() -> ServiceView:
    return ServiceView(
        kind="local_life",
        text=(
            "🧭 <b>周边服务</b>\n\n"
            "为方便已入住客户，我们会逐步整理各区域常用生活信息：\n\n"
            "• 餐饮 / 超市 / 快递\n"
            "• 物业 / 搬家 / 维修\n"
            "• 医院 / 药店 / 日常服务\n\n"
            "具体价格和服务以商家实际回复为准；需要时也可直接联系顾问。"
        ),
        rows=(
            (
                ServiceChoice("🧹 保洁家政", "v3u:home:contact"),
                ServiceChoice("🚚 搬家协助", "v3u:assure:moving"),
            ),
            (
                ServiceChoice("🗺 周边推荐", "v3u:service:nearby"),
                ServiceChoice("💬 其他需求", "v3u:service:general"),
            ),
            (ServiceChoice("⬅️ 返回", "v3u:home:service"),),
        ),
    )


def nearby_view() -> ServiceView:
    return ServiceView(
        kind="nearby",
        text=(
            "🗺 <b>周边服务</b>\n\n"
            "可查看富力城已核实的周边商家；其他区域可提交需求，我们会在1个工作日内反馈。"
        ),
        rows=(
            (ServiceChoice("🏙 富力城导航", "v3u:service:rfcity"),),
            (ServiceChoice("📍 其他区域需求提交", "v3u:service:nearby_other"),),
            (ServiceChoice("⬅️ 返回生活服务", "v3u:service:local"),),
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
        kind="rfcity_home",
        text=(
            "🏙 <b>R&amp;F City 便民导航</b>\n\n"
            "富力生活常用服务都在这里：\n"
            "吃饭、超市、快递、物业、酒店、休闲。\n\n"
            "先看房，也可以先看生活是否方便。"
        ),
        rows=(
            (
                ServiceChoice("🍴 餐厅小吃", "v3u:service:rfcity:restaurant"),
                ServiceChoice("🔥 烧烤夜宵", "v3u:service:rfcity:bbq"),
            ),
            (
                ServiceChoice("🥤 奶茶饮品", "v3u:service:rfcity:drinks"),
                ServiceChoice("🛒 超市便利", "v3u:service:rfcity:supermarket"),
            ),
            (
                ServiceChoice("🏨 酒店租房", "v3u:service:rfcity:hotel"),
                ServiceChoice("🏋️ 休闲生活", "v3u:service:rfcity:recreation"),
            ),
            (
                ServiceChoice("🚛 快递物流", "v3u:service:rfcity:logistics"),
                ServiceChoice("👨‍💻 富力物业", "v3u:service:rfcity:property"),
            ),
            (ServiceChoice("⬅️ 返回生活服务", "v3u:service:local"),),
        ),
    )


def rfcity_category_view(category: str) -> ServiceView:
    clean = str(category or "").strip().lower()
    text = _RFCITY_TEXTS.get(clean)
    if text is None:
        raise ValueError("unsupported_rfcity_category")
    return ServiceView(
        kind="rfcity_category",
        text=text + _RFCITY_FOOTER,
        rows=((ServiceChoice("⬅️ 返回生活服务", "v3u:service:local"),),),
    )


__all__ = [
    "ServiceChoice",
    "ServiceView",
    "general_prompt_view",
    "general_success_view",
    "issue_prompt_view",
    "local_life_view",
    "nearby_view",
    "property_view",
    "repair_home_view",
    "repair_success_view",
    "rfcity_category_view",
    "rfcity_home_view",
    "service_home_view",
    "slot_view",
]

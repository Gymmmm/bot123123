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
            "房子定下来以后，入住和居住过程中需要处理的事情，可以从这里找侨联。"
        ),
        rows=(
            (ServiceChoice("📋 入住交接留档", "v3u:assure:handover"), ServiceChoice("🚚 搬家协助", "v3u:assure:moving")),
            (ServiceChoice("🔧 房屋问题报修", "v3u:service:repair"), ServiceChoice("🏢 物业沟通", "v3u:service:property")),
            (ServiceChoice("📍 周边生活", "v3u:service:local"), ServiceChoice("💬 中文顾问", "v3u:home:contact")),
            (ServiceChoice("⬅️ 回首页", "v3u:t:home"),),
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
            "噪音、停车、门禁、公共区域、垃圾处理等需要物业协调的问题，可以联系中文顾问。\n\n"
            "说明 <b>发生了什么 + 大概时间 + 是否已经联系过物业</b>，我们会协助整理并跟进。"
        ),
        rows=((ServiceChoice("💬 中文顾问", "v3u:home:contact"),), (ServiceChoice("⬅️ 返回入住服务", "v3u:home:service"),)),
    )


def issue_prompt_view(draft: ServiceRequestDraft) -> ServiceView:
    urgent = draft.issue_key in {"repair_water", "repair_power", "repair_door"}
    note = "\n\n如涉及持续漏水、断电或无法正常进出，请同时联系中文顾问。" if urgent else ""
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
    urgent_note = "\n\n如情况仍在扩大，请联系中文顾问。" if urgent else ""
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
            "🗺 <b>金边华人生活配套</b>\n\n"
            "侨联逐步整理各区域常用生活信息：\n\n"
            "• 中餐 / 夜宵\n"
            "• 超市 / 送货 / 搬家\n"
            "• 洗衣 / 保洁 / 维修\n"
            "• 医院 / 药店\n"
            "• 其他日常生活服务"
        ),
        rows=(
            (ServiceChoice("🏙 富力城周边", "v3u:service:rfcity"),),
            (ServiceChoice("📍 其他区域需求", "v3u:service:nearby_other"),),
            (ServiceChoice("⬅️ 返回入住服务", "v3u:home:service"),),
        ),
    )


def nearby_view() -> ServiceView:
    view = local_life_view()
    return ServiceView(kind="nearby", text=view.text, rows=view.rows)


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


# Final surface overrides. Kept at the bottom so legacy helpers remain available
# without exposing their old copy/buttons.
def service_home_view() -> ServiceView:
    return ServiceView(
        kind="service_home",
        text=(
            "<b>侨联服务</b>\n\n"
            "租房不只是找房和签约。入住后的房屋与物业事项、租约查看、"
            "交接留档以及周边生活，都可以从这里继续找侨联。\n\n"
            "请选择您需要了解或处理的服务事项。"
        ),
        rows=(
            (ServiceChoice("我的租约", "v3u:service:tenant_lease"), ServiceChoice("入住管家", "v3u:service:concierge")),
            (ServiceChoice("安心租房", "v3u:home:rental"), ServiceChoice("周边生活", "v3u:service:local")),
            (ServiceChoice("中文顾问", "v3u:home:contact"),),
            (ServiceChoice("回首页", "v3u:t:home"),),
        ),
    )


def concierge_home_view() -> ServiceView:
    return ServiceView(
        kind="concierge_home",
        text=(
            "<b>入住管家</b>\n\n"
            "住进去以后，报修、物业、水电网络这些日常事务难免会碰到。\n\n"
            "把情况从这里告知侨联，Bot 先帮您整理好信息，需要继续处理时再交给中文顾问对接。"
        ),
        rows=(
            (ServiceChoice("房屋报修", "v3u:service:repair"), ServiceChoice("物业协调", "v3u:service:property")),
            (ServiceChoice("水电缴费协助", "v3u:service:utilities"), ServiceChoice("搬家协助", "v3u:service:moving")),
            (ServiceChoice("保洁服务", "v3u:service:cleaning"), ServiceChoice("网络协助", "v3u:service:network_help")),
            (ServiceChoice("其他住房问题", "v3u:service:general"),),
            (ServiceChoice("返回侨联服务", "v3u:home:service"),),
        ),
    )


def repair_home_view() -> ServiceView:
    return ServiceView(
        kind="repair_home",
        text="<b>房屋报修</b>\n\n请选择问题类型。",
        rows=(
            (ServiceChoice("空调状况", "v3u:service:issue:repair_ac"), ServiceChoice("热水漏水", "v3u:service:issue:repair_water")),
            (ServiceChoice("灯具电路", "v3u:service:issue:repair_power"), ServiceChoice("门锁门禁", "v3u:service:issue:repair_door")),
            (ServiceChoice("洗衣机", "v3u:service:issue:repair_washer"), ServiceChoice("冰箱", "v3u:service:issue:repair_fridge")),
            (ServiceChoice("网络问题", "v3u:service:issue:repair_network"), ServiceChoice("家具损坏", "v3u:service:issue:repair_furniture")),
            (ServiceChoice("其他问题", "v3u:service:issue:repair_other"),),
            (ServiceChoice("退出报修", "v3u:service:repair_exit"),),
        ),
    )


def issue_prompt_view(draft: ServiceRequestDraft) -> ServiceView:
    return ServiceView(
        kind="repair_issue",
        text=(
            f"<b>{he(draft.issue_label)}</b>\n\n"
            "请直接发送文字说明问题。\n"
            "例如：<code>空调可以启动，但一直不制冷。</code>"
        ),
        rows=((ServiceChoice("返回", "v3u:service:repair"),),),
    )


def repair_media_view(draft: ServiceRequestDraft, media_count: int = 0) -> ServiceView:
    count_line = f"\n\n已添加 {int(media_count)} 个附件。" if media_count else ""
    rows = (
        (ServiceChoice("下一步：选择时间", "v3u:service:repair_media_next"),),
        (ServiceChoice("返回", "v3u:service:repair_modify_desc"),),
    ) if media_count else (
        (ServiceChoice("跳过", "v3u:service:repair_media_skip"),),
        (ServiceChoice("返回", "v3u:service:repair_modify_desc"),),
    )
    return ServiceView(
        kind="repair_media",
        text=(
            "<b>补充图片或视频</b>\n\n"
            "可以连续发送现场图片或视频；没有附件也可以直接跳过。"
            + count_line
        ),
        rows=rows,
    )


def slot_view(draft: ServiceRequestDraft) -> ServiceView:
    return ServiceView(
        kind="repair_slot",
        text="<b>希望什么时候方便处理？</b>\n\n这个时间只用于说明您的方便时段，不代表已安排上门。",
        rows=(
            (ServiceChoice("今天内", "v3u:service:slot:today"), ServiceChoice("明天上午", "v3u:service:slot:tomorrow_am")),
            (ServiceChoice("明天下午", "v3u:service:slot:tomorrow_pm"),),
            (ServiceChoice("返回", "v3u:service:repair_back_media"),),
        ),
    )


def repair_confirm_view(
    draft: ServiceRequestDraft,
    *,
    slot_label: str,
    media_count: int = 0,
    property_name: str = "",
) -> ServiceView:
    lines = ["<b>确认报修信息</b>", ""]
    if property_name:
        lines.extend([he(property_name), ""])
    lines.extend([
        "报修类型", he(draft.issue_label), "",
        "问题描述", he(draft.detail), "",
        "附件", f"已添加 {int(media_count)} 个图片/视频" if media_count else "未添加", "",
        "希望处理时间", he(slot_label),
    ])
    return ServiceView(
        kind="repair_confirm",
        text="\n".join(lines),
        rows=(
            (ServiceChoice("确认提交", "v3u:service:repair_confirm"),),
            (ServiceChoice("修改描述", "v3u:service:repair_modify_desc"), ServiceChoice("修改时间", "v3u:service:repair_modify_time")),
            (ServiceChoice("退出报修", "v3u:service:repair_exit"),),
        ),
    )


def repair_result_view(*, outcome: str, issue_label: str = "", property_name: str = "") -> ServiceView:
    clean = str(outcome or "").strip()
    if clean == "ticket":
        title = "✅ <b>报修信息已记录</b>"
        body = "您的房屋问题已经记录。\n后续处理情况以中文顾问与您确认的结果为准。"
        rows = ((ServiceChoice("中文顾问", "v3u:home:contact"),), (ServiceChoice("返回入住管家", "v3u:service:concierge"),))
    elif clean == "handoff":
        title = "✅ <b>问题已记录</b>"
        body = "您提供的情况已经整理好。\n后续由中文顾问根据这些信息与您继续确认。"
        rows = ((ServiceChoice("中文顾问", "v3u:home:contact"),), (ServiceChoice("返回入住管家", "v3u:service:concierge"),))
    else:
        return ServiceView(
            kind="repair_failed",
            text="<b>提交未成功</b>\n\n这次信息暂未成功提交。\n您可以重新尝试，或直接联系中文顾问。",
            rows=(
                (ServiceChoice("重新提交", "v3u:service:repair_confirm"),),
                (ServiceChoice("中文顾问", "v3u:home:contact"),),
                (ServiceChoice("退出报修", "v3u:service:repair_exit"),),
            ),
        )
    lines=[title,""]
    if property_name:
        lines.extend([he(property_name),""])
    if issue_label:
        lines.append(f"报修类型：{he(issue_label)}")
        lines.append("")
    lines.append(body)
    return ServiceView(kind=f"repair_{clean}", text="\n".join(lines), rows=rows)


def repair_exit_view() -> ServiceView:
    return ServiceView(
        kind="repair_exit",
        text="<b>已退出本次报修</b>\n\n本次信息未提交。\n您可以继续从入住管家选择其他服务。",
        rows=((ServiceChoice("返回入住管家", "v3u:service:concierge"),),),
    )


_PROPERTY_CATEGORIES = (
    ("门禁门卡", "access"),
    ("停车问题", "parking"),
    ("噪音问题", "noise"),
    ("公共区域", "common"),
    ("物业费用", "fees"),
    ("公共设施", "facilities"),
    ("其他问题", "other"),
)


def property_view() -> ServiceView:
    choices = [ServiceChoice(label, f"v3u:service:property_category:{key}") for label, key in _PROPERTY_CATEGORIES]
    return ServiceView(
        kind="property",
        text="<b>物业协调</b>\n\n请选择需要协调的问题类型。",
        rows=(
            (choices[0], choices[1]),
            (choices[2], choices[3]),
            (choices[4], choices[5]),
            (choices[6],),
            (ServiceChoice("退出", "v3u:service:property_exit"),),
        ),
    )


def property_description_view(category_label: str) -> ServiceView:
    return ServiceView(
        kind="property_description",
        text=f"<b>{he(category_label)}</b>\n\n请直接发送文字说明发生了什么。",
        rows=((ServiceChoice("返回", "v3u:service:property"),),),
    )


def property_time_view() -> ServiceView:
    return ServiceView(
        kind="property_time",
        text="<b>事情什么时候发生？</b>",
        rows=(
            (ServiceChoice("今天", "v3u:service:property_time:今天"), ServiceChoice("昨天", "v3u:service:property_time:昨天")),
            (ServiceChoice("这几天", "v3u:service:property_time:这几天"), ServiceChoice("其他时间", "v3u:service:property_time:other")),
            (ServiceChoice("返回", "v3u:service:property_modify_desc"),),
        ),
    )


def property_contacted_view() -> ServiceView:
    return ServiceView(
        kind="property_contacted",
        text="<b>是否已经联系过物业？</b>",
        rows=(
            (ServiceChoice("已经联系过", "v3u:service:property_contacted:yes"), ServiceChoice("还没有联系", "v3u:service:property_contacted:no")),
            (ServiceChoice("返回", "v3u:service:property_modify_time"),),
        ),
    )


def property_confirm_view(*, category: str, description: str, event_time: str, contacted: bool, property_name: str = "") -> ServiceView:
    lines=["<b>确认物业协调信息</b>",""]
    if property_name:
        lines.extend([he(property_name),""])
    lines.extend([
        "问题类型",he(category),"",
        "情况描述",he(description),"",
        "发生时间",he(event_time),"",
        "是否已联系物业","已经联系过" if contacted else "还没有联系",
    ])
    return ServiceView(
        kind="property_confirm",
        text="\n".join(lines),
        rows=(
            (ServiceChoice("确认提交", "v3u:service:property_confirm"),),
            (ServiceChoice("修改描述", "v3u:service:property_modify_desc"), ServiceChoice("修改时间", "v3u:service:property_modify_time")),
            (ServiceChoice("修改联系状态", "v3u:service:property_modify_contacted"),),
            (ServiceChoice("退出", "v3u:service:property_exit"),),
        ),
    )


def property_result_view(*, success: bool) -> ServiceView:
    if success:
        return ServiceView(
            kind="property_success",
            text="✅ <b>情况已记录</b>\n\n您提供的物业情况已经整理好。\n后续沟通与处理情况，以中文顾问与您确认的结果为准。",
            rows=((ServiceChoice("中文顾问", "v3u:home:contact"),),(ServiceChoice("返回入住管家", "v3u:service:concierge"),)),
        )
    return ServiceView(
        kind="property_failed",
        text="<b>提交未成功</b>\n\n这次信息暂未成功提交。\n您可以重新尝试，或直接联系中文顾问。",
        rows=((ServiceChoice("重新提交", "v3u:service:property_confirm"),),(ServiceChoice("中文顾问", "v3u:home:contact"),),(ServiceChoice("退出", "v3u:service:property_exit"),)),
    )


def property_exit_view() -> ServiceView:
    return ServiceView(
        kind="property_exit",
        text="<b>已退出本次物业协调</b>\n\n本次信息未提交。\n您可以继续从入住管家选择其他服务。",
        rows=((ServiceChoice("返回入住管家", "v3u:service:concierge"),),),
    )


def utility_stub_view(kind: str) -> ServiceView:
    labels = {
        "utilities": ("水电缴费协助", "请把账单或需要协助的情况直接告诉中文顾问。"),
        "moving": ("搬家协助", "请直接说明大概日期、出发地、目的地和物品情况。"),
        "cleaning": ("保洁服务", "请直接说明需要日常、入住、退租或深度保洁，以及大概日期。"),
        "network_help": ("网络协助", "请直接说明是新装、续费、故障或其他网络问题。"),
    }
    title, body = labels.get(kind, ("住房服务", "请直接说明需要协助的事情。"))
    return ServiceView(
        kind=f"{kind}_prompt",
        text=f"<b>{title}</b>\n\n{body}",
        rows=((ServiceChoice("中文顾问", "v3u:home:contact"),),(ServiceChoice("返回入住管家", "v3u:service:concierge"),)),
    )


def local_life_view() -> ServiceView:
    return ServiceView(
        kind="local_life",
        text=(
            "<b>周边生活</b>\n\n"
            "目前富力城周边已经整理了一批真实生活资料。\n"
            "其他区域暂时可以直接告诉中文顾问您住哪里、想找什么。"
        ),
        rows=(
            (ServiceChoice("富力城周边", "v3u:service:rfcity"),),
            (ServiceChoice("问问其他区域", "v3u:home:contact"),),
            (ServiceChoice("返回侨联服务", "v3u:home:service"),),
        ),
    )


def nearby_view() -> ServiceView:
    return local_life_view()


def rfcity_home_view() -> ServiceView:
    return ServiceView(
        kind="rfcity",
        text="<b>富力城生活导航</b>\n\n选择分类查看已整理的真实联系方式。",
        rows=(
            (ServiceChoice("餐厅小吃", "v3u:service:rfcity:restaurant"), ServiceChoice("烧烤夜宵", "v3u:service:rfcity:bbq")),
            (ServiceChoice("奶茶饮品", "v3u:service:rfcity:drinks"), ServiceChoice("超市便利", "v3u:service:rfcity:supermarket")),
            (ServiceChoice("酒店租房", "v3u:service:rfcity:hotel"), ServiceChoice("运动休闲", "v3u:service:rfcity:recreation")),
            (ServiceChoice("快递物流", "v3u:service:rfcity:logistics"), ServiceChoice("物业", "v3u:service:rfcity:property")),
            (ServiceChoice("返回周边生活", "v3u:service:local"),),
        ),
    )


__all__ = [
    "ServiceChoice", "ServiceView", "general_prompt_view", "general_success_view", "issue_prompt_view",
    "concierge_home_view", "local_life_view", "nearby_view", "property_view", "property_description_view", "property_time_view", "property_contacted_view", "property_confirm_view", "property_result_view", "property_exit_view", "repair_home_view", "repair_media_view", "repair_confirm_view", "repair_result_view", "repair_exit_view", "repair_success_view",
    "rfcity_category_view", "rfcity_home_view", "service_home_view", "slot_view", "utility_stub_view",
]

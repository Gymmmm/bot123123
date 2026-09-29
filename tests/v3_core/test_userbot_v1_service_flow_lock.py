from types import SimpleNamespace

from v3_core.user_bot.assurance_views import build_assurance_home_view
from v3_core.user_bot.service_views import concierge_home_view, repair_home_view, service_home_view
from v3_core.user_bot.tenant_v1 import lease_view, missing_lease_view


def _rows(view):
    return [[choice.label for choice in row] for row in view.rows]


def _callbacks(view):
    return [
        choice.callback_data
        for row in view.rows
        for choice in row
        if choice.callback_data
    ]


def test_p15_service_hub_copy_buttons_and_callbacks_locked():
    view = service_home_view()
    assert view.text == (
        "🛎️ <b>侨联服务</b>\n\n"
        "找房只是开始。\n"
        "视频代看、入住留档、租后管家，都有明确入口。"
    )
    assert _rows(view) == [
        ["📋 我的租约", "🤝 租后管家"],
        ["🛡️ 侨联安心租", "💬 1V1中文顾问"],
        ["🏠 返回首页"],
    ]
    assert _callbacks(view) == [
        "v3u:service:tenant_lease",
        "v3u:service:concierge",
        "v3u:home:rental",
        "v3u:home:contact",
        "v3u:t:home",
    ]


def test_p16_resident_service_copy_buttons_and_coordination_callbacks_locked():
    view = concierge_home_view()
    assert view.text == "🤝 <b>租后管家</b>\n\n报修、物业、账单、搬家、保洁、网络，不用自己到处找人。\n选一项，侨联接着办。"
    assert _rows(view) == [
        ["🔧 报修直通", "🏢 物业代办"],
        ["🔌 账单代办", "🚚 搬家帮办"],
        ["🧹 保洁代约", "🌐 网络帮办"],
        ["❓ 其他住房问题"],
        ["⬅️ 返回侨联服务"],
    ]
    callbacks = _callbacks(view)
    assert callbacks == [
        "v3u:service:repair",
        "v3u:service:coordination",
        "v3u:service:utilities",
        "v3u:service:moving",
        "v3u:service:cleaning",
        "v3u:service:network_help",
        "v3u:service:general",
        "v3u:home:service",
    ]
    assert not any(callback.startswith("v3u:service:property") for callback in callbacks)


def test_p20_repair_copy_buttons_and_callbacks_locked():
    view = repair_home_view()
    assert view.text == "🔧 <b>报修直通</b>\n\n问题一次说清，后面按这条继续处理。\n\n请选择需要处理的问题："
    assert _rows(view) == [
        ["❄️ 空调", "🚿 热水 / 漏水"],
        ["💡 灯具 / 电路", "🔐 门锁 / 门禁"],
        ["🧺 洗衣机", "🧊 冰箱"],
        ["🌐 网络", "🪑 家具损坏"],
        ["❓ 其他问题"],
        ["⬅️ 退出报修"],
    ]
    assert _callbacks(view) == [
        "v3u:service:issue:repair_ac",
        "v3u:service:issue:repair_water",
        "v3u:service:issue:repair_power",
        "v3u:service:issue:repair_door",
        "v3u:service:issue:repair_washer",
        "v3u:service:issue:repair_fridge",
        "v3u:service:issue:repair_network",
        "v3u:service:issue:repair_furniture",
        "v3u:service:issue:repair_other",
        "v3u:service:repair_exit",
    ]


def test_p18_unbound_lease_copy_and_buttons_locked():
    view = missing_lease_view()
    assert view.text == (
        "📋 <b>我的租约</b>\n\n"
        "目前没有查到已绑定的租约。\n\n"
        "如果已经通过侨联入住，可以联系中文顾问核对。"
    )
    assert _rows(view) == [
        ["💬 1V1中文顾问"],
        ["⬅️ 返回侨联服务"],
    ]
    assert _callbacks(view) == [
        "v3u:home:contact",
        "v3u:home:service",
    ]


def test_p19_bound_lease_copy_buttons_and_callbacks_locked():
    binding = SimpleNamespace(
        lease_end_date="",
        contract_end_date="2027-01-31",
        monthly_rent=800,
        deposit_months=2,
        rent_day=5,
        property_name="测试房源",
    )
    service = SimpleNamespace(require_active_binding=lambda user_id: binding)
    view = lease_view(service, 123)
    assert view.text == (
        "📋 <b>我的租约</b>\n\n"
        "🏠 测试房源\n"
        "月租｜$800/月\n"
        "押金｜2个月\n"
        "交租日｜每月5日\n"
        "到期日｜2027-01-31\n\n"
        "状态｜🟢 租约有效"
    )
    assert _rows(view) == [
        ["🔄 申请续租", "💬 1V1中文顾问"],
        ["⬅️ 返回侨联服务"],
    ]
    assert _callbacks(view) == [
        "v3u:service:tenant_renew",
        "v3u:home:contact",
        "v3u:home:service",
    ]


def test_p24_assurance_copy_buttons_and_callbacks_locked():
    view = build_assurance_home_view()
    assert view.text == (
        "🛡️ <b>侨联安心租</b>\n\n"
        "看房先确认，费用先说清，入住有记录，租后有人接。\n\n"
        "<b>视频代看｜费用先说清｜入住留档｜租后管家</b>"
    )
    assert _rows(view) == [
        ["🛡️ 入住留档"],
        ["💬 1V1中文顾问"],
        ["⬅️ 返回侨联服务"],
    ]
    assert _callbacks(view) == [
        "v3u:assure:handover",
        "v3u:home:contact",
        "v3u:home:service",
    ]
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
        "找房只是开始。\n\n"
        "签约、入住，以及住进去后的房屋问题，都可以继续找侨联。"
    )
    assert _rows(view) == [
        ["📋 我的租约", "🛡️ 入住服务"],
        ["🏠 安心租房", "💬 中文顾问"],
        ["⬅️ 返回首页"],
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
    assert view.text == "🛡️ <b>入住服务</b>\n\n入住后的房屋和生活问题，可以从这里处理。"
    assert _rows(view) == [
        ["🔧 房屋报修", "🏢 物业协调"],
        ["🔌 水电协助", "🚚 搬家协助"],
        ["🧹 保洁服务", "🌐 网络协助"],
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
    assert view.text == "🔧 <b>房屋报修</b>\n\n请选择需要处理的问题："
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
        ["💬 中文顾问"],
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
        ["🔄 申请续租", "💬 中文顾问"],
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
        "🏠 <b>安心租房</b>\n\n"
        "从看房到入住，重要信息尽量提前确认并留档。\n\n"
        "入住交接、费用确认和住房问题，都可以继续找侨联。"
    )
    assert _rows(view) == [
        ["📋 入住交接留档"],
        ["💬 中文顾问"],
        ["⬅️ 返回侨联服务"],
    ]
    assert _callbacks(view) == [
        "v3u:assure:handover",
        "v3u:home:contact",
        "v3u:home:service",
    ]
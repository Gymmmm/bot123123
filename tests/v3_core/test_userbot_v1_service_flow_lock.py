from types import SimpleNamespace

from v3_core.user_bot.assurance_views import build_assurance_home_view
from v3_core.user_bot.service_views import aftercare_home_view, repair_home_view, service_home_view
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
        "租房不只是找到房子。\n"
        "从看房、签约到入住，有需要都可以继续联系侨联。"
    )
    assert _rows(view) == [
        ["🎥 视频带看", "🛡️ 看房与交接"],
        ["🤝 租后服务", "📋 我的租约"],
        ["💬 中文顾问", "⬅️ 返回首页"],
    ]
    assert _callbacks(view) == ["v3u:service:video", "v3u:home:rental", "v3u:service:aftercare", "v3u:service:tenant_lease", "v3u:home:contact", "v3u:t:home"]


def test_p16_aftercare_only_exposes_real_workflows():
    view = aftercare_home_view()
    assert "统一在这里提交" in view.text
    assert _callbacks(view) == ["v3u:service:repair", "v3u:home:service"]


def test_p20_unified_housing_problem_categories():
    from v3_core.user_bot.service_flow import SERVICE_REQUEST_LABELS
    view = repair_home_view()
    assert "提交住房问题" in view.text
    assert _rows(view) == [
        ["🔧 维修问题", "🏢 物业问题"],
        ["💡 账单问题", "❓ 其他问题"],
        ["⬅️ 返回租后服务"],
    ]
    callbacks = _callbacks(view)
    assert callbacks[-1] == "v3u:service:concierge"
    keys = [value.split("issue:")[1] for value in callbacks[:-1]]
    assert keys == ["repair_other", "aftercare_property", "aftercare_bill", "aftercare_other"]
    assert all(key in SERVICE_REQUEST_LABELS for key in keys)


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
        "🏠 <b>看房与交接</b>\n\n"
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
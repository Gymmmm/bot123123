from pathlib import Path

from v3_core.user_bot.admin_console_bridge import admin_home_keyboard
from v3_core.user_bot.assurance_views import build_assurance_home_view, build_moving_view
from v3_core.user_bot.home_callbacks import encode_home_callback, parse_home_callback
from v3_core.user_bot.home_views import build_about_view, build_booking_view, build_contact_view, build_home_view
from v3_core.user_bot.service_product_views import service_home_view
from v3_core.user_bot.service_views import local_life_view, nearby_view, property_view, repair_home_view, rfcity_home_view
from v3_core.user_bot.source_display import source_display_label


def _labels(view):
    return [choice.label for row in view.rows for choice in row]


def _callbacks(view):
    return [choice.callback_data for row in view.rows for choice in row if getattr(choice, "callback_data", "")]


def _button_labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def test_v3_home_uses_final_conversion_navigation():
    view = build_home_view(channel_url="https://t.me/qiaolian")
    labels = _labels(view)
    assert labels == ["🔍 开始找房", "🛎️ 侨联服务", "📢 最新房源", "💬 中文顾问"]
    forbidden = ("智能找房", "顾问帮我找", "关于侨联", "房源频道", "侨联保障", "租赁服务指南", "我想换房")
    assert not any(any(term in label for term in forbidden) for label in labels)


def test_home_without_channel_keeps_core_conversion_actions():
    labels = _labels(build_home_view())
    assert labels == ["🔍 开始找房", "🛎️ 侨联服务", "💬 中文顾问"]


def test_about_and_booking_are_compatible_secondary_home_actions():
    assert parse_home_callback(encode_home_callback("about")).action == "about"
    assert parse_home_callback(encode_home_callback("book")).action == "book"
    about = build_about_view().text
    assert "安心租房" in about
    assert "看房、费用确认、入住交接留档" in about
    assert "预约看房" in build_booking_view().text
    assert "换房" not in build_about_view().text


def test_service_hub_is_public_and_does_not_require_binding():
    view = service_home_view()
    labels = _labels(view)
    assert labels == [
        "📋 我的租约", "🛡️ 入住服务", "🏠 安心租房",
        "💬 中文顾问", "⬅️ 返回首页",
    ]
    callbacks = _callbacks(view)
    assert callbacks == [
        "v3u:service:tenant_lease", "v3u:service:concierge",
        "v3u:home:rental", "v3u:home:contact", "v3u:t:home",
    ]
    assert "没有显示你的住房信息" not in view.text


def test_rental_service_is_public_parent_content_center():
    view = build_assurance_home_view()
    assert view.text == (
        "🏠 <b>侨联地产｜金边中文租房</b>\n"
        "⭐ 金边本地6年经验\n"
        "📍 富力城｜炳发城｜BKK1｜钻石岛\n"
        "💬 专业中文顾问\n"
        "📸 真实房源｜实拍更新\n"
        "📹 实地看房 / 视频代看\n"
        "找房 · 看房 · 签约 · 入住 · 售后\n"
        "签约不是服务的结束。"
    )
    assert _labels(view) == ["🔍 开始找房", "💬 中文顾问", "⬅️ 返回首页"]
    assert _callbacks(view) == [
        "v3u:home:search", "v3u:home:contact", "v3u:t:home",
    ]


def test_listing_attribution_sources_have_chinese_admin_labels():
    expected = {
        "channel_listing": "频道房源",
        "search_result": "找房结果",
        "listing_details": "租赁详情",
        "listing_photos": "更多实拍",
        "similar_listing": "相近房源",
        "appointment_success": "预约完成",
    }
    for source, label in expected.items():
        assert source_display_label(source) == label


def test_repair_page_exposes_every_supported_core_issue():
    callbacks = set(_callbacks(repair_home_view()))
    for issue in ("repair_ac", "repair_water", "repair_power", "repair_door", "repair_washer", "repair_fridge", "repair_network", "repair_furniture", "repair_other"):
        assert f"v3u:service:issue:{issue}" in callbacks


def test_public_static_page_callbacks_use_registered_v3_namespaces():
    views = [
        build_about_view(), build_booking_view(), build_contact_view(), service_home_view(),
        build_assurance_home_view(), build_moving_view(), repair_home_view(), property_view(),
        local_life_view(), nearby_view(), rfcity_home_view(),
    ]
    allowed = ("v3u:home:", "v3u:service:", "v3u:assure:", "v3u:t:")
    for view in views:
        for callback in _callbacks(view):
            assert callback.startswith(allowed), (view.kind, callback)


def test_admin_home_exposes_consult_contract_renewal_and_service_work():
    labels = _button_labels(admin_home_keyboard())
    assert "🆕 新咨询" in labels
    assert "📅 今日预约" in labels
    assert "📄 租客与合同" in labels
    assert "🔄 续租跟进" in labels
    assert "🛠 服务工单" in labels


def test_production_entrypoint_wires_3858_admin_and_workflow_routes():
    entrypoint = Path("run_v3_user_bot.py").read_text(encoding="utf-8")
    dual_app = Path("qiaolian_dual/app.py").read_text(encoding="utf-8")
    admin_callbacks = Path("qiaolian_dual/callback_admin.py").read_text(encoding="utf-8")

    assert "from qiaolian_dual.user_bot import main" in entrypoint
    assert "acquire_user_bot_polling_lock" in entrypoint
    assert "release_user_bot_polling_lock" in entrypoint
    assert "v3_core.user_bot.app" not in entrypoint

    assert "CommandHandler('contracts', cmd_contracts)" in dual_app
    assert "CommandHandler('admin', cmd_admin_home)" in dual_app
    assert "CallbackQueryHandler(handle_admin_query, pattern=r'^adminq:')" not in dual_app
    assert "data.startswith(('adminq:', 'adminlead:', 'adminrepair:'))" in admin_callbacks
    assert "await handle_admin_query(update, context)" in admin_callbacks
    assert "data.startswith('adminlead:')" in admin_callbacks
    assert "data.startswith('adminrepair:')" in admin_callbacks
    assert "if action == 'done':" in admin_callbacks


def test_v3_app_registers_admin_contract_and_workflow_routes():
    source = Path("v3_core/user_bot/app.py").read_text(encoding="utf-8")
    assert 'CommandHandler("admin", admin)' in source
    assert 'CommandHandler("contracts", contracts)' in source
    assert 'pattern=r"^adminq:"' in source
    assert 'pattern=r"^(?:admincontract|adminlead|adminrepair):"' in source
    assert 'raw.startswith("admincontract:")' in source
    assert 'raw.startswith(("adminlead:", "adminrepair:"))' in source

from pathlib import Path

from v3_core.user_bot.admin_console_bridge import admin_home_keyboard
from v3_core.user_bot.home_callbacks import encode_home_callback, parse_home_callback
from v3_core.user_bot.home_views import build_about_view, build_booking_view, build_home_view
from v3_core.user_bot.service_product_views import service_home_view


def _labels(view):
    return [choice.label for row in view.rows for choice in row]


def _button_labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def test_v3_home_uses_product_upgrade_navigation():
    view = build_home_view(channel_url="https://t.me/qiaolian")
    labels = _labels(view)
    assert labels[:5] == [
        "🔍 智能找房",
        "📖 关于侨联",
        "📅 预约看房",
        "💬 联系顾问",
        "🛡 入住服务",
    ]
    assert "📢 房源频道" in labels
    assert "🧭 周边服务" not in labels
    assert "📅 我的预约" not in labels
    assert "🛡 侨联保障" not in labels


def test_about_and_booking_are_first_class_home_actions():
    assert parse_home_callback(encode_home_callback("about")).action == "about"
    assert parse_home_callback(encode_home_callback("book")).action == "book"
    assert "关于侨联地产" in build_about_view().text
    assert "从房源详情直接点「预约看房」" in build_booking_view().text


def test_service_hub_covers_full_rental_lifecycle_without_new_backend_flow():
    view = service_home_view()
    labels = _labels(view)
    assert "📄 租赁服务指南" in labels
    assert "🔧 报修与维护" in labels
    assert "🏢 物业沟通" in labels
    assert "🔄 续租 / 换房" in labels
    assert "🔐 退租与押金" in labels
    assert "🧭 周边服务" in labels

    callbacks = [choice.callback_data for row in view.rows for choice in row]
    assert "v3u:service:repair" in callbacks
    assert "v3u:service:property" in callbacks
    assert "v3u:home:rental" in callbacks
    assert "v3u:assure:deposit" in callbacks
    assert "v3u:service:local" in callbacks


def test_admin_home_exposes_consult_contract_renewal_and_service_work():
    labels = _button_labels(admin_home_keyboard())
    assert "🆕 新咨询" in labels
    assert "📅 今日预约" in labels
    assert "📄 租客与合同" in labels
    assert "🔄 续租跟进" in labels
    assert "🛠 服务工单" in labels


def test_production_entrypoint_wires_complete_admin_adapters():
    source = Path("run_v3_user_bot.py").read_text(encoding="utf-8")
    assert "show_unified_admin_home" in source
    assert "admin_contract_command_handler=cmd_contracts" in source
    assert "admin_contract_callback_handler=handle_admin_contract_callback" in source
    assert "admin_contract_text_handler=handle_admin_contract_text" in source
    assert "admin_workflow_callback_handler=handle_v3_admin_workflow" in source
    assert "qiaolian_dual.v3_admin_workflow_bridge" in source


def test_v3_app_registers_admin_contract_and_workflow_routes():
    source = Path("v3_core/user_bot/app.py").read_text(encoding="utf-8")
    assert 'CommandHandler("admin", admin)' in source
    assert 'CommandHandler("contracts", contracts)' in source
    assert 'pattern=r"^adminq:"' in source
    assert 'pattern=r"^admincontract:"' in source
    assert 'pattern=r"^admin(?:lead|repair):"' in source

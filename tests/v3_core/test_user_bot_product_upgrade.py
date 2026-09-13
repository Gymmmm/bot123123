from v3_core.user_bot.home_callbacks import encode_home_callback, parse_home_callback
from v3_core.user_bot.home_views import build_about_view, build_booking_view, build_home_view
from v3_core.user_bot.service_product_views import service_home_view


def _labels(view):
    return [choice.label for row in view.rows for choice in row]


def test_v3_home_uses_product_upgrade_navigation():
    view = build_home_view(channel_url="https://t.me/qiaolian")
    labels = _labels(view)
    assert labels[:6] == [
        "🔍 智能找房",
        "📖 关于侨联",
        "📅 预约看房",
        "💬 联系顾问",
        "🛡 入住服务",
        "🧭 周边服务",
    ]
    assert "📢 房源频道" in labels
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

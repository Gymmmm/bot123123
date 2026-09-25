
from v3_core.user_bot.adviser_notes import adviser_notes_for_view
from v3_core.user_bot.home_views import build_home_view
from v3_core.user_bot.search_cards import build_search_card
from v3_core.user_bot.service_views import service_home_view, concierge_home_view
from v3_core.user_bot.tenant_v1 import renew_view, terminate_view
from v3_core.user_bot.transition_views import TransitionViewService


def _labels(view):
    return [choice.label for row in view.rows for choice in row]


def test_home_matches_final_service_surface():
    view = build_home_view(channel_url="https://t.me/example")
    assert _labels(view) == ["🔍 智能找房", "📖 关于侨联地产", "📅 预约看房", "💎 直接问顾问", "⚡ 入住管家", "🧭 周边服务"]
    assert "金边中文租房" in view.text


def test_search_entry_is_direct_filter_panel():
    view = TransitionViewService.search_entry()
    assert "想找什么样的房子？" in view.text
    assert "BKK1 一房，预算 $600" in view.text
    assert _labels(view) == [
        "📍 按区域", "💰 按预算", "🏠 按户型",
        "💬 中文顾问", "⬅️ 返回首页",
    ]


def test_listing_surface_source_contains_final_first_layer_actions():
    source = open(build_search_card.__code__.co_filename, encoding="utf-8").read()
    for label in ("上一套", "下一套", "更多实拍", "预约看房", "换搜索条件"):
        assert label in source
    for label in ("看实拍", "咨询这套"):
        assert label not in source


def test_service_hubs_match_final_layouts():
    assert _labels(service_home_view()) == [
        "📋 我的租约", "🛡️ 入住服务", "🏠 安心租房",
        "💬 中文顾问", "⬅️ 返回首页",
    ]
    assert _labels(concierge_home_view()) == [
        "🔧 房屋报修", "🏢 物业协调", "🔌 水电协助", "🚚 搬家协助",
        "🧹 保洁服务", "🌐 网络协助", "❓ 其他住房问题", "⬅️ 返回侨联服务",
    ]


class _Binding:
    property_name = "富力城"
    lease_end_date = "2027-01-01"
    contract_end_date = "2027-01-01"


class _Service:
    def require_active_binding(self, user_id):
        return _Binding()


def test_renew_terminate_are_safe_intent_surfaces():
    assert _labels(renew_view(_Service(), 1)) == ["提交续租意向", "中文顾问"]
    assert _labels(terminate_view(_Service(), 1)) == ["提交退租意向", "中文顾问"]


def test_adviser_notes_remain_publisher_read_only():
    class Hidden:
        snapshot = {"adviser_copy_source": "hidden", "adviser_copy": "不要显示"}
    class Shown:
        snapshot = {"adviser_copy_source": "publisher", "adviser_copy": "采光好"}
    assert adviser_notes_for_view(Hidden()) == ""
    assert adviser_notes_for_view(Shown()) == "采光好"

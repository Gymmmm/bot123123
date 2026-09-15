from pathlib import Path
import autopilot_publish_bot as ap
from qiaolian_dual.callback_rental import _ASSET_DIR, moving_keyboard, rental_home_keyboard
from qiaolian_dual.channel_post import format_channel_listing_post
from qiaolian_dual.keyboards_search import nearby_area_keyboard
from qiaolian_dual.status_labels import status_label

def _callbacks(keyboard):
    return [button.callback_data for row in keyboard.inline_keyboard for button in row]

def test_status_mapping_is_central_and_complete():
    assert [status_label(x) for x in ("pending", "approved", "rejected", "draft", "ready", "published")] == ["待审核", "已通过", "已驳回", "草稿", "待发布", "已发布"]

def test_channel_post_is_compact_and_has_no_comment_area_copy():
    text = format_channel_listing_post({"project": "富力城", "layout": "一房", "price": 680, "property_type": "公寓", "size_sqm": 55, "floor": "12楼", "deposit": "押一付一", "contract_term": "一年", "status": "active"}, "l_1")
    assert all(x not in text for x in ("评论区", "侨联说"))
    assert all(x in text for x in ("🏠", "💰", "🏢", "🔑"))

def test_weather_fallback_order_uses_wmo_and_stable_last():
    order, templates = ap._load_weather_reminder_templates()
    assert order == ("storm", "rain_heavy", "rain_light", "rain_possible", "hot", "sunny", "humid", "good", "stable")
    assert ap._select_weather_reminder(95, 31, 25, 90)[0] == "storm"
    assert ap._select_weather_reminder(0, 36, 27, 0)[0] == "hot"
    assert ap._select_weather_reminder(45, 27, 23, 0)[0] == "stable"
    assert all("想你" not in text and "接你" not in text for text in templates.values())

def test_assurance_has_exact_four_callbacks_and_separate_assets():
    assert _callbacks(rental_home_keyboard()) == ["hub:rental:handover", "hub:rental:deposit", "hub:rental:moving", "hub:advisor"]
    assert "hub:service" not in _callbacks(moving_keyboard())
    for kind in ("handover", "deposit"):
        for suffix in ("png", "pdf"):
            path = _ASSET_DIR / f"{kind}.{suffix}"
            assert path.is_file() and path.stat().st_size > 10_000
    assert (_ASSET_DIR / "handover.pdf").read_bytes() != (_ASSET_DIR / "deposit.pdf").read_bytes()

def test_nearby_other_area_is_dedicated_and_copy_keeps_sla():
    labels = [b.text for row in nearby_area_keyboard().inline_keyboard for b in row]
    assert "📍 其他区域需求提交" in labels
    source = Path("qiaolian_dual/callback_service.py").read_text(encoding="utf-8") + Path("qiaolian_dual/message_handlers.py").read_text(encoding="utf-8")
    assert "1个工作日内" in source and "action='nearby_request'" in source and "source='nearby_service'" in source

def test_public_copy_guard_has_no_obvious_informal_pronouns():
    combined = "\n".join(Path(p).read_text(encoding="utf-8") for p in ("qiaolian_dual/callback_rental.py", "qiaolian_dual/callback_service.py"))
    for phrase in ("你的", "帮你", "联系你", "收到你的"):
        assert phrase not in combined

def test_publish_success_names_formal_channel():
    source = Path("autopilot_publish_bot.py").read_text(encoding="utf-8")
    assert "房源已发布到正式频道" in source and "房源已发布到测试频道" not in source

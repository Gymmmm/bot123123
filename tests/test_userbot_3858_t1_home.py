from qiaolian_dual.adapters.deeplink import DeeplinkAdapter
from qiaolian_dual.keyboards_common import main_keyboard, no_match_followup_keyboard
from qiaolian_dual.messages import home_text
from qiaolian_dual.session_deeplink import parse_start_arg_payload
from qiaolian_dual.texts import welcome_text


def _labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def test_t1_home_is_3858_six_buttons():
    assert _labels(main_keyboard()) == [
        "🔍 智能找房",
        "📖 关于侨联地产",
        "📅 预约看房",
        "💎 直接问顾问",
        "⚡ 入住管家",
        "🧭 周边服务",
    ]


def test_t1_welcome_uses_3858_home_copy():
    text = welcome_text()
    assert text == home_text()
    assert "侨联地产 · 金边华人房产服务" in text


def test_t1_no_match_keeps_3858_recovery():
    assert _labels(no_match_followup_keyboard()) == ["💬 联系顾问", "🎯 继续筛选", "🏠 返回首页"]


def test_t1_deeplink_adapter_wins_property_and_book_video():
    details = parse_start_arg_payload("property_QL-PP-AAAA_details")
    assert details["action"] == "details"
    assert details["target"] == "QL-PP-AAAA"
    assert details["channel_return"] is True
    contact = parse_start_arg_payload("property_QL-PP-AAAA_contact")
    assert contact["action"] == "consult"
    video = parse_start_arg_payload("book_video_QL-PP-AAAA")
    assert video["action"] == "video"
    assert DeeplinkAdapter().parse("want_home") is None

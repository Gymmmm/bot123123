"""Live video focus stays optional and uses existing booking callbacks."""
from v3_core.user_bot.video_focus import FOCUS_SESSION_KEY, focus_summary, video_focus_view
from v3_core.user_bot.transition_callbacks import encode_transition_choice, parse_transition_callback


def test_video_focus_toggle_codec_and_selection():
    view = video_focus_view(["sunlight", "water"])
    assert view.kind == "appointment_video_focus"
    assert "采光朝向、水压热水" in focus_summary(["sunlight", "water"])
    assert "✅ ☀️ 采光朝向" in [x.label for row in view.rows for x in row]
    for row in view.rows:
        for choice in row:
            encoded = encode_transition_choice(choice)
            assert len(encoded.encode()) <= 64
            parsed = parse_transition_callback(encoded)
            assert parsed is not None
            assert parsed.kind == choice.kind
            assert parsed.value == choice.value


def test_video_focus_invalid_code_and_optional_skip():
    assert parse_transition_callback("v3u:t:appointment_focus_toggle:invalid") is None
    assert focus_summary(["invalid"]) == ""
    assert FOCUS_SESSION_KEY == "v3_video_focus"
    assert "跳过" in " ".join(c.label for row in video_focus_view().rows for c in row)

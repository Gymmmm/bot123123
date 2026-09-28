from __future__ import annotations

from qiaolian_dual.admin_commands import _advisor_response_keyboard
from qiaolian_dual.callback_contract import matches as matches_contract_callback
from qiaolian_dual.jobs import _lease_reminder_keyboard
from v3_core.user_bot.home_callbacks import parse_home_callback
from v3_core.user_bot.legacy_routes import legacy_home_action
from v3_core.user_bot.transition_callbacks import parse_transition_callback


def _callback_data(markup) -> list[str]:
    return [
        str(button.callback_data)
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data
    ]


def test_lease_reminder_keeps_binding_aware_renew_and_uses_canonical_contact() -> None:
    callbacks = _callback_data(_lease_reminder_keyboard(123))
    assert callbacks == ["contract:renew_yes:123", "v3u:home:contact"]
    assert matches_contract_callback(callbacks[0])
    contact = parse_home_callback(callbacks[1])
    assert contact is not None
    assert contact.action == "contact"


def test_advisor_response_notice_uses_canonical_v3_callbacks() -> None:
    callbacks = _callback_data(_advisor_response_keyboard())
    assert callbacks == ["v3u:home:contact", "v3u:t:home"]
    contact = parse_home_callback(callbacks[0])
    assert contact is not None
    assert contact.action == "contact"
    home = parse_transition_callback(callbacks[1])
    assert home is not None
    assert home.kind == "home"


def test_historical_callbacks_remain_compatible_for_old_messages() -> None:
    assert legacy_home_action("appointment_menu:contact") == "contact"
    assert legacy_home_action("home") == "home"
    assert matches_contract_callback("contract:renew_yes:123")


def test_video_appointment_mode_is_already_a_valid_v3_transition() -> None:
    video = parse_transition_callback("v3u:t:appointment_mode:video")
    offline = parse_transition_callback("v3u:t:appointment_mode:offline")
    assert video is not None
    assert video.kind == "appointment_mode"
    assert video.value == "video"
    assert offline is not None
    assert offline.kind == "appointment_mode"
    assert offline.value == "offline"
    assert parse_transition_callback("v3u:t:appointment_mode:test") is None

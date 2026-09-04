from qiaolian_dual.appointment_ui import (
    _appointment_confirm_keyboard,
    _appointment_date_keyboard,
    _appointment_time_keyboard,
)
from qiaolian_dual.appointments_view import _appointment_card_keyboard
from qiaolian_dual.callback_listing import _with_return_nav
from qiaolian_dual.common import InlineKeyboardButton, InlineKeyboardMarkup


def _callbacks(markup):
    return {
        str(getattr(button, 'callback_data', '') or '')
        for row in markup.inline_keyboard
        for button in row
    }


def test_listing_secondary_pages_have_search_and_home_exit():
    base = InlineKeyboardMarkup([[InlineKeyboardButton('详情', callback_data='listing:detail:l_1')]])
    callbacks = _callbacks(_with_return_nav(base))
    assert 'home_smart_search' in callbacks
    assert 'home' in callbacks


def test_appointment_date_has_parent_and_home_exit():
    callbacks = _callbacks(_appointment_date_keyboard())
    assert 'appoint_back_mode' in callbacks
    assert 'home' in callbacks


def test_appointment_time_has_parent_and_home_exit():
    callbacks = _callbacks(_appointment_time_keyboard())
    assert 'appoint_back_date' in callbacks
    assert 'home' in callbacks


def test_appointment_confirm_has_edit_and_home_exit():
    callbacks = _callbacks(_appointment_confirm_keyboard())
    assert 'apedit:time' in callbacks
    assert 'home' in callbacks


def test_appointment_list_has_search_and_home_exit():
    callbacks = _callbacks(_appointment_card_keyboard())
    assert 'home_smart_search' in callbacks
    assert 'home' in callbacks

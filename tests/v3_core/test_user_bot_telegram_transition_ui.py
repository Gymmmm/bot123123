from __future__ import annotations

from v3_core.user_bot.telegram_transition_ui import build_transition_keyboard
from v3_core.user_bot.transition_views import TransitionChoice, TransitionView


def test_renderer_preserves_transition_rows_and_uses_v3_callback_contract():
    view = TransitionView(
        kind="search_budget",
        text="预算",
        rows=(
            (
                TransitionChoice("$400以内", "budget_choice", "b1", budget_max=400),
                TransitionChoice("$400–600", "budget_choice", "b2", budget_min=400, budget_max=600),
            ),
            (TransitionChoice("✍️ 自己输入", "budget_custom"),),
            (TransitionChoice("⬅️ 返回", "change_search"),),
        ),
    )

    keyboard = build_transition_keyboard(view)

    assert [[button.text for button in row] for row in keyboard.inline_keyboard] == [
        ["$400以内", "$400–600"],
        ["✍️ 自己输入"],
        ["⬅️ 返回"],
    ]
    callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]
    assert callbacks == [
        "v3u:t:budget_choice:b1",
        "v3u:t:budget_choice:b2",
        "v3u:t:budget_custom",
        "v3u:change_search",
    ]
    assert all(len(value.encode("utf-8")) <= 64 for value in callbacks if value)


def test_renderer_reuses_listing_details_public_callback():
    view = TransitionView(
        kind="appointment_date",
        text="预约",
        rows=((TransitionChoice("⬅️ 返回房源", "listing_details", public_listing_id="QL-RF-A2B3"),),),
    )

    keyboard = build_transition_keyboard(view)

    assert keyboard.inline_keyboard[0][0].callback_data == "v3u:listing:details:QL-RF-A2B3"

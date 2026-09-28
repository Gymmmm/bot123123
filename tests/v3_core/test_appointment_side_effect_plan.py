from __future__ import annotations

from v3_core.user_bot.appointment_management import AppointmentCancellationResult
from v3_core.user_bot.appointment_service import AppointmentSubmissionResult
from v3_core.user_bot.appointment_side_effects import (
    cancellation_effect_plan,
    submission_effect_plan,
)


def test_created_submission_preserves_production_follow_up_order():
    result = AppointmentSubmissionResult(
        kind="created",
        appointment_id=1,
        lead_action="appointment_submit",
        lead_source="channel_deeplink",
    )

    assert submission_effect_plan(result).effects == (
        "recompute_listing_availability",
        "record_lead",
        "sync_channel",
        "notify_admin",
    )


def test_reused_submission_has_no_write_recompute_or_lead_but_still_syncs_and_notifies():
    result = AppointmentSubmissionResult(
        kind="reused",
        appointment_id=7,
        lead_action=None,
        lead_source="channel_deeplink",
        should_recompute_listing_availability=False,
        should_sync_channel=True,
        should_notify_admin=True,
    )

    assert submission_effect_plan(result).effects == (
        "sync_channel",
        "notify_admin",
    )


def test_cancel_only_recomputes_inventory_then_syncs_channel():
    result = AppointmentCancellationResult(
        appointment_id=7,
        listing_id="LST_1",
        previous_status="confirmed",
    )

    assert cancellation_effect_plan(result).effects == (
        "recompute_listing_availability",
        "sync_channel",
    )

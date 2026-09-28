from qiaolian_dual.appointment_ui import _normalize_custom_date as legacy_normalize_custom_date
from qiaolian_dual.common import (
    APPOINTMENT_MODE_LABELS as LEGACY_MODE_LABELS,
    APPOINTMENT_TIME_LABELS as LEGACY_TIME_LABELS,
)
from v3_core.user_bot.appointments import (
    APPOINTMENT_MODE_LABELS,
    APPOINTMENT_TIME_LABELS,
    AppointmentDraft,
    display_time,
    duplicate_identity,
    editable_status,
    normalize_custom_date,
    normalize_mode,
    valid_custom_time,
)


def test_mode_and_named_time_labels_match_locked_production():
    assert APPOINTMENT_MODE_LABELS == LEGACY_MODE_LABELS
    assert APPOINTMENT_TIME_LABELS == LEGACY_TIME_LABELS
    for key, value in LEGACY_TIME_LABELS.items():
        assert display_time(key) == value


def test_custom_date_normalization_matches_locked_production_matrix():
    cases = (
        "0905",
        "905",
        "09-05",
        "9/5",
        "2026-09-05",
        "9月5日",
        " 下周三 ",
        "本周天",
        "13月1日",
        "2月30日",
        "tomorrow",
        "",
    )
    for raw in cases:
        assert normalize_custom_date(raw) == legacy_normalize_custom_date(raw)


def test_custom_time_validation_matches_current_callback_contract_examples():
    for value in ("20:00", "8:05", "23:59", "晚上8点", "下午 3点", "傍晚18:30"):
        assert valid_custom_time(value)
    for value in ("24:00", "20:99", "8点", "晚上", "tomorrow", ""):
        assert not valid_custom_time(value)


def test_new_appointment_draft_is_date_then_time_then_ready():
    draft = AppointmentDraft(listing_id="LST_1")
    assert draft.mode == "offline"
    assert draft.step == "date"

    video = draft.with_mode("video").with_date("09-05")
    assert video.mode == "video"
    assert video.step == "time"

    ready = video.with_time("pm")
    assert ready.step == "ready"
    assert ready.ready


def test_changing_date_clears_previously_selected_time():
    ready = AppointmentDraft(
        listing_id="LST_1",
        mode="offline",
        date="09-05",
        time="pm",
    )
    changed = ready.with_date("09-06")
    assert changed.date == "09-06"
    assert changed.time == ""
    assert changed.step == "time"


def test_unknown_mode_falls_back_to_offline_like_production_start_flow():
    assert normalize_mode("video") == "video"
    assert normalize_mode("offline") == "offline"
    assert normalize_mode("anything") == "offline"
    assert normalize_mode("") == "offline"


def test_exact_duplicate_identity_includes_user_listing_mode_date_and_time():
    assert duplicate_identity(
        user_id=123,
        listing_id=" LST_1 ",
        mode="video",
        date="09-05",
        time="pm",
    ) == (123, "LST_1", "video", "09-05", "pm")


def test_done_and_cancelled_are_not_editable_but_other_current_statuses_are():
    assert not editable_status("done")
    assert not editable_status("cancelled")
    for status in ("pending", "assigned", "contacted", "confirmed", ""):
        assert editable_status(status)

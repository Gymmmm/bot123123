from __future__ import annotations

from v3_core.user_bot.appointment_service import AppointmentSubmissionResult
from v3_core.user_bot.appointment_side_effects import AppointmentEffectPlan
from v3_core.user_bot.appointment_submit_executor import AppointmentSubmitExecution
from v3_core.user_bot.lead_effects import LeadEffectExecutor
from v3_core.user_bot.lead_service import LeadService, LeadUser
from v3_core.user_bot.public_appointment import PublicAppointmentDraft
from v3_core.user_bot.search_query import SearchCriteria
from v3_core.user_bot.transition_actions import SearchSubmitIntent


class FakeRepository:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.rows = []

    def create(self, values):
        if self.fail:
            raise RuntimeError("lead_write_failed")
        self.rows.append(dict(values))
        return 41


def _user():
    return LeadUser(user_id=123, username="alice", display_name="Alice")


def _search_intent():
    return SearchSubmitIntent(
        criteria=SearchCriteria(
            property_type="公寓",
            location_keys=("BKK1",),
            budget_min=600,
            budget_max=900,
            raw_text="",
        ),
        source="user_search",
        goal="any",
        area_display="BKK1",
        budget_label="$600-900",
        touch_payload={"channel_message_id": 88},
    )


def _draft():
    return PublicAppointmentDraft(
        public_listing_id="QL-RF-A2B3",
        mode="offline",
        date="09-10",
        time="pm",
        source="listing_callback",
    )


def test_search_lead_effect_records_with_fixed_timestamp_and_metadata():
    repository = FakeRepository()
    effects = LeadEffectExecutor(
        LeadService(repository),
        now=lambda: "2026-09-09 02:00:00",
    )

    result = effects.record_search(user=_user(), intent=_search_intent())

    assert result.status == "recorded"
    assert result.record is not None and result.record.lead_id == 41
    assert repository.rows[0]["action"] == "search_pref_submit"
    assert repository.rows[0]["source"] == "user_search"
    assert repository.rows[0]["area"] == "BKK1"
    assert repository.rows[0]["budget_min"] == 600
    assert repository.rows[0]["budget_max"] == 900
    assert repository.rows[0]["message_id"] == 88
    assert repository.rows[0]["created_at"] == "2026-09-09 02:00:00"


def test_lead_write_failure_is_best_effort_and_never_raises():
    effects = LeadEffectExecutor(
        LeadService(FakeRepository(fail=True)),
        now=lambda: "2026-09-09 02:00:00",
    )

    result = effects.record_search(user=_user(), intent=_search_intent())

    assert result.status == "failed"
    assert result.error == "lead_write_failed"


def test_reused_appointment_skips_duplicate_lead():
    repository = FakeRepository()
    effects = LeadEffectExecutor(LeadService(repository))
    execution = AppointmentSubmitExecution(
        public_listing_id="QL-RF-A2B3",
        listing_id="l_1001",
        submission=AppointmentSubmissionResult(
            kind="reused",
            appointment_id=7,
            lead_action=None,
            lead_source="listing_callback",
        ),
        effects=AppointmentEffectPlan(("notify_admin", "sync_channel")),
    )

    result = effects.record_appointment(
        user=_user(),
        execution=execution,
        draft=_draft(),
    )

    assert result.status == "skipped"
    assert repository.rows == []


def test_created_appointment_records_fixed_sha_lead_shape():
    repository = FakeRepository()
    effects = LeadEffectExecutor(
        LeadService(repository),
        now=lambda: "2026-09-09 02:00:00",
    )
    execution = AppointmentSubmitExecution(
        public_listing_id="QL-RF-A2B3",
        listing_id="l_1001",
        submission=AppointmentSubmissionResult(
            kind="created",
            appointment_id=7,
            lead_action="appointment_submit",
            lead_source="listing_callback",
        ),
        effects=AppointmentEffectPlan(("record_lead", "notify_admin")),
    )

    result = effects.record_appointment(
        user=_user(),
        execution=execution,
        draft=_draft(),
    )

    assert result.status == "recorded"
    row = repository.rows[0]
    assert row["action"] == "appointment_submit"
    assert row["listing_id"] == "l_1001"
    assert row["payload"]["appointment_id"] == 7
    assert row["payload"]["viewing_mode"] == "offline"
    assert row["payload"]["appointment_date"] == "09-10"
    assert row["payload"]["appointment_time"] == "pm"

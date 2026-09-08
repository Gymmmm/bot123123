from __future__ import annotations

from types import SimpleNamespace

from v3_core.user_bot.appointment_service import AppointmentSubmissionResult
from v3_core.user_bot.appointment_side_effects import AppointmentEffectPlan
from v3_core.user_bot.appointment_submit_executor import AppointmentSubmitExecution
from v3_core.user_bot.consult import ConsultIntent
from v3_core.user_bot.lead_service import (
    LeadService,
    LeadUser,
    appointment_lead_request,
    general_contact_lead_request,
    listing_contact_lead_request,
    search_lead_request,
)
from v3_core.user_bot.public_appointment import PublicAppointmentDraft
from v3_core.user_bot.search_query import SearchCriteria
from v3_core.user_bot.transition_actions import SearchSubmitIntent


class MemoryLeadRepository:
    def __init__(self):
        self.rows = []

    def create(self, values):
        self.rows.append(dict(values))
        return len(self.rows)


def test_search_lead_request_matches_fixed_sha_search_pref_shape():
    intent = SearchSubmitIntent(
        criteria=SearchCriteria(
            property_type="公寓",
            location_keys=("BKK1",),
            budget_min=600,
            budget_max=900,
        ),
        source="user_search",
        goal="住宅",
        area_display="BKK1",
        budget_label="$600–900",
        touch_payload={"from_public_listing_id": "QL-RF-A2B3"},
    )

    request = search_lead_request(intent)

    assert request.action == "search_pref_submit"
    assert request.source == "user_search"
    assert request.area == "BKK1"
    assert request.property_type == "公寓"
    assert request.budget_min == 600
    assert request.budget_max == 900
    assert request.payload == {
        "goal": "住宅",
        "area_hint": "BKK1",
        "budget_label": "$600–900",
        "from_public_listing_id": "QL-RF-A2B3",
    }


def test_appointment_created_or_updated_lead_shape_and_duplicate_has_no_lead():
    draft = PublicAppointmentDraft(
        "QL-RF-A2B3",
        mode="video",
        date="09-10",
        time="pm",
        source="listing_callback",
    )
    created = AppointmentSubmitExecution(
        public_listing_id=draft.public_listing_id,
        listing_id="LST_PRIVATE_1",
        submission=AppointmentSubmissionResult(
            kind="created",
            appointment_id=7,
            lead_action="appointment_submit",
            lead_source="listing_callback",
        ),
        effects=AppointmentEffectPlan(("record_lead",)),
    )

    request = appointment_lead_request(created, draft)
    assert request is not None
    assert request.action == "appointment_submit"
    assert request.source == "listing_callback"
    assert request.listing_id == "LST_PRIVATE_1"
    assert request.payload == {
        "appointment_id": 7,
        "viewing_mode": "video",
        "appointment_date": "09-10",
        "appointment_time": "pm",
    }

    reused = AppointmentSubmitExecution(
        public_listing_id=draft.public_listing_id,
        listing_id="LST_PRIVATE_1",
        submission=AppointmentSubmissionResult(
            kind="reused",
            appointment_id=7,
            lead_action=None,
            lead_source="listing_callback",
            should_recompute_listing_availability=False,
        ),
        effects=AppointmentEffectPlan(("sync_channel", "notify_admin")),
    )
    assert appointment_lead_request(reused, draft) is None


def test_general_and_listing_contact_use_fixed_sha_consult_action():
    general = general_contact_lead_request(source="hub")
    assert general.action == "consult_menu_click"
    assert general.source == "hub"
    assert general.listing_id == ""

    listing = listing_contact_lead_request(
        ConsultIntent(
            listing_id="LST_PRIVATE_1",
            public_listing_id="QL-RF-A2B3",
            source="listing_callback",
            inventory_status="active",
            offer_status="active",
            publication_instance_id="PUB_1",
        )
    )
    assert listing.action == "consult_menu_click"
    assert listing.source == "listing_callback"
    assert listing.listing_id == "LST_PRIVATE_1"


def test_lead_service_normalizes_attribution_fields_before_repository_write():
    repo = MemoryLeadRepository()
    service = LeadService(repo)
    request = general_contact_lead_request(source="hub")
    request = type(request)(
        action=request.action,
        source=request.source,
        payload={
            "channel_message_id": "777",
            "post_token": " token-1 ",
            "caption_variant": "a",
            "agent_id": "agent-1",
            "response_at": "2026-09-09 02:00:00",
        },
    )

    result = service.record(
        user=LeadUser(123, "alice", "Alice"),
        request=request,
        created_at="2026-09-09 02:01:00",
    )

    assert result.lead_id == 1
    row = repo.rows[0]
    assert row["user_id"] == 123
    assert row["username"] == "alice"
    assert row["message_id"] == 777
    assert row["post_token"] == "token-1"
    assert row["caption_variant"] == "a"
    assert row["agent_id"] == "agent-1"
    assert row["response_at"] == "2026-09-09 02:00:00"
    assert row["created_at"] == "2026-09-09 02:01:00"

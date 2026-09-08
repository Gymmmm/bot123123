from __future__ import annotations

import pytest

from v3_core.user_bot.public_appointment import (
    PublicAppointmentDraft,
    PublicAppointmentResolver,
)
from v3_core.user_bot.public_inventory import PublishedListingView


class MemoryInventory:
    def __init__(self, view=None):
        self.view = view
        self.calls = []

    def resolve(self, public_listing_id):
        self.calls.append(str(public_listing_id))
        if self.view and self.view.public_listing_id == str(public_listing_id):
            return self.view
        return None


def _view(*, listing_status="active", offer_status="active"):
    return PublishedListingView(
        listing={
            "listing_id": "LST_INTERNAL_1",
            "public_listing_id": "QL-RF-A2B3",
            "inventory_status": listing_status,
        },
        offer={
            "offer_id": "OFF_1",
            "offer_type": "rent",
            "offer_status": offer_status,
            "publication_policy": "telegram_rent",
        },
        publication={"instance_id": "PUB_1", "publish_status": "published"},
        package={"snapshot_json": "{}", "gallery_json": "[]"},
    )


def test_public_appointment_draft_normalizes_public_identity_and_never_stores_internal_id():
    draft = PublicAppointmentDraft(
        public_listing_id="ql-rf-a2b3",
        source="listing_callback",
    )

    assert draft.public_listing_id == "QL-RF-A2B3"
    assert draft.mode == "offline"
    assert draft.step == "date"
    assert not hasattr(draft, "listing_id")
    assert "LST_" not in repr(draft)


def test_public_draft_preserves_date_time_state_without_internal_identity():
    draft = (
        PublicAppointmentDraft("QL-RF-A2B3")
        .with_mode("video")
        .with_date("9月10日")
        .with_time("pm")
    )

    assert draft.public_listing_id == "QL-RF-A2B3"
    assert draft.mode == "video"
    assert draft.date == "9月10日"
    assert draft.time == "pm"
    assert draft.ready


def test_submit_boundary_revalidates_publication_and_bookability_before_internal_conversion():
    inventory = MemoryInventory(_view())
    public = PublicAppointmentDraft(
        "QL-RF-A2B3",
        mode="video",
        date="9月10日",
        time="pm",
        source="listing_callback",
    )

    internal = PublicAppointmentResolver(inventory).resolve(public)

    assert inventory.calls == ["QL-RF-A2B3"]
    assert internal.listing_id == "LST_INTERNAL_1"
    assert internal.mode == "video"
    assert internal.date == "9月10日"
    assert internal.time == "pm"
    assert internal.source == "listing_callback"


def test_rented_or_unpublished_listing_cannot_cross_submit_boundary():
    rented = PublicAppointmentResolver(
        MemoryInventory(_view(listing_status="rented", offer_status="inactive"))
    )
    with pytest.raises(ValueError, match="listing_not_bookable"):
        rented.resolve(PublicAppointmentDraft("QL-RF-A2B3"))

    unpublished = PublicAppointmentResolver(MemoryInventory(None))
    with pytest.raises(ValueError, match="listing_not_publicly_published"):
        unpublished.resolve(PublicAppointmentDraft("QL-RF-A2B3"))


def test_invalid_non_public_identity_is_rejected_at_session_draft_creation():
    for value in ("QC0350", "LST_1", "", "not-an-id"):
        with pytest.raises(ValueError, match="appointment_requires_public_listing_id"):
            PublicAppointmentDraft(value)

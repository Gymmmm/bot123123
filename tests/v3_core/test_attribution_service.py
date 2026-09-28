from __future__ import annotations

from v3_core.attribution.service import AttributionService, UserIdentity
from v3_core.publishing.channel_contract import channel_start_payload


PUBLIC_ID = "QL-RF-A2B3"


class MemoryAttributionRepository:
    def __init__(self):
        self.rows = {}
        self.upserts = []

    def get(self, user_id: int):
        row = self.rows.get(int(user_id))
        return dict(row) if row else None

    def upsert(self, touch):
        row = dict(touch)
        self.rows[int(row["user_id"])] = row
        self.upserts.append(row)


def test_explicit_service_freezes_first_touch_and_updates_latest_touch():
    repo = MemoryAttributionRepository()
    service = AttributionService(repo)
    identity = UserIdentity(123, "alice", "Alice")

    first = service.remember(
        identity,
        start_arg=channel_start_payload(PUBLIC_ID, "details"),
        now="2026-09-08 20:00:00",
    )
    latest = service.remember(
        identity,
        action="appointment_submit",
        source="listing_card",
        listing_id=PUBLIC_ID,
        payload={"listing_id": PUBLIC_ID},
        now="2026-09-08 20:10:00",
    )

    assert first["first_source_type"] == "channel_listing_detail"
    assert latest["first_source_type"] == "channel_listing_detail"
    assert latest["first_entry_at"] == "2026-09-08 20:00:00"
    assert latest["latest_source_type"] == "bot_listing_book"
    assert latest["latest_touch_at"] == "2026-09-08 20:10:00"
    assert len(repo.upserts) == 2


def test_prepare_lead_returns_attributed_payload_without_calling_a_lead_writer():
    repo = MemoryAttributionRepository()
    service = AttributionService(repo)
    identity = UserIdentity(123, "alice", "Alice")

    prepared = service.prepare_lead(
        identity,
        action="photos_click",
        source="channel_deeplink",
        listing_id=PUBLIC_ID,
        payload={"listing_id": PUBLIC_ID, "custom": "keep"},
        now="2026-09-08 20:00:00",
    )

    assert prepared.payload["custom"] == "keep"
    assert prepared.payload["source_type"] == "channel_listing_photos"
    assert prepared.payload["first_source_type"] == "channel_listing_photos"
    assert prepared.payload["entry_action"] == "photos_click"
    assert prepared.touch == repo.rows[123]


def test_service_has_no_schema_or_lead_storage_side_effect_without_repository_implementation():
    repo = MemoryAttributionRepository()
    service = AttributionService(repo)

    result = service.remember(
        UserIdentity(0),
        action="smart_search",
        source="user_search",
        now="2026-09-08 20:00:00",
    )

    assert result["user_id"] == 0
    assert repo.rows[0]["latest_source_type"] == "bot_search"

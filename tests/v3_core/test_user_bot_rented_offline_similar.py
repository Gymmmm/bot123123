"""Tests for rented/offline similar search feature."""
from __future__ import annotations

import json

import pytest

from v3_core.user_bot.listing_responses import SemanticAction
from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.similar_intent import SimilarIntentService, _parse_budget_from_frozen, _parse_room_type_from_frozen


class MemoryInventory:
    def __init__(self, views: dict[str, PublishedListingView]):
        self.views = views

    def resolve(self, public_listing_id):
        return self.views.get(str(public_listing_id))


def _frozen_view(
    *,
    status="active",
    offer_status="active",
    project_name="富力城",
    layout="2房1厅",
    monthly_rent_usd=800,
    payment_terms="押1付1",
    contract_term="1年",
):
    """Create a frozen view matching the v3_publication_snapshot.v1 schema."""
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_1",
        "public_listing_id": "QL-RF-A2B3",
        "offer_id": "OFF_1",
        "canonical_record_id": "CAN_1",
        "canonical_facts_hash": "hash-1",
        "canonical_facts": {},
        "listing": {
            "project_name": project_name,
            "property_type": "公寓",
            "layout": layout,
            "room_type": layout,
            "public_location_display": "BKK1",
            "public_location_key": "BKK1",
            "size_sqm": 95,
            "floor": "19",
        },
        "offer": {
            "offer_type": "rent",
            "monthly_rent_usd": monthly_rent_usd,
            "payment_terms": payment_terms,
            "contract_term": contract_term,
            "publication_policy": "telegram_rent",
        },
    }
    return PublishedListingView(
        listing={
            "listing_id": "LST_1",
            "public_listing_id": "QL-RF-A2B3",
            "inventory_status": status,
        },
        offer={
            "offer_id": "OFF_1",
            "offer_type": "rent",
            "offer_status": offer_status,
            "publication_policy": "telegram_rent",
        },
        publication={"instance_id": "PUB_1"},
        package={
            "snapshot_json": json.dumps(snapshot, ensure_ascii=False),
            "gallery_json": "[]",
        },
    )


class TestParseBudgetFromFrozen:
    def test_parses_rent_from_frozen_offer(self):
        frozen = {"monthly_rent_usd": 850}
        budget_min, budget_max, budget_label = _parse_budget_from_frozen(frozen)
        assert budget_min == 850
        assert budget_max == 850
        assert budget_label == "$850/月"

    def test_returns_empty_for_missing_rent(self):
        frozen = {}
        budget_min, budget_max, budget_label = _parse_budget_from_frozen(frozen)
        assert budget_min is None
        assert budget_max is None
        assert budget_label == ""


class TestParseRoomTypeFromFrozen:
    def test_parses_room_type_and_property_type(self):
        frozen = {"room_type": "2房1厅", "property_type": "公寓"}
        room_type, property_type = _parse_room_type_from_frozen(frozen)
        assert room_type == "2房1厅"
        assert property_type == "公寓"


class TestRentedOfflineSimilarSearch:
    def test_rented_listing_includes_budget_in_intent(self):
        """Rented listings should include budget/room_type for direct similar search."""
        view = _frozen_view(status="rented", offer_status="inactive", monthly_rent_usd=900)
        inventory = MemoryInventory({"QL-RF-A2B3": view})
        service = SimilarIntentService(inventory)
        
        result = service.resolve("QL-RF-A2B3")
        
        assert result.ok
        assert result.intent is not None
        assert result.intent.goal == "any"
        assert result.intent.next_step == "search_submit"
        assert result.intent.budget_min == 900
        assert result.intent.budget_max == 900
        assert result.intent.budget_label == "$900/月"
        assert result.intent.room_type == "2房1厅"
        assert result.intent.property_type == "公寓"

    def test_offline_listing_includes_budget_in_intent(self):
        """Offline listings should include budget/room_type for direct similar search."""
        view = _frozen_view(status="offline", offer_status="inactive", monthly_rent_usd=1200)
        inventory = MemoryInventory({"QL-RF-A2B3": view})
        service = SimilarIntentService(inventory)
        
        result = service.resolve("QL-RF-A2B3")
        
        assert result.ok
        assert result.intent is not None
        assert result.intent.goal == "any"
        assert result.intent.next_step == "search_submit"
        assert result.intent.budget_min == 1200
        assert result.intent.budget_max == 1200

    def test_inactive_listing_includes_budget_in_intent(self):
        """Inactive listings should include budget/room_type for direct similar search."""
        view = _frozen_view(status="inactive", offer_status="inactive", monthly_rent_usd=750)
        inventory = MemoryInventory({"QL-RF-A2B3": view})
        service = SimilarIntentService(inventory)
        
        result = service.resolve("QL-RF-A2B3")
        
        assert result.ok
        assert result.intent is not None
        assert result.intent.goal == "any"
        assert result.intent.next_step == "search_submit"

    def test_active_listing_asks_for_budget(self):
        """Active listings should ask for budget preference."""
        view = _frozen_view(status="active", offer_status="active")
        inventory = MemoryInventory({"QL-RF-A2B3": view})
        service = SimilarIntentService(inventory)
        
        result = service.resolve("QL-RF-A2B3")
        
        assert result.ok
        assert result.intent is not None
        assert result.intent.goal == "budget"
        assert result.intent.next_step == "budget"
        # No budget extracted for active listings
        assert result.intent.budget_min is None
        assert result.intent.budget_max is None
        assert result.intent.budget_label == ""

    def test_reserved_listing_asks_for_budget(self):
        """Reserved listings should ask for budget preference (still bookable)."""
        view = _frozen_view(status="reserved", offer_status="active")
        inventory = MemoryInventory({"QL-RF-A2B3": view})
        service = SimilarIntentService(inventory)
        
        result = service.resolve("QL-RF-A2B3")
        
        assert result.ok
        assert result.intent is not None
        assert result.intent.goal == "budget"
        assert result.intent.next_step == "budget"

    def test_pending_listing_asks_for_budget(self):
        """Pending listings should ask for budget preference."""
        view = _frozen_view(status="pending", offer_status="active")
        inventory = MemoryInventory({"QL-RF-A2B3": view})
        service = SimilarIntentService(inventory)
        
        result = service.resolve("QL-RF-A2B3")
        
        assert result.ok
        assert result.intent is not None
        assert result.intent.goal == "budget"
        assert result.intent.next_step == "budget"

    def test_source_is_preserved(self):
        """Source should be preserved through the intent."""
        view = _frozen_view(status="rented")
        inventory = MemoryInventory({"QL-RF-A2B3": view})
        service = SimilarIntentService(inventory)
        
        result = service.resolve("QL-RF-A2B3", source="listing_details")
        
        assert result.ok
        assert result.intent is not None
        assert result.intent.source == "listing_details"


class TestRentedListingDetailsKeyboard:
    def test_rented_listing_no_book_button(self):
        """Rented listings should not show book button in details keyboard."""
        view = _frozen_view(status="rented", offer_status="inactive")
        snapshot = json.loads(view.package["snapshot_json"])
        from v3_core.user_bot.listing_responses import build_details_response
        response = build_details_response(view)
        
        labels = [action.label for row in response.action_rows for action in row]
        assert "📅 预约看房" not in labels
        assert "💬 咨询这套" in labels
        assert "🔍 继续找房" in labels

    def test_offline_listing_no_book_button(self):
        """Offline listings should not show book button."""
        view = _frozen_view(status="offline", offer_status="inactive")
        from v3_core.user_bot.listing_responses import build_details_response
        response = build_details_response(view)
        
        labels = [action.label for row in response.action_rows for action in row]
        assert "📅 预约看房" not in labels

    def test_pending_listing_no_book_button(self):
        """Pending listings should not show book button."""
        view = _frozen_view(status="pending", offer_status="active")
        from v3_core.user_bot.listing_responses import build_details_response
        response = build_details_response(view)
        
        labels = [action.label for row in response.action_rows for action in row]
        assert "📅 预约看房" not in labels

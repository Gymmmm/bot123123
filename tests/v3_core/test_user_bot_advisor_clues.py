"""Tests for structured advisor clues feature."""
from __future__ import annotations

import json

import pytest

from v3_core.user_bot.consult import ConsultIntent
from v3_core.user_bot.listing_contact import build_structured_advisor_clues
from v3_core.user_bot.public_inventory import PublishedListingView


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
):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_INTERNAL_123",
        "public_listing_id": "QL-RF-A2B3",
        "offer_id": "OFF_1",
        "listing": {
            "project_name": project_name,
            "property_type": "公寓",
            "layout": layout,
            "public_location_display": "BKK1",
            "size_sqm": 95,
            "floor": "19",
        },
        "offer": {
            "offer_type": "rent",
            "monthly_rent_usd": monthly_rent_usd,
            "payment_terms": "押1付1",
            "contract_term": "1年",
            "publication_policy": "telegram_rent",
        },
    }
    return PublishedListingView(
        listing={
            "listing_id": "LST_INTERNAL_123",
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


class TestStructuredAdvisorClues:
    def test_includes_source(self):
        view = _frozen_view()
        inventory = MemoryInventory({"QL-RF-A2B3": view})
        intent = ConsultIntent(
            listing_id="LST_INTERNAL_123",
            public_listing_id="QL-RF-A2B3",
            source="channel_deeplink",
            inventory_status="active",
            offer_status="active",
            publication_instance_id="PUB_1",
            touchpoint="listing_details",
        )
        
        clues = build_structured_advisor_clues(intent, inventory)
        
        assert any("来源" in c and "频道房源" in c for c in clues)

    def test_includes_internal_listing_id(self):
        view = _frozen_view()
        inventory = MemoryInventory({"QL-RF-A2B3": view})
        intent = ConsultIntent(
            listing_id="LST_INTERNAL_123",
            public_listing_id="QL-RF-A2B3",
            source="listing_callback",
            inventory_status="active",
            offer_status="active",
            publication_instance_id="PUB_1",
        )
        
        clues = build_structured_advisor_clues(intent, inventory)
        
        assert any("内部ID" in c and "LST_INTERNAL_123" in c for c in clues)
        # Internal ID should NOT contain the public listing ID pattern
        assert not any("QL-RF-A2B3" in c and "内部ID" in c for c in clues)

    def test_includes_price(self):
        view = _frozen_view(monthly_rent_usd=950)
        inventory = MemoryInventory({"QL-RF-A2B3": view})
        intent = ConsultIntent(
            listing_id="LST_INTERNAL_123",
            public_listing_id="QL-RF-A2B3",
            source="listing_callback",
            inventory_status="active",
            offer_status="active",
            publication_instance_id="PUB_1",
        )
        
        clues = build_structured_advisor_clues(intent, inventory)
        
        assert any("价格" in c and "$950" in c for c in clues)

    def test_includes_layout(self):
        view = _frozen_view(layout="3房2卫")
        inventory = MemoryInventory({"QL-RF-A2B3": view})
        intent = ConsultIntent(
            listing_id="LST_INTERNAL_123",
            public_listing_id="QL-RF-A2B3",
            source="listing_callback",
            inventory_status="active",
            offer_status="active",
            publication_instance_id="PUB_1",
        )
        
        clues = build_structured_advisor_clues(intent, inventory)
        
        assert any("户型" in c and "3房2卫" in c for c in clues)

    def test_includes_status(self):
        view = _frozen_view(status="active")
        inventory = MemoryInventory({"QL-RF-A2B3": view})
        intent = ConsultIntent(
            listing_id="LST_INTERNAL_123",
            public_listing_id="QL-RF-A2B3",
            source="listing_callback",
            inventory_status="active",
            offer_status="active",
            publication_instance_id="PUB_1",
        )
        
        clues = build_structured_advisor_clues(intent, inventory)
        
        assert any("房态" in c and ("🟢" in c or "当前可预约" in c) for c in clues)

    def test_includes_rented_status_for_rented_listing(self):
        view = _frozen_view(status="rented", offer_status="inactive")
        inventory = MemoryInventory({"QL-RF-A2B3": view})
        intent = ConsultIntent(
            listing_id="LST_INTERNAL_123",
            public_listing_id="QL-RF-A2B3",
            source="listing_callback",
            inventory_status="rented",
            offer_status="inactive",
            publication_instance_id="PUB_1",
        )
        
        clues = build_structured_advisor_clues(intent, inventory)
        
        assert any("房态" in c and ("🔴" in c or "已租出" in c) for c in clues)

    def test_includes_touchpoint_when_present(self):
        view = _frozen_view()
        inventory = MemoryInventory({"QL-RF-A2B3": view})
        intent = ConsultIntent(
            listing_id="LST_INTERNAL_123",
            public_listing_id="QL-RF-A2B3",
            source="channel_deeplink",
            inventory_status="active",
            offer_status="active",
            publication_instance_id="PUB_1",
            touchpoint="listing_photos",
        )
        
        clues = build_structured_advisor_clues(intent, inventory)
        
        assert any("转化页" in c for c in clues)

    def test_returns_empty_for_missing_view(self):
        inventory = MemoryInventory({})
        intent = ConsultIntent(
            listing_id="LST_INTERNAL_123",
            public_listing_id="QL-RF-MISSING",
            source="listing_callback",
            inventory_status="active",
            offer_status="active",
            publication_instance_id="PUB_1",
        )
        
        clues = build_structured_advisor_clues(intent, inventory)
        
        assert clues == []

    def test_customer_placeholder_included(self):
        view = _frozen_view()
        inventory = MemoryInventory({"QL-RF-A2B3": view})
        intent = ConsultIntent(
            listing_id="LST_INTERNAL_123",
            public_listing_id="QL-RF-A2B3",
            source="listing_callback",
            inventory_status="active",
            offer_status="active",
            publication_instance_id="PUB_1",
        )
        
        clues = build_structured_advisor_clues(intent, inventory)
        
        assert any("客户" in c and "{customer}" in c for c in clues)

    def test_action_is_user_consult(self):
        view = _frozen_view()
        inventory = MemoryInventory({"QL-RF-A2B3": view})
        intent = ConsultIntent(
            listing_id="LST_INTERNAL_123",
            public_listing_id="QL-RF-A2B3",
            source="listing_callback",
            inventory_status="active",
            offer_status="active",
            publication_instance_id="PUB_1",
        )
        
        clues = build_structured_advisor_clues(intent, inventory)
        
        assert any("行为" in c and "咨询" in c for c in clues)

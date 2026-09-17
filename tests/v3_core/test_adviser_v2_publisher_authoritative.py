from __future__ import annotations

import json

from v3_core.publishing.package_service import PackageBuildService


class _Reader:
    def __init__(self, facts):
        self.facts = dict(facts)

    def listing(self, listing_id):
        return {
            "listing_id": listing_id,
            "public_listing_id": "QL-RF-A2B3",
            "canonical_record_id": "CAN_1",
            "inventory_status": "active",
            "project_name": "富力城",
            "layout": self.facts.get("layout") or "",
            "public_location_display": "洪森大道",
        }

    def offer(self, offer_id):
        return {
            "offer_id": offer_id,
            "listing_id": "LST_1",
            "offer_type": "rent",
            "offer_status": "active",
            "publication_policy": "telegram_rent",
            "monthly_rent_usd": 800,
        }

    def canonical(self, canonical_record_id):
        return {
            "canonical_record_id": canonical_record_id,
            "facts_hash": "facts-hash",
            "facts": dict(self.facts),
        }


class _Store:
    def __init__(self):
        self.kwargs = None

    def create(self, **kwargs):
        self.kwargs = kwargs
        return kwargs


def _build(facts, adviser_copy_override=None):
    store = _Store()
    service = PackageBuildService(
        reader=_Reader(facts),
        store=store,
        user_bot_username="QiaolianTestBot",
        advisor_url="https://t.me/qiaolian",
    )
    service.build(
        listing_id="LST_1",
        offer_id="OFF_1",
        cover_style="right_price",
        cover_path="/tmp/cover.jpg",
        gallery=[],
        adviser_copy_override=adviser_copy_override,
    )
    return store.kwargs["snapshot"]


def test_publisher_v2_freezes_at_most_one_auto_adviser_paragraph():
    snapshot = _build({
        "layout": "2房1厅",
        "bedrooms": 2,
        "adviser_signals": ["never_lived", "pet_allowed", "river_view", "cleaning_2x"],
    })
    assert snapshot["adviser_copy"]
    assert "\n" not in snapshot["adviser_copy"]
    assert snapshot["adviser_copy_version"] == "v2_publisher_authoritative"
    assert snapshot["adviser_copy_source"] == "auto"


def test_manual_adviser_override_still_wins_and_is_normalized_to_one_paragraph():
    snapshot = _build(
        {"layout": "3房2厅", "bedrooms": 3, "adviser_signals": ["never_lived"]},
        adviser_copy_override="人工第一句。\n人工第二句。",
    )
    assert snapshot["adviser_copy"] == "人工第一句。 人工第二句。"
    assert snapshot["adviser_copy_source"] == "manual"
    assert snapshot["adviser_copy_version"] == "v2_publisher_authoritative"


def test_empty_manual_override_remains_hidden():
    snapshot = _build(
        {"layout": "2房1厅", "bedrooms": 2, "adviser_signals": ["never_lived"]},
        adviser_copy_override="",
    )
    assert snapshot["adviser_copy"] == ""
    assert snapshot["adviser_copy_source"] == "hidden"

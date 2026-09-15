from __future__ import annotations

import json

from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.public_search import SearchExecution
from v3_core.user_bot.search_flow import SearchFlowService
from v3_core.user_bot.search_query import SearchCriteria


def _view(
    *,
    listing_id: str,
    public_id: str,
    project: str,
    location: str,
    rent: int,
    status: str = "active",
):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": listing_id,
        "public_listing_id": public_id,
        "offer_id": f"OFF_{listing_id}",
        "canonical_record_id": f"CAN_{listing_id}",
        "canonical_facts_hash": f"hash-{listing_id}",
        "canonical_facts": {},
        "listing": {
            "project_name": project,
            "property_type": "公寓",
            "layout": "2房1厅",
            "public_location_display": location,
            "size_sqm": 90,
            "floor": "12",
        },
        "offer": {
            "offer_type": "rent",
            "monthly_rent_usd": rent,
            "publication_policy": "telegram_rent",
        },
    }
    return PublishedListingView(
        listing={
            "listing_id": listing_id,
            "public_listing_id": public_id,
            "inventory_status": status,
        },
        offer={
            "offer_id": f"OFF_{listing_id}",
            "offer_type": "rent",
            "offer_status": "active",
            "publication_policy": "telegram_rent",
        },
        publication={"instance_id": f"PUB_{listing_id}"},
        package={
            "snapshot_json": json.dumps(snapshot, ensure_ascii=False),
            "gallery_json": "[]",
            "cover_path": "",
        },
    )


class RecordingSearch:
    def __init__(self, *, strict: SearchExecution, similar: SearchExecution | None = None):
        self.strict_result = strict
        self.similar_result = similar or SearchExecution(items=(), mode="no_match")
        self.calls = []

    def strict_for_results(self, criteria, *, limit=5):
        self.calls.append(("strict_for_results", criteria, limit))
        return self.strict_result

    def similar(self, criteria, *, limit=5):
        self.calls.append(("similar", criteria, limit))
        return self.similar_result


def test_raw_text_flows_through_parser_search_and_frozen_cards():
    first = _view(
        listing_id="LST_1",
        public_id="QL-BK-A2B3",
        project="BKK公寓",
        location="BKK1",
        rent=800,
    )
    search = RecordingSearch(strict=SearchExecution(items=(first,), mode="strict"))
    flow = SearchFlowService(search)

    result = flow.search_text("BKK1 800以内 一房", limit=5)

    assert result.criteria.location_keys == ("BKK1",)
    assert result.criteria.budget_min is None
    assert result.criteria.budget_max == 800
    assert result.criteria.room_type == "1房"
    assert result.criteria.property_type == ""
    assert result.mode == "strict"
    assert result.public_listing_ids == ("QL-BK-A2B3",)
    assert len(result.cards) == 1
    assert result.cards[0].public_listing_id == "QL-BK-A2B3"
    assert result.matched
    assert [call[0] for call in search.calls] == ["strict_for_results"]


def test_flow_preserves_single_result_augmentation_marker_without_reclassifying_mode():
    first = _view(
        listing_id="LST_1",
        public_id="QL-BK-A2B3",
        project="严格命中",
        location="BKK1",
        rent=800,
    )
    similar = _view(
        listing_id="LST_2",
        public_id="QL-BK-C4D5",
        project="相似房源",
        location="BKK1",
        rent=780,
    )
    search = RecordingSearch(
        strict=SearchExecution(
            items=(first, similar),
            mode="strict",
            has_similar=True,
        )
    )
    flow = SearchFlowService(search)

    result = flow.search_text("BKK1 公寓 800以内")

    assert result.mode == "strict"
    assert result.has_similar
    assert result.public_listing_ids == ("QL-BK-A2B3", "QL-BK-C4D5")
    assert result.cards[0].action_rows[0][0].action == "previous"
    assert result.cards[0].action_rows[0][1].action == "next"


def test_normal_no_match_never_calls_explicit_similar_path():
    search = RecordingSearch(
        strict=SearchExecution(items=(), mode="no_match"),
        similar=SearchExecution(
            items=(
                _view(
                    listing_id="LST_2",
                    public_id="QL-BK-C4D5",
                    project="本可作为类似房源",
                    location="BKK1",
                    rent=780,
                ),
            ),
            mode="budget_only",
        ),
    )
    flow = SearchFlowService(search)

    result = flow.search_text("钻石岛 办公室 800以内")

    assert result.mode == "no_match"
    assert result.cards == ()
    assert result.public_listing_ids == ()
    assert not result.matched
    assert not result.has_similar
    assert [call[0] for call in search.calls] == ["strict_for_results"]


def test_explicit_similar_is_a_separate_flow_and_preserves_relaxation_mode():
    item = _view(
        listing_id="LST_3",
        public_id="QL-B2-E6F7",
        project="BKK2公寓",
        location="BKK2",
        rent=750,
        status="reserved",
    )
    search = RecordingSearch(
        strict=SearchExecution(items=(), mode="no_match"),
        similar=SearchExecution(items=(item,), mode="no_type"),
    )
    flow = SearchFlowService(search)
    criteria = SearchCriteria(
        property_type="别墅",
        location_keys=("BKK2",),
        budget_min=700,
        budget_max=900,
    )

    result = flow.similar(criteria, limit=3)

    assert result.criteria is criteria
    assert result.mode == "no_type"
    assert result.public_listing_ids == ("QL-B2-E6F7",)
    assert "已有预约 · 仍可预约" in result.cards[0].text
    assert [call[0] for call in search.calls] == ["similar"]
    assert search.calls[0][2] == 3


def test_guided_search_criteria_reuses_same_normal_strict_flow():
    item = _view(
        listing_id="LST_1",
        public_id="QL-BK-A2B3",
        project="BKK公寓",
        location="BKK1",
        rent=800,
    )
    search = RecordingSearch(strict=SearchExecution(items=(item,), mode="strict"))
    flow = SearchFlowService(search)
    criteria = SearchCriteria(
        property_type="公寓",
        location_keys=("BKK1",),
        budget_max=800,
        room_type="2房",
        raw_text="guided",
    )

    result = flow.search_criteria(criteria, limit=4)

    assert result.criteria is criteria
    assert result.public_listing_ids == ("QL-BK-A2B3",)
    assert search.calls[0][0] == "strict_for_results"
    assert search.calls[0][1] is criteria
    assert search.calls[0][2] == 4

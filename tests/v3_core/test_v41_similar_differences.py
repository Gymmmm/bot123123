"""V4.1: similar cards show ≤2 short differences, only vs criteria the user chose."""
from __future__ import annotations

from types import SimpleNamespace

from v3_core.user_bot.search_flow import SearchFlowService
from v3_core.user_bot.search_query import SearchCriteria
from v3_core.user_bot.similar_differences import MAX_DIFFERENCES, listing_differences

from test_v4_result_cards import _view


def _diff(view, criteria, rent=800, location="堆谷", layout="2房1厅"):
    return listing_differences(view, criteria, rent=rent, location=location, layout=layout)


def test_unselected_criteria_are_never_flagged():
    view = _view(rent=800, location_key="TK", location="堆谷", bedrooms=2)
    assert _diff(view, SearchCriteria()) == ()
    # Only budget chosen: area/rooms differ from nothing, so only budget can be flagged.
    assert _diff(view, SearchCriteria(budget_max=700)) == ("💰 超预算 $100",)
    # Only area chosen.
    assert _diff(view, SearchCriteria(location_keys=("BKK1",))) == ("📍 区域不同：堆谷",)


def test_room_difference_only_when_user_chose_room_count_and_bedrooms_differ():
    view = _view(bedrooms=2)
    assert _diff(view, SearchCriteria(room_type="2房")) == ()
    assert _diff(view, SearchCriteria(room_type="")) == ()
    assert _diff(view, SearchCriteria(room_type="any")) == ()
    assert _diff(view, SearchCriteria(room_type="3房")) == ("🛏️ 户型不同：2房",)


def test_at_most_two_differences():
    view = _view(rent=900, location_key="TK", location="堆谷", bedrooms=2)
    labels = _diff(view, SearchCriteria(location_keys=("BKK1",), budget_max=700, room_type="1房"))
    assert len(labels) == MAX_DIFFERENCES == 2
    assert all(len(label) <= 14 for label in labels)


def test_find_similar_from_a_listing_never_labels_listing_derived_criteria():
    item = _view("QL-BK-C4D5", rent=950, location_key="BKK1", location="BKK1", bedrooms=3)
    search = SimpleNamespace(similar=lambda criteria, limit=5: SimpleNamespace(
        items=(item,), mode="no_type", has_similar=True))
    derived = SearchCriteria(location_keys=("TK",), budget_max=800, room_type="2房")
    result = SearchFlowService(search).similar(derived, from_listing=True)
    assert result.label_criteria is None
    text = result.cards[0].text
    for word in ("超预算", "区域不同", "户型不同"):
        assert word not in text
    # A user-driven relaxed search still labels against the user's own criteria.
    user = SearchFlowService(search).similar(derived)
    assert "💰 超预算 $150" in user.cards[0].text

"""3858 natural-language room-type must reach published inventory search."""

from qiaolian_dual.search import detect_room_type, search_listings_with_fallback


def test_detect_room_type_accepts_liang_fang():
    assert detect_room_type("富力城两房") == "2房"
    assert detect_room_type("桥牌一房") == "1房"
    assert detect_room_type("The Bridge 2房") == "2房"


def test_search_listings_with_fallback_forwards_room_type(monkeypatch):
    seen = {}

    def fake_public(**kwargs):
        seen.update(kwargs)
        return [{"listing_id": "l_2"}]

    monkeypatch.setattr("qiaolian_dual.search._public_search_listings", fake_public)
    matches, mode = search_listings_with_fallback(
        property_type=None,
        area=None,
        budget_min=None,
        budget_max=None,
        project_terms=("富力城",),
        room_type="2房",
        limit=5,
    )
    assert mode == "strict"
    assert matches == [{"listing_id": "l_2"}]
    assert seen["project_terms"] == ("富力城",)
    assert seen["room_type"] == "2房"

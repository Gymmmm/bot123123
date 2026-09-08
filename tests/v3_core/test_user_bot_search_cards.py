from __future__ import annotations

import json

import pytest

from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.search_cards import build_search_card, build_search_cards


def _view(
    *,
    listing_id: str,
    public_id: str,
    project: str,
    location: str,
    rent: int,
    status: str = "active",
    cover_path: str = "",
    layout: str = "2房1厅",
    property_type: str = "公寓",
    size: float = 95,
    floor: str = "19",
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
            "property_type": property_type,
            "layout": layout,
            "public_location_display": location,
            "size_sqm": size,
            "floor": floor,
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
            "offer_status": "active" if status in {"active", "reserved"} else "inactive",
            "publication_policy": "telegram_rent",
        },
        publication={"instance_id": f"PUB_{listing_id}"},
        package={
            "snapshot_json": json.dumps(snapshot, ensure_ascii=False),
            "gallery_json": "[]",
            "cover_path": cover_path,
        },
    )


def _actions(card):
    return [[item.action for item in row] for row in card.action_rows]


def test_search_card_preserves_fixed_sha_copy_and_uses_frozen_cover(tmp_path):
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"frozen-cover")
    first = _view(
        listing_id="LST_1",
        public_id="QL-RF-A2B3",
        project="富力城",
        location="BKK1",
        rent=800,
        cover_path=str(cover),
    )
    second = _view(
        listing_id="LST_2",
        public_id="QL-BK-C4D5",
        project="第二套",
        location="BKK1",
        rent=780,
    )

    card = build_search_card((first, second), 0)

    assert card.text == (
        "🏠 <b>富力城｜2房1厅</b>\n"
        "💰 <b>$800/月</b>\n"
        "\n"
        "📍 BKK1\n"
        "📐 95㎡ · 19楼\n"
        "\n"
        "🟢 <b>当前可预约</b>\n"
        "第 1/2 套"
    )
    assert card.photo_path == str(cover)
    assert _actions(card) == [
        ["previous", "next"],
        ["details", "photos"],
        ["book"],
        ["consult"],
        ["change_search"],
    ]


def test_search_card_navigation_wraps_by_public_listing_id():
    items = (
        _view(
            listing_id="LST_1",
            public_id="QL-RF-A2B3",
            project="一",
            location="BKK1",
            rent=800,
        ),
        _view(
            listing_id="LST_2",
            public_id="QL-BK-C4D5",
            project="二",
            location="BKK1",
            rent=780,
        ),
        _view(
            listing_id="LST_3",
            public_id="QL-B2-E6F7",
            project="三",
            location="BKK2",
            rent=750,
        ),
    )

    first = build_search_card(items, 0)
    previous, next_ = first.action_rows[0]

    assert previous.action == "previous"
    assert previous.target_public_listing_id == "QL-B2-E6F7"
    assert previous.target_index == 2
    assert next_.action == "next"
    assert next_.target_public_listing_id == "QL-BK-C4D5"
    assert next_.target_index == 1

    wrapped = build_search_card(items, 4)
    assert wrapped.index == 1
    assert wrapped.public_listing_id == "QL-BK-C4D5"


def test_search_card_listing_actions_target_current_public_identity():
    view = _view(
        listing_id="LST_1",
        public_id="QL-RF-A2B3",
        project="富力城",
        location="富力城",
        rent=800,
    )

    card = build_search_card((view,), 0)
    actions = [item for row in card.action_rows for item in row]

    assert _actions(card) == [
        ["details", "photos"],
        ["book"],
        ["consult"],
        ["change_search"],
    ]
    for action in actions:
        if action.action != "change_search":
            assert action.target_public_listing_id == "QL-RF-A2B3"


def test_missing_frozen_cover_does_not_fall_back_to_legacy_media(tmp_path):
    missing = tmp_path / "missing-cover.jpg"
    view = _view(
        listing_id="LST_1",
        public_id="QL-RF-A2B3",
        project="富力城",
        location="富力城",
        rent=800,
        cover_path=str(missing),
    )

    card = build_search_card((view,), 0)

    assert card.photo_path == ""


def test_live_reserved_status_changes_badge_without_changing_frozen_facts():
    view = _view(
        listing_id="LST_1",
        public_id="QL-RF-A2B3",
        project="富力城",
        location="BKK1",
        rent=800,
        status="reserved",
    )

    card = build_search_card((view,), 0)

    assert "富力城｜2房1厅" in card.text
    assert "$800/月" in card.text
    assert "🟡 <b>已有预约 · 仍可预约</b>" in card.text
    assert _actions(card)[1 if len(card.action_rows) > 4 else 1] == ["book"]


def test_build_search_cards_builds_one_card_per_result():
    items = (
        _view(
            listing_id="LST_1",
            public_id="QL-RF-A2B3",
            project="一",
            location="BKK1",
            rent=800,
        ),
        _view(
            listing_id="LST_2",
            public_id="QL-BK-C4D5",
            project="二",
            location="BKK1",
            rent=780,
        ),
    )

    cards = build_search_cards(items)

    assert [card.index for card in cards] == [0, 1]
    assert [card.total for card in cards] == [2, 2]


def test_empty_search_card_is_rejected():
    with pytest.raises(ValueError, match="search_card_requires_results"):
        build_search_card((), 0)

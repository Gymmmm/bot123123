from __future__ import annotations

import json

import pytest

from v3_core.user_bot.listing_responses import SemanticAction, build_details_response
from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.search_cards import build_search_card
from v3_core.user_bot.telegram_ui import build_action_keyboard


def _view(*, listing_id="LST_1", public_id="QL-RF-A2B3", status="active"):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": listing_id,
        "public_listing_id": public_id,
        "offer_id": f"OFF_{listing_id}",
        "canonical_record_id": f"CAN_{listing_id}",
        "canonical_facts_hash": f"hash-{listing_id}",
        "canonical_facts": {},
        "listing": {
            "project_name": "富力城",
            "property_type": "公寓",
            "layout": "2房1厅",
            "public_location_display": "富力城",
            "size_sqm": 95,
            "floor": "19",
        },
        "offer": {
            "offer_type": "rent",
            "monthly_rent_usd": 800,
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


def _callback_rows(markup):
    return [
        [button.callback_data for button in row]
        for row in markup.inline_keyboard
    ]


def test_details_keyboard_is_encoded_from_public_listing_identity():
    response = build_details_response(_view())

    markup = build_action_keyboard(response.action_rows)

    assert _callback_rows(markup) == [
        [
            "v3u:listing:book:QL-RF-A2B3",
            "v3u:listing:photos:QL-RF-A2B3",
        ],
        ["v3u:listing:consult:QL-RF-A2B3"],
    ]
    assert "LST_1" not in repr(_callback_rows(markup))


def test_search_card_keyboard_encodes_navigation_with_public_ids():
    first = _view(listing_id="LST_1", public_id="QL-RF-A2B3")
    second = _view(listing_id="LST_2", public_id="QL-BK-C4D5")
    card = build_search_card((first, second), 0)

    markup = build_action_keyboard(card.action_rows)

    rows = _callback_rows(markup)
    assert rows[0] == [
        "v3u:card:1:QL-BK-C4D5",
        "v3u:card:1:QL-BK-C4D5",
    ]
    assert rows[1] == [
        "v3u:listing:details:QL-RF-A2B3",
        "v3u:listing:photos:QL-RF-A2B3",
    ]
    assert rows[2] == ["v3u:listing:book:QL-RF-A2B3"]
    assert rows[3] == ["v3u:listing:consult:QL-RF-A2B3"]
    assert rows[4] == ["v3u:change_search"]
    assert "LST_" not in repr(rows)


def test_three_card_navigation_distinguishes_previous_and_next_targets():
    items = (
        _view(listing_id="LST_1", public_id="QL-RF-A2B3"),
        _view(listing_id="LST_2", public_id="QL-BK-C4D5"),
        _view(listing_id="LST_3", public_id="QL-B2-E6F7"),
    )
    markup = build_action_keyboard(build_search_card(items, 0).action_rows)

    assert _callback_rows(markup)[0] == [
        "v3u:card:2:QL-B2-E6F7",
        "v3u:card:1:QL-BK-C4D5",
    ]


def test_keyboard_rejects_empty_rows_only_and_missing_labels():
    with pytest.raises(ValueError, match="telegram_keyboard_requires_actions"):
        build_action_keyboard(())
    with pytest.raises(ValueError, match="telegram_button_label_missing"):
        build_action_keyboard(((SemanticAction("", "change_search"),),))


def test_keyboard_enforces_callback_byte_limit(monkeypatch):
    import v3_core.user_bot.telegram_ui as telegram_ui

    monkeypatch.setattr(telegram_ui, "encode_semantic_action", lambda action: "x" * 65)
    with pytest.raises(ValueError, match="telegram_callback_data_too_long"):
        telegram_ui.build_action_keyboard(
            ((SemanticAction("按钮", "change_search"),),)
        )

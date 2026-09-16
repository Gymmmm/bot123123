from qiaolian_dual.session_deeplink import parse_start_arg_payload as legacy_parse_start_arg_payload
from v3_core.publishing.channel_contract import channel_start_payload
from v3_core.user_bot.deeplink import PUBLIC_CHANNEL_ACTIONS, parse_channel_start_payload


PUBLIC_IDS = (
    "QL-RF-A2B3",
    "QL-BK-C4D5",
    "QL-PP-E6F7",
)


def test_public_channel_payload_round_trips_between_publisher_and_user_bot():
    for public_id in PUBLIC_IDS:
        for action in PUBLIC_CHANNEL_ACTIONS:
            payload = channel_start_payload(public_id, action)
            route = parse_channel_start_payload(payload)
            assert route is not None
            assert route.public_listing_id == public_id
            assert route.action == action


def test_public_channel_payload_matches_locked_production_parser_for_official_actions():
    for public_id in PUBLIC_IDS:
        for action in PUBLIC_CHANNEL_ACTIONS:
            payload = channel_start_payload(public_id, action)
            legacy = legacy_parse_start_arg_payload(payload)
            route = parse_channel_start_payload(payload)
            assert legacy is not None
            assert route is not None
            assert legacy["action"] == route.action
            assert legacy["target"] == route.public_listing_id


def test_public_channel_contract_does_not_grow_fourth_listing_button():
    public_id = "QL-RF-A2B3"
    for action in ("consult", "advisor", "similar", "video", "discussion_entry", "map"):
        assert parse_channel_start_payload(f"property_{public_id}_{action}") is None


def test_public_channel_parser_rejects_internal_or_legacy_ids_for_new_v3_links():
    for target in ("l_350", "QC0350", "QJ350", "350", ""):
        assert parse_channel_start_payload(f"property_{target}_details") is None


def test_public_channel_parser_normalizes_valid_public_id_case():
    route = parse_channel_start_payload("property_ql-rf-a2b3_PhOtOs")
    assert route is not None
    assert route.public_listing_id == "QL-RF-A2B3"
    assert route.action == "photos"

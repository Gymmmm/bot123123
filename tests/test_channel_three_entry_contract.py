import pytest

from qiaolian_dual.channel_links import channel_start_payload


def test_channel_contract_is_details_photos_and_booking_only():
    listing_id = "l_350"
    assert channel_start_payload(listing_id, "details") == "property_QC0350_details"
    assert channel_start_payload(listing_id, "photos") == "property_QC0350_photos"
    assert channel_start_payload(listing_id, "book") == "property_QC0350_book"


def test_channel_does_not_add_advisor_as_a_fourth_listing_button():
    with pytest.raises(ValueError, match="unsupported_channel_action"):
        channel_start_payload("l_350", "advisor")

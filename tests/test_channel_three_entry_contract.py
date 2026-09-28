import pytest

from qiaolian_dual.channel_links import channel_start_payload
from qiaolian_dual.public_listing_id import public_listing_id


def test_channel_contract_is_details_photos_and_booking_only():
    listing_id = "l_350"
    public_id = public_listing_id(listing_id)
    assert channel_start_payload(listing_id, "details") == f"property_{public_id}_details"
    assert channel_start_payload(listing_id, "photos") == f"property_{public_id}_photos"
    assert channel_start_payload(listing_id, "book") == f"property_{public_id}_book"


def test_channel_does_not_add_advisor_as_a_fourth_listing_button():
    with pytest.raises(ValueError, match="unsupported_channel_action"):
        channel_start_payload("l_350", "advisor")

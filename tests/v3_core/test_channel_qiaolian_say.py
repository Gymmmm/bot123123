from v3_core.inventory.qiaolian_say import PHRASES
from v3_core.publishing.channel_renderer import render_channel_caption


def _listing():
    return {
        "project_name": "富力城",
        "public_location_display": "富力城",
        "property_type": "公寓",
        "layout": "2房1厅",
        "size_sqm": 95,
        "floor": "19",
        "inventory_status": "active",
    }


def _offer():
    return {
        "offer_type": "rent",
        "monthly_rent_usd": 800,
        "payment_terms": "押1付1",
        "contract_term": "1年",
    }


def test_channel_caption_uses_same_controlled_qiaolian_notes():
    caption = render_channel_caption(
        listing=_listing(),
        offer=_offer(),
        public_listing_id="QL-RF-A2B3",
        canonical_facts={"qiaolian_tags": ["management_wifi"]},
    )

    assert "💬 侨联说" in caption
    assert any(phrase in caption for phrase in PHRASES["management_wifi"])
    assert len(caption) <= 1024


def test_channel_caption_honors_manual_qiaolian_copy():
    caption = render_channel_caption(
        listing=_listing(),
        offer=_offer(),
        public_listing_id="QL-RF-A2B3",
        canonical_facts={
            "qiaolian_tags": ["pool_gym"],
            "qiaolian_say": ["物业和网络这些都安排好了。", "一周两次保洁，日常维护够用。"],
        },
    )

    assert "💬 侨联说" in caption
    assert "物业和网络这些都安排好了。" in caption
    assert "一周两次保洁，日常维护够用。" in caption


def test_channel_caption_honors_qiaolian_disable():
    caption = render_channel_caption(
        listing=_listing(),
        offer=_offer(),
        public_listing_id="QL-RF-A2B3",
        canonical_facts={
            "qiaolian_tags": ["management_wifi"],
            "qiaolian_say_disabled": True,
        },
    )

    assert "💬 侨联说" not in caption


def test_legacy_renderer_call_does_not_invent_qiaolian_copy_without_facts():
    caption = render_channel_caption(
        listing=_listing(),
        offer=_offer(),
        public_listing_id="QL-RF-A2B3",
    )

    assert "💬 侨联说" not in caption

from qiaolian_dual.talk_engine import generate_talk

def test_talk_uses_only_supported_location_building_and_value_facts():
    listing = {"area": "BKK1", "project": "ABC Residence", "property_type": "公寓", "size_sqm": 60, "floor": "12楼", "price": 800, "management_fee": "包含"}
    text = generate_talk(listing, max_points=3)
    assert "BKK1" in text and "ABC Residence" in text
    assert "公寓" in text and "60㎡" in text and "12楼" in text
    assert "$800" in text and "物业费已包含" in text

def test_talk_does_not_invent_marketing_claims_or_use_manual_copy():
    text = generate_talk({"qiaolian_talk": "核心位置、生活便利、视野开阔、性价比高、租金含物业"})
    assert text == ""

def test_unknown_costs_and_empty_listing_produce_no_talk():
    assert generate_talk({"management_fee": "待确认", "internet_fee": "未知"}) == ""

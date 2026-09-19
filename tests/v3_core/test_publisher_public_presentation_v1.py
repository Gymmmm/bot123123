from v3_core.publishing.public_presentation import public_listing_presentation


def test_direct_projects_use_registry_naming_and_geo():
    peak = public_listing_presentation({"project": "香格里拉"})
    assert peak["project_entity_id"] == "project:the-peak"
    assert peak["preferred_project_name_cn"]
    assert peak["canonical_project_name_en"] == "The Peak"
    assert "钻石岛" not in peak["canonical_geo"]

    bridge = public_listing_presentation({"project": "桥牌"})
    assert bridge["project_entity_id"] == "project:the-bridge"
    palm = public_listing_presentation({"project": "Palm Creek"})
    assert palm["project_entity_id"] == "project:chankiri-palm-creek"


def test_ambiguous_family_and_market_aliases_are_never_promoted():
    for value in ("60米炳发", "50米炳发", "摩根", "Chankiri", "BKK1雅居乐"):
        result = public_listing_presentation({"project": value, "area": "原始位置"})
        assert not result.get("project_entity_id")
        assert result["project"] == value
        assert result["area"] == "原始位置"
        assert result["project_resolution_ambiguity"] is True


def test_agile_direct_identity_keeps_registry_geo_not_bkk1_alias():
    result = public_listing_presentation({"project": "Agile Sky Residence", "area": "BKK1"})
    assert result["project_entity_id"] == "project:agile-sky-residence"
    assert result["canonical_geo"] != "BKK1"
    assert result["area"] != "BKK1"


def test_office_identity_is_exposed_without_becoming_residential():
    result = public_listing_presentation({"project": "摩根大厦"})
    assert result["project_entity_id"] == "project:morgan-tower"
    assert result["project_residential_eligible"] is False

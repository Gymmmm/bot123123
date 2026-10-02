from v3_core.presentation.location_display import display_location, display_project_location

def test_alias_stack_collapses_to_one_readable_location():
    assert display_location("一号路 / 60米 / 50米炳发", project="炳发城") == "60米大道"
    assert display_project_location(project="炳发城", location="一号路 / 60米 / 50米炳发") == "炳发城｜60米大道"

def test_explicit_near_relation_is_preserved_but_not_invented():
    assert display_location("60米 / 近永旺3") == "60米大道｜近永旺3"
    assert "近" not in display_location("一号路 / 60米")

def test_project_duplicate_is_removed_from_location():
    assert display_location("富力城 / BKK3", project="富力城") == "BKK3"
    assert display_project_location(project="富力城", location="富力城 / BKK3") == "富力城｜BKK3"

def test_single_area_stays_single():
    assert display_location("BKK1") == "BKK1"

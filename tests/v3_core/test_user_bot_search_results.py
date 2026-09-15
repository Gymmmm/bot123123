from v3_core.user_bot.search_results import augment_strict_single_result


def test_strict_single_result_appends_real_unique_similar_items_to_five():
    primary = [{"listing_id": "L1", "rank": "strict"}]
    similar = [
        {"listing_id": "L1", "rank": "duplicate"},
        {"listing_id": "L2"},
        {"listing_id": ""},
        {"listing_id": "L3"},
        {"listing_id": "L4"},
        {"listing_id": "L5"},
        {"listing_id": "L6"},
    ]

    result = augment_strict_single_result(primary, similar)

    assert [item["listing_id"] for item in result.items] == ["L1", "L2", "L3", "L4", "L5"]
    assert result.has_similar
    assert result.items[0]["rank"] == "strict"


def test_non_strict_mode_never_appends_similar_items():
    result = augment_strict_single_result(
        [{"listing_id": "L1"}],
        [{"listing_id": "L2"}],
        match_mode="fallback",
    )

    assert [item["listing_id"] for item in result.items] == ["L1"]
    assert not result.has_similar


def test_zero_or_multiple_strict_results_are_left_unchanged():
    empty = augment_strict_single_result([], [{"listing_id": "L2"}])
    multiple = augment_strict_single_result(
        [{"listing_id": "L1"}, {"listing_id": "L2"}],
        [{"listing_id": "L3"}],
    )

    assert empty.items == ()
    assert not empty.has_similar
    assert [item["listing_id"] for item in multiple.items] == ["L1", "L2"]
    assert not multiple.has_similar


def test_input_lists_are_not_mutated():
    matches = [{"listing_id": "L1"}]
    similar = [{"listing_id": "L2"}]

    augment_strict_single_result(matches, similar)

    assert matches == [{"listing_id": "L1"}]
    assert similar == [{"listing_id": "L2"}]

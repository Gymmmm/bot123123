from qiaolian_dual.listing import _resolve_area_from_target
from qiaolian_dual.location_mapping import AREA_SLUG_TABLE, resolve_area_slug
from qiaolian_dual.session_deeplink import parse_start_arg_payload


def test_locked_area_slugs_are_ascii_lowercase_unique_and_resolve():
    slugs = [slug for _display, slug, _key in AREA_SLUG_TABLE]
    assert len(slugs) == 11
    assert len(slugs) == len(set(slugs))
    assert all(slug.isascii() for slug in slugs)
    assert all(slug == slug.lower() for slug in slugs)
    for _display, slug, key in AREA_SLUG_TABLE:
        assert resolve_area_slug(slug) == key


def test_more_area_deeplinks_resolve_all_locked_slugs():
    for _display, slug, key in AREA_SLUG_TABLE:
        payload = parse_start_arg_payload(f"more_{slug}")
        assert payload is not None
        assert payload["action"] == "more"
        assert payload["target"] == slug
        assert _resolve_area_from_target(payload["target"]) == (key, "")


def test_locked_underscore_slugs_preserve_underscores():
    expected = {
        "koh_pich": "钻石岛",
        "gold_street": "金街",
        "chroy_changvar": "水净华",
        "russian_market": "俄罗斯市场",
    }
    for slug, key in expected.items():
        assert resolve_area_slug(slug) == key

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected exactly one match, got {count}: {old[:120]!r}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


package = ROOT / "publication_package.py"
cover_tests = ROOT / "tests" / "test_cover_consolidation.py"
prelaunch_tests = ROOT / "tests" / "test_prelaunch_product_contract.py"
launch_tests = ROOT / "tests" / "test_launch_readiness_contract.py"

replace_once(
    package,
    '''def classify(*, source_type: str, source_name: str, property_type: str,\n             project: str, media_type: str = "image", price: Any = None,\n             highlights: Any = None, is_special: bool = False) -> dict[str, str]:\n    """Return stable listing metadata and one deterministic default cover.\n\n    Still-image listings always start with the classic blue card. Operators can\n    explicitly choose right-price or black-gold before approval. This removes\n    the old price/source/highlight routing that made previews unpredictable.\n    """\n    media = str(media_type or "image").lower()\n    source = f"{source_type or ''} {source_name or ''}".lower()\n    listing = f"{property_type or ''} {project or ''}".lower()\n\n    normalized_source = "wechat" if ("wechat" in source or "微信" in source) else "telegram"\n\n    if "video" in media:\n        return {\n            "source_type": normalized_source,\n            "listing_type": "video",\n            "media_type": "video",\n            "cover_template": "video_vertical",\n        }\n\n    is_villa = "别墅" in listing or "villa" in listing\n    is_townhouse = any(token in listing for token in ("排屋", "联排", "townhouse"))\n    listing_type = "villa" if is_villa else ("townhouse" if is_townhouse else "apartment")\n\n    _ = (price, highlights, is_special)\n    return {\n        "source_type": normalized_source,\n        "listing_type": listing_type,\n        "media_type": "image",\n        "cover_template": "classic_blue",\n    }\n''',
    '''def classify(*, source_type: str, source_name: str, property_type: str,\n             project: str, media_type: str = "image", price: Any = None,\n             highlights: Any = None, is_special: bool = False) -> dict[str, str]:\n    """Return stable listing metadata and the launch-default cover.\n\n    Launch contract:\n      - ordinary still listings -> classic blue\n      - villa / townhouse / monthly rent >= 1200 -> black gold\n      - right-price remains operator-selectable only; it is never auto-routed\n      - video keeps the dedicated vertical template\n\n    Property classification never changes just because the visual template does.\n    """\n    media = str(media_type or "image").lower()\n    source = f"{source_type or ''} {source_name or ''}".lower()\n    listing = f"{property_type or ''} {project or ''}".lower()\n\n    normalized_source = "wechat" if ("wechat" in source or "微信" in source) else "telegram"\n\n    if "video" in media:\n        return {\n            "source_type": normalized_source,\n            "listing_type": "video",\n            "media_type": "video",\n            "cover_template": "video_vertical",\n        }\n\n    is_villa = "别墅" in listing or "villa" in listing\n    is_townhouse = any(token in listing for token in ("排屋", "联排", "townhouse"))\n    listing_type = "villa" if is_villa else ("townhouse" if is_townhouse else "apartment")\n    try:\n        numeric_price = float(re.sub(r"[^0-9.]", "", str(price or "0")) or 0)\n    except (TypeError, ValueError):\n        numeric_price = 0\n\n    # `highlights` and `is_special` remain accepted for compatibility, but they\n    # do not silently switch the visual style. Right-price is manual-only.\n    _ = (highlights, is_special)\n    cover_template = "black_gold" if (is_villa or is_townhouse or numeric_price >= 1200) else "classic_blue"\n    return {\n        "source_type": normalized_source,\n        "listing_type": listing_type,\n        "media_type": "image",\n        "cover_template": cover_template,\n    }\n''',
)

replace_once(
    cover_tests,
    '''def test_still_listings_have_one_deterministic_default():\n    for property_type, price in (("公寓", 600), ("排屋", 1200), ("别墅", 5000)):\n        routed = classify(\n            source_type="telegram",\n            source_name="collector",\n            property_type=property_type,\n            project="示例房源",\n            price=price,\n        )\n        assert routed["cover_template"] == "classic_blue"\n''',
    '''def test_launch_default_cover_routing_is_deterministic():\n    cases = (\n        ("公寓", 600, "classic_blue"),\n        ("公寓", 1199, "classic_blue"),\n        ("公寓", 1200, "black_gold"),\n        ("排屋", 600, "black_gold"),\n        ("别墅", 5000, "black_gold"),\n    )\n    for property_type, price, expected in cases:\n        routed = classify(\n            source_type="telegram",\n            source_name="collector",\n            property_type=property_type,\n            project="示例房源",\n            price=price,\n        )\n        assert routed["cover_template"] == expected\n\n\ndef test_right_price_is_manual_only():\n    routed = classify(\n        source_type="telegram",\n        source_name="collector",\n        property_type="公寓",\n        project="特价房源",\n        price=600,\n        is_special=True,\n    )\n    assert routed["cover_template"] == "classic_blue"\n    assert normalize_cover_style("right_price") == "right_price"\n''',
)

replace_once(
    prelaunch_tests,
    "    assert routed['cover_template'] == 'classic_blue'\n",
    "    assert routed['cover_template'] == 'black_gold'\n",
)

launch_tests.write_text(
    '''from pathlib import Path\n\nfrom publication_package import classify\nfrom qiaolian_dual.canonical_facts import PARSER_REVISION\nfrom qiaolian_dual.session_deeplink import parse_start_arg_payload\nfrom v2.qiaolian_publisher_v2.keyboards import publish_post_keyboard\n\n\ndef test_parser_revision_is_launch_v12():\n    assert PARSER_REVISION == "v1.2"\n\n\ndef test_channel_detail_payload_reaches_details_route():\n    payload = parse_start_arg_payload("detail__abc123__l_1047")\n    assert payload is not None\n    assert payload["action"] == "details"\n    assert payload["target"] == "l_1047"\n\n\ndef test_channel_post_has_exactly_three_launch_actions():\n    keyboard = publish_post_keyboard("l_1047", "BKK1", "QiaolianBot", post_token="abc123")\n    buttons = [button for row in keyboard.inline_keyboard for button in row]\n    assert [button.text for button in buttons] == ["📋 租赁详情", "📸 更多实拍", "📅 预约看房"]\n    assert all(button.url for button in buttons)\n\n\ndef test_launch_cover_defaults_match_final_product_contract():\n    assert classify(source_type="telegram", source_name="collector", property_type="公寓", project="", price=800)["cover_template"] == "classic_blue"\n    assert classify(source_type="telegram", source_name="collector", property_type="公寓", project="", price=1200)["cover_template"] == "black_gold"\n    assert classify(source_type="telegram", source_name="collector", property_type="排屋", project="", price=700)["cover_template"] == "black_gold"\n    assert classify(source_type="telegram", source_name="collector", property_type="别墅", project="", price=700)["cover_template"] == "black_gold"\n\n\ndef test_production_deploy_does_not_shadow_ssh_user_with_empty_secret():\n    workflow = Path(".github/workflows/qiaolian-production-deploy.yml").read_text(encoding="utf-8")\n    assert "PROD_SSH_USER: root" in workflow\n    assert "PROD_SSH_USER: '${{ secrets.PROD_SSH_USER }}'" not in workflow\n''',
    encoding="utf-8",
)

print("final launch closeout patch applied")

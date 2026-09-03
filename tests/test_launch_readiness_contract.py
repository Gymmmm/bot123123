from pathlib import Path

from publication_package import classify
from qiaolian_dual.canonical_facts import PARSER_REVISION
from qiaolian_dual.session_deeplink import parse_start_arg_payload
from v2.qiaolian_publisher_v2.keyboards import publish_post_keyboard


def test_parser_revision_is_launch_v12():
    assert PARSER_REVISION == "v1.2"


def test_channel_detail_payload_reaches_details_route():
    payload = parse_start_arg_payload("detail__abc123__l_1047")
    assert payload is not None
    assert payload["action"] == "details"
    assert payload["target"] == "l_1047"


def test_channel_post_has_exactly_three_launch_actions():
    keyboard = publish_post_keyboard("l_1047", "BKK1", "QiaolianBot", post_token="abc123")
    buttons = [button for row in keyboard.inline_keyboard for button in row]
    assert [button.text for button in buttons] == ["📋 租赁详情", "📸 更多实拍", "📅 预约看房"]
    assert all(button.url for button in buttons)


def test_launch_cover_defaults_match_final_product_contract():
    assert classify(source_type="telegram", source_name="collector", property_type="公寓", project="", price=800)["cover_template"] == "classic_blue"
    assert classify(source_type="telegram", source_name="collector", property_type="公寓", project="", price=1200)["cover_template"] == "black_gold"
    assert classify(source_type="telegram", source_name="collector", property_type="排屋", project="", price=700)["cover_template"] == "black_gold"
    assert classify(source_type="telegram", source_name="collector", property_type="别墅", project="", price=700)["cover_template"] == "black_gold"

def test_cover_source_remains_in_complete_gallery():
    source = Path("publication_package.py").read_text(encoding="utf-8")
    assert "detail_sources = list(originals)" in source
    assert "path for path in originals if path != cover_source" not in source


def test_under_four_photos_are_retained_as_insufficient_media():
    source = Path("collector_bot.py").read_text(encoding="utf-8")
    assert 'parse_status="insufficient_media" if insufficient_media else "pending"' in source
    assert '"media_status": "insufficient_media" if insufficient_media else "sufficient_media"' in source
    assert 'return {"status": "skipped", "reason": "fewer_than_min_images"}' not in source


def test_deploy_gate_covers_current_launch_suite_and_browser_runtime():
    workflow = Path(".github/workflows/qiaolian-production-deploy.yml").read_text(encoding="utf-8")
    for required in (
        "tests/test_launch_readiness_contract.py",
        "tests/test_parser_v12_regressions.py",
        "tests/test_parser_v2_safe.py",
        "tests/test_media_pipeline_v11.py",
        "PLAYWRIGHT_BROWSERS_PATH",
        "playwright install chromium",
    ):
        assert required in workflow
    assert "PROD_SSH_USER: '${{ secrets.PROD_SSH_USER }}'" not in workflow

import argparse
import asyncio
from pathlib import Path

from html_cover_renderer import _canonical_cover_fields
from tools import publish_houses_csv


def test_cover_renderer_prefers_canonical_facts_over_polluted_legacy_fields():
    fields = _canonical_cover_fields(
        {
            "project": "联排别墅",
            "area": "金边",
            "price": "$999/月",
            "canonical_facts": {
                "project_name": "Urban Village",
                "project_alias": "城市社区",
                "property_type_display": "公寓",
                "deal_type": "rent",
                "layout": "2房1厅",
                "public_location_display": "洪森大道",
                "size_sqm": 82,
                "floor": "12楼",
                "monthly_rent_usd": 680,
            },
        }
    )
    assert fields["project"] == "Urban Village"
    assert fields["property_type"] == "公寓"
    assert fields["area"] == "洪森大道"
    assert fields["price"] == "$680"
    assert fields["price_line"] == "$680/月"
    assert fields["size"] == "82㎡"


def test_sale_cover_never_gets_month_suffix():
    fields = _canonical_cover_fields(
        {
            "price": "$120000/月",
            "canonical_facts": {"deal_type": "sale", "sale_price_usd": 120000},
        }
    )
    assert fields["price"] == "$120000"
    assert fields["price_line"] == "$120000"
    assert fields["price_suffix"] == ""


def test_csv_direct_send_is_fail_closed_before_bot_creation(tmp_path: Path):
    args = argparse.Namespace(
        bot_token="fake",
        channel_id="-1001",
        dry_run=False,
        prepare_only=False,
    )
    assert asyncio.run(publish_houses_csv._run(args)) == 4


def test_batch_query_requires_both_approved_states():
    source = (Path(__file__).parents[1] / "scripts" / "publish_ready_batch.py").read_text(
        encoding="utf-8"
    )
    assert "p.status='approved'" in source
    assert "d.review_status='approved'" in source


def test_legacy_direct_senders_are_explicitly_blocked():
    root = Path(__file__).parents[1]
    csv_pipeline = (root / "scripts" / "houses_csv_pipeline.py").read_text(encoding="utf-8")
    zufang = (root / "tools" / "publish_zufang555.py").read_text(encoding="utf-8")
    samples = (root / "tools" / "post_three_caption_samples.py").read_text(encoding="utf-8")
    assert "send-next cannot bypass" in csv_pipeline
    assert "legacy zufang555 direct publishing cannot bypass" in zufang
    assert "WHERE p.status = 'approved'" in samples
    assert "p.post_text" in samples

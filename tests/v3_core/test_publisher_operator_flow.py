from __future__ import annotations

import json

import pytest

from v3_core.ingest.source_registry import AdminSourceRegistry, normalize_entity
from v3_core.publishing.operator_flow import OperatorPublisherAdminController


def _labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def test_operator_home_is_task_oriented_and_hides_pipeline_internals():
    labels = _labels(OperatorPublisherAdminController.home_keyboard())
    assert labels == [
        "➕ 发布房源",
        "📢 广播中心",
        "🔵 房态管理",
        "📡 采集源",
        "📚 发布记录",
    ]
    assert not any("异常" in label for label in labels)
    assert not any("运行状态" in label for label in labels)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("@example_channel", "example_channel"),
        ("https://t.me/example_channel", "example_channel"),
        ("t.me/example_channel?single", "example_channel"),
        ("-1001234567890", "-1001234567890"),
    ],
)
def test_normalize_entity(raw, expected):
    assert normalize_entity(raw) == expected


def test_admin_source_overlay_add_remove_does_not_rewrite_base(tmp_path):
    base = tmp_path / "sources.json"
    original = [
        {
            "source_name": "base_source",
            "entity_id": "base_source",
            "source_type": "telegram_channel",
        }
    ]
    base.write_text(json.dumps(original), encoding="utf-8")
    db = tmp_path / "data" / "qiaolian.db"
    db.parent.mkdir()
    registry = AdminSourceRegistry(base_path=base, db_path=db)

    registry.add("@new_source")
    names = {row["source_name"] for row in registry.rows()}
    assert names == {"base_source", "new_source"}
    assert json.loads(base.read_text(encoding="utf-8")) == original

    registry.remove("base_source")
    names = {row["source_name"] for row in registry.rows()}
    assert names == {"new_source"}
    assert json.loads(base.read_text(encoding="utf-8")) == original


def test_admin_source_overlay_is_stored_beside_shared_db(tmp_path):
    base = tmp_path / "sources.json"
    base.write_text("[]", encoding="utf-8")
    db = tmp_path / "runtime" / "qiaolian.db"
    db.parent.mkdir()
    registry = AdminSourceRegistry(base_path=base, db_path=db)
    registry.add("@new_source")
    assert registry.overlay_path == db.parent / "collector_sources_admin.json"
    assert registry.overlay_path.is_file()

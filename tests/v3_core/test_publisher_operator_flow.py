from __future__ import annotations

import json

import pytest

from v3_core.ingest.source_registry import AdminSourceRegistry, normalize_entity
from v3_core.publishing.operator_flow import OperatorPublisherAdminController


def _labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def test_operator_home_keyboard_keeps_task_oriented_layout():
    """OperatorPublisherAdminController.home_keyboard stays task-oriented.

    The rendered home comes from PublisherInventoryAdminController in
    practice, but the operator-level keyboard still has to keep the same
    spirit: pipeline-internals (审核事实, 冻结, 待发送) must stay hidden.
    """
    labels = _labels(OperatorPublisherAdminController.home_keyboard())
    assert labels, "operator home keyboard should not be empty"
    for forbidden in ("异常", "运行状态", "审核事实", "冻结", "待发送"):
        assert not any(forbidden in label for label in labels), (
            f"{forbidden} should stay hidden from the operator"
        )
    # Pipeline entry points remain: 新建房源 / 频道运营 / 房态管理.
    for required in ("➕", "📢", "🔵"):
        assert any(label.startswith(required) for label in labels), (
            f"{required} entry missing from operator home"
        )


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

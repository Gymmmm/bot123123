from __future__ import annotations

import json
from pathlib import Path
import sqlite3

from v3_core.readiness import run_preflight
from v3_core.storage.bootstrap import REQUIRED_V3_TABLES, existing_tables, initialize_v3_storage


REPO_ROOT = Path(__file__).resolve().parents[2]


def _check(report, name: str):
    return next(item for item in report.checks if item.name == name)


def test_read_only_preflight_does_not_create_missing_database(tmp_path):
    db = tmp_path / "missing.db"
    assert not db.exists()

    report = run_preflight(repo_root=REPO_ROOT, db_path=db, component="storage", env={})

    assert not report.ok
    assert not db.exists()
    assert not _check(report, "storage:database").ok
    assert not _check(report, "storage:v3_tables").ok


def test_initialize_is_additive_and_preserves_existing_rows(tmp_path):
    db = tmp_path / "existing.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE legacy_keep (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute("INSERT INTO legacy_keep(value) VALUES ('do-not-touch')")
        conn.commit()

    initialize_v3_storage(db)

    assert REQUIRED_V3_TABLES.issubset(existing_tables(db))
    with sqlite3.connect(db) as conn:
        row = conn.execute("SELECT id,value FROM legacy_keep").fetchone()
    assert row == (1, "do-not-touch")


def test_storage_preflight_passes_after_explicit_initialize(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)

    report = run_preflight(repo_root=REPO_ROOT, db_path=db, component="storage", env={})

    assert report.ok, report.as_dict()
    assert _check(report, "storage:v3_tables").detail == "complete"


def test_collector_preflight_validates_enabled_source_without_starting_telegram(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    sources = tmp_path / "sources.json"
    sources.write_text(
        json.dumps(
            [
                {
                    "source_name": "contract-test",
                    "entity_id": "123456789",
                    "is_enabled": True,
                }
            ]
        ),
        encoding="utf-8",
    )
    env = {
        "TG_API_ID": "12345",
        "TG_API_HASH": "contract-test-hash",
        "COLLECTOR_SOURCES_JSON": str(sources),
    }

    report = run_preflight(
        repo_root=REPO_ROOT,
        db_path=db,
        component="collector",
        env=env,
    )

    assert report.ok, report.as_dict()
    assert _check(report, "collector:sources").detail == "enabled_sources:1"


def test_publisher_preflight_requires_discussion_disabled(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    env = {
        "PUBLISHER_BOT_TOKEN": "contract-token",
        "ADMIN_IDS": "1001,1002",
        "CHANNEL_ID": "-1001234567890",
        "DEEPLINK_BOT_USERNAME": "qiaolian_test_bot",
        "V3_COVER_OUTPUT_DIR": str(tmp_path / "covers"),
        "CHANNEL_DISCUSSION_ENABLED": "true",
    }

    blocked = run_preflight(
        repo_root=REPO_ROOT,
        db_path=db,
        component="publisher",
        env=env,
    )
    assert not blocked.ok
    assert not _check(blocked, "publisher:discussion_default_off").ok

    env["CHANNEL_DISCUSSION_ENABLED"] = "false"
    ready = run_preflight(
        repo_root=REPO_ROOT,
        db_path=db,
        component="publisher",
        env=env,
    )
    assert ready.ok, ready.as_dict()


def test_all_formal_cover_templates_are_preflighted(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    env = {
        "PUBLISHER_BOT_TOKEN": "contract-token",
        "ADMIN_IDS": "1001",
        "CHANNEL_ID": "-1001234567890",
        "USER_BOT_USERNAME": "qiaolian_test_bot",
        "V3_COVER_OUTPUT_DIR": str(tmp_path / "covers"),
    }

    report = run_preflight(
        repo_root=REPO_ROOT,
        db_path=db,
        component="publisher",
        env=env,
    )

    names = {check.name for check in report.checks}
    assert {
        "template:classic_blue",
        "template:right_price",
        "template:black_gold",
        "template:video_vertical",
    }.issubset(names)

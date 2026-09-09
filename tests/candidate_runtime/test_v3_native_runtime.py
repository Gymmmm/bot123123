from __future__ import annotations

import os
import sqlite3

import pytest

from candidate_runtime.bootstrap import database_integrity, initialize_runtime_storage, missing_runtime_tables
from candidate_runtime.runtime_env import configure_candidate_environment


def test_empty_db_recovery_uses_native_v3_tables(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime"
    monkeypatch.setenv("QIAOLIAN_RUNTIME_ROOT", str(runtime))
    monkeypatch.setenv("DB_PATH", "data/candidate.db")
    paths = configure_candidate_environment(tmp_path / "repo")
    initialize_runtime_storage(paths.db_path, backup_dir=paths.backup_dir)
    assert not missing_runtime_tables(paths.db_path)
    ok, detail = database_integrity(paths.db_path)
    assert ok and detail == "ok"
    with sqlite3.connect(paths.db_path) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"listings_v3", "listing_offers", "publication_instances", "rental_cases_v3", "rental_case_events_v3"} <= tables
    assert "drafts" not in tables
    assert "listings" not in tables
    assert "posts" not in tables


def test_runtime_rejects_mutable_path_outside_root(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime"
    monkeypatch.setenv("QIAOLIAN_RUNTIME_ROOT", str(runtime))
    monkeypatch.setenv("DB_PATH", str(tmp_path / "outside.db"))
    with pytest.raises(RuntimeError, match="DB_PATH escapes candidate runtime root"):
        configure_candidate_environment(tmp_path / "repo")


def test_existing_db_is_backed_up_before_additive_initialize(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime"
    monkeypatch.setenv("QIAOLIAN_RUNTIME_ROOT", str(runtime))
    monkeypatch.setenv("DB_PATH", "data/candidate.db")
    paths = configure_candidate_environment(tmp_path / "repo")
    paths.db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(paths.db_path) as conn:
        conn.execute("CREATE TABLE sentinel(value TEXT)")
        conn.execute("INSERT INTO sentinel VALUES ('keep')")
    initialize_runtime_storage(paths.db_path, backup_dir=paths.backup_dir)
    backups = list(paths.backup_dir.glob("candidate-*.sqlite3"))
    assert backups
    with sqlite3.connect(paths.db_path) as conn:
        assert conn.execute("SELECT value FROM sentinel").fetchone()[0] == "keep"

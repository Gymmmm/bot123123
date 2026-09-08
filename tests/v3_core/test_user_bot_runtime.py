from __future__ import annotations

import pytest

from v3_core.user_bot.runtime import build_read_runtime


def test_runtime_factory_wires_one_public_service_graph_without_creating_db(tmp_path):
    db = tmp_path / "missing-v3.db"

    runtime = build_read_runtime(db)

    assert runtime.db_path == db.resolve()
    assert not db.exists()
    assert runtime.routes.inventory is runtime.inventory
    assert runtime.listings.routes is runtime.routes
    assert runtime.search.search.reader is runtime.search_reader
    assert runtime.search_flow.search is runtime.search
    assert runtime.search_sessions.inventory is runtime.inventory
    assert runtime.consults.inventory is runtime.inventory
    assert runtime.similars.inventory is runtime.inventory
    assert runtime.callbacks.listings is runtime.listings
    assert runtime.callbacks.search_sessions is runtime.search_sessions
    assert runtime.callbacks.consults is runtime.consults
    assert runtime.callbacks.similars is runtime.similars


def test_factory_does_not_hide_missing_database_by_initializing_schema(tmp_path):
    db = tmp_path / "missing-v3.db"
    runtime = build_read_runtime(db)

    with pytest.raises(FileNotFoundError):
        runtime.inventory.resolve("QL-RF-A2B3")
    with pytest.raises(FileNotFoundError):
        runtime.search_reader.search(limit=1)

    assert not db.exists()

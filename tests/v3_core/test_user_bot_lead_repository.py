from __future__ import annotations

import sqlite3

import pytest

from v3_core.storage.bootstrap import REQUIRED_V3_TABLES, initialize_v3_storage
from v3_core.storage.lead_repository import SQLiteLeadRepository


def test_lead_repository_constructor_does_not_create_database(tmp_path):
    db = tmp_path / "missing.db"

    SQLiteLeadRepository(db)

    assert not db.exists()


def test_explicit_bootstrap_creates_leads_v3_without_touching_legacy_leads(tmp_path):
    db = tmp_path / "v3.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE leads (id INTEGER PRIMARY KEY, marker TEXT NOT NULL)")
        conn.execute("INSERT INTO leads (id,marker) VALUES (1,'legacy-keep')")
        conn.commit()

    initialize_v3_storage(db)

    with sqlite3.connect(db) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        legacy = conn.execute("SELECT marker FROM leads WHERE id=1").fetchone()

    assert "leads_v3" in tables
    assert "leads_v3" in REQUIRED_V3_TABLES
    assert legacy == ("legacy-keep",)


def test_create_round_trips_locked_production_lead_fields(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    repo = SQLiteLeadRepository(db)

    lead_id = repo.create(
        {
            "user_id": 123,
            "username": "alice",
            "display_name": "Alice",
            "source": "user_search",
            "action": "search_pref_submit",
            "listing_id": "",
            "area": "BKK1",
            "property_type": "公寓",
            "budget_min": 600,
            "budget_max": 900,
            "payload": {"goal": "any", "area_hint": "BKK1"},
            "message_id": 777,
            "post_token": "token-1",
            "caption_variant": "a",
            "agent_id": "agent-1",
            "response_at": "",
            "conversion_value": 0,
            "advisor_id": "",
            "advisor_name": "",
            "assigned_at": "",
            "intent": "rent",
            "lead_status": "new",
            "created_at": "2026-09-09 02:00:00",
        }
    )

    row = repo.get(lead_id)
    assert row is not None
    assert row["user_id"] == 123
    assert row["source"] == "user_search"
    assert row["action"] == "search_pref_submit"
    assert row["area"] == "BKK1"
    assert row["property_type"] == "公寓"
    assert row["budget_min"] == 600
    assert row["budget_max"] == 900
    assert row["payload"] == {"area_hint": "BKK1", "goal": "any"}
    assert row["message_id"] == 777
    assert row["lead_status"] == "new"


def test_lead_create_requires_real_user_action_and_timestamp(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    repo = SQLiteLeadRepository(db)

    with pytest.raises(ValueError, match="lead_user_id_required"):
        repo.create({"action": "x", "created_at": "2026-09-09 02:00:00"})
    with pytest.raises(ValueError, match="lead_action_required"):
        repo.create({"user_id": 123, "created_at": "2026-09-09 02:00:00"})
    with pytest.raises(ValueError, match="lead_created_at_required"):
        repo.create({"user_id": 123, "action": "x"})

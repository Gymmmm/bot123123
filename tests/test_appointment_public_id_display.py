"""Public appointment UI must never show internal listing ids."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from qiaolian_dual import appointments_view


INTERNAL_ID = "l_9001"
PUBLIC_ID = "QL-PP-T9K2"


def _seed_listings_v3(db_path: Path, *, listing_id: str | None = None, public_id: str | None = None) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS listings_v3 (
                listing_id TEXT PRIMARY KEY,
                public_listing_id TEXT,
                display_title TEXT,
                inventory_status TEXT DEFAULT 'active'
            )"""
        )
        if listing_id and public_id:
            conn.execute(
                "INSERT OR REPLACE INTO listings_v3(listing_id, public_listing_id, display_title) VALUES (?, ?, ?)",
                (listing_id, public_id, "测试房源"),
            )
        conn.commit()


def test_appointment_public_id_maps_internal_to_ql(tmp_path, monkeypatch):
    db_path = tmp_path / "appt.db"
    _seed_listings_v3(db_path, listing_id=INTERNAL_ID, public_id=PUBLIC_ID)
    monkeypatch.setattr(appointments_view, "DB_PATH", str(db_path))

    assert appointments_view._appointment_public_listing_id(INTERNAL_ID) == PUBLIC_ID
    assert appointments_view._appointment_listing_compact(INTERNAL_ID) == PUBLIC_ID
    assert appointments_view._appointment_listing_compact(PUBLIC_ID) == PUBLIC_ID


def test_appointment_public_id_mapping_miss_shows_advisor_pending(tmp_path, monkeypatch):
    db_path = tmp_path / "appt_miss.db"
    _seed_listings_v3(db_path)
    monkeypatch.setattr(appointments_view, "DB_PATH", str(db_path))

    assert appointments_view._appointment_public_listing_id("l_404") == ""
    assert appointments_view._appointment_listing_compact("l_404") == "待顾问匹配"
    assert "l_404" not in appointments_view._appointment_listing_compact("l_404")


def test_appointment_public_id_db_error_shows_advisor_pending(tmp_path, monkeypatch):
    missing = tmp_path / "no-such" / "missing.db"
    monkeypatch.setattr(appointments_view, "DB_PATH", str(missing))

    assert appointments_view._appointment_public_listing_id(INTERNAL_ID) == ""
    assert appointments_view._appointment_listing_compact(INTERNAL_ID) == "待顾问匹配"


def test_appointment_summary_line_never_leaks_internal_id(tmp_path, monkeypatch):
    db_path = tmp_path / "appt_summary.db"
    _seed_listings_v3(db_path)
    monkeypatch.setattr(appointments_view, "DB_PATH", str(db_path))

    line = "\n".join(
        appointments_view._appointment_summary_line(
            {
                "status": "pending",
                "viewing_mode": "onsite",
                "appointment_date": "2026-10-01",
                "appointment_time": "10:00-12:00",
                "listing_id": "l_777",
            }
        )
    )
    assert "待顾问匹配" in line
    assert "l_777" not in line
    assert "L_777" not in line

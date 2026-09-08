"""Read-only V3 inventory queries used by publication services."""
from __future__ import annotations

import json
import sqlite3
from typing import Any

from .schema import ensure_v3_schema


class InventoryReader:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        with self._connect() as conn:
            ensure_v3_schema(conn)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def listing(self, listing_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM listings_v3 WHERE listing_id=?", (str(listing_id),)
            ).fetchone()
        if row is None:
            raise KeyError(listing_id)
        return dict(row)

    def offer(self, offer_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM listing_offers WHERE offer_id=?", (str(offer_id),)
            ).fetchone()
        if row is None:
            raise KeyError(offer_id)
        return dict(row)

    def canonical(self, canonical_record_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM canonical_records WHERE canonical_record_id=?",
                (str(canonical_record_id),),
            ).fetchone()
        if row is None:
            raise KeyError(canonical_record_id)
        data = dict(row)
        facts = json.loads(str(data["facts_json"]))
        if not isinstance(facts, dict):
            raise ValueError("canonical facts payload is not an object")
        data["facts"] = facts
        return data

    def canonical_for_listing(self, listing_id: str) -> dict[str, Any]:
        listing = self.listing(listing_id)
        return self.canonical(str(listing["canonical_record_id"]))

    def review_approved(self, *, offer_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT 1 FROM review_items
                   WHERE offer_id=? AND review_status='approved'
                   ORDER BY approved_at DESC LIMIT 1""",
                (str(offer_id),),
            ).fetchone()
        return row is not None


__all__ = ["InventoryReader"]

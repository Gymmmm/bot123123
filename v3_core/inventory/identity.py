"""V3 listing/public identity allocation.

Preserves the production protocols `l_<number>` and
`QL-<location>-<letter><digit><letter><digit>` without reading drafts.  New
numeric IDs continue after both legacy `listings` and V3 `listings_v3` so the
side-by-side migration cannot collide with production inventory.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
import secrets
import sqlite3
from typing import Any


LETTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ"
DIGITS = "23456789"

LOCATION_CODES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("RF", ("富力城", "富力中心城", "r&f city", "rf city", "金边中心城")),
    ("BK", ("bkk1", "bkk 1", "万景岗1", "万景岗一区")),
    ("B2", ("bkk2", "bkk 2", "万景岗2", "万景岗二区")),
    ("B3", ("bkk3", "bkk 3", "万景岗3", "万景岗三区")),
    ("DD", ("钻石岛", "钻岛", "koh pich", "diamond island")),
    ("AE", ("永旺1", "永旺一", "aeon1", "aeon 1")),
    ("A2", ("永旺2", "永旺二", "aeon2", "aeon 2")),
    ("TK", ("tk/7月区", "tk", "7月区", "tuol kork")),
    ("SS", ("森速", "sen sok")),
    ("HS", ("洪森大道", "hun sen")),
    ("CH", ("水净华", "chroy changvar")),
)

DDL = """
CREATE TABLE IF NOT EXISTS listing_identity_reservations_v3 (
    listing_id TEXT PRIMARY KEY,
    public_id TEXT NOT NULL UNIQUE,
    canonical_record_id TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


@dataclass(frozen=True)
class ListingIdentity:
    listing_id: str
    public_listing_id: str


def _numeric_id(value: Any) -> int:
    match = re.fullmatch(r"(?i)l_(\d+)", str(value or "").strip())
    return int(match.group(1)) if match else 0


def _location_code(facts: dict[str, Any]) -> str:
    haystack = " ".join(
        str(facts.get(key) or "")
        for key in (
            "project_name",
            "project_alias",
            "project_brand",
            "public_location_key",
            "public_location_display",
            "canonical_area_key",
            "canonical_area_display",
        )
    ).lower()
    for code, aliases in LOCATION_CODES:
        if any(alias.lower() in haystack for alias in aliases):
            return code
    return "PP"


def _random_code() -> str:
    return "".join(
        (
            secrets.choice(LETTERS),
            secrets.choice(DIGITS),
            secrets.choice(LETTERS),
            secrets.choice(DIGITS),
        )
    )


class IdentityService:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        with self._connect() as conn:
            conn.executescript(DDL)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
        return conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone() is not None

    def _max_listing_number(self, conn: sqlite3.Connection) -> int:
        maximum = 0
        for table in (
            "listing_identity_reservations_v3",
            "listings_v3",
            "listings",
        ):
            if not self._table_exists(conn, table):
                continue
            try:
                rows = conn.execute(f"SELECT listing_id FROM {table}").fetchall()
            except sqlite3.Error:
                continue
            for row in rows:
                maximum = max(maximum, _numeric_id(row[0]))
        return maximum

    def allocate(
        self,
        *,
        canonical_record_id: str,
        facts: dict[str, Any],
    ) -> ListingIdentity:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                """SELECT listing_id,public_id FROM listing_identity_reservations_v3
                   WHERE canonical_record_id=? ORDER BY created_at LIMIT 1""",
                (str(canonical_record_id),),
            ).fetchone()
            if existing:
                conn.commit()
                return ListingIdentity(str(existing["listing_id"]), str(existing["public_id"]))

            number = self._max_listing_number(conn) + 1
            listing_id = f"l_{number}"
            location = _location_code(facts)
            for _ in range(128):
                public_id = f"QL-{location}-{_random_code()}"
                try:
                    conn.execute(
                        """INSERT INTO listing_identity_reservations_v3
                           (listing_id,public_id,canonical_record_id)
                           VALUES (?,?,?)""",
                        (listing_id, public_id, str(canonical_record_id)),
                    )
                    conn.commit()
                    return ListingIdentity(listing_id, public_id)
                except sqlite3.IntegrityError:
                    if conn.execute(
                        "SELECT 1 FROM listing_identity_reservations_v3 WHERE listing_id=?",
                        (listing_id,),
                    ).fetchone():
                        number += 1
                        listing_id = f"l_{number}"
                    continue
            raise RuntimeError("listing_identity_collision_limit")


__all__ = ["IdentityService", "ListingIdentity", "LOCATION_CODES"]

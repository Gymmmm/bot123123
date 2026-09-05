"""Stable, non-sequential public listing identifiers with legacy lookup support."""
from __future__ import annotations

import re
import secrets
import sqlite3
from pathlib import Path

from .config import DB_PATH

ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
PUBLIC_RE = re.compile(r"^QL-[A-HJ-NP-Z2-9]{6}$", re.I)
LEGACY_RE = re.compile(r"^(?:QC|QJ)[_-]?(\d+)$", re.I)
INTERNAL_RE = re.compile(r"^L[_-]?(\d+)$", re.I)


def _connect(db_path: str | Path | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path or DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE IF NOT EXISTS listing_public_ids (
        listing_id TEXT PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""")
    return conn


def legacy_to_internal(value: object) -> str:
    raw = str(value or "").strip()
    match = LEGACY_RE.fullmatch(raw) or INTERNAL_RE.fullmatch(raw)
    return f"l_{int(match.group(1))}" if match else raw


def public_listing_id(listing_id: object, *, db_path: str | Path | None = None) -> str:
    raw = str(listing_id or "").strip()
    if not raw:
        return ""
    if PUBLIC_RE.fullmatch(raw):
        return raw.upper()
    internal = legacy_to_internal(raw)
    if not INTERNAL_RE.fullmatch(internal):
        return raw.upper()
    with _connect(db_path) as conn:
        row = conn.execute("SELECT public_id FROM listing_public_ids WHERE listing_id=?", (internal,)).fetchone()
        if row:
            return str(row[0])
        for _ in range(64):
            candidate = "QL-" + "".join(secrets.choice(ALPHABET) for _ in range(6))
            try:
                conn.execute("INSERT INTO listing_public_ids(listing_id, public_id) VALUES (?, ?)", (internal, candidate))
                conn.commit()
                return candidate
            except sqlite3.IntegrityError:
                row = conn.execute("SELECT public_id FROM listing_public_ids WHERE listing_id=?", (internal,)).fetchone()
                if row:
                    return str(row[0])
        raise RuntimeError("public_listing_id_collision_limit")


def resolve_listing_id(value: object, *, db_path: str | Path | None = None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    legacy = legacy_to_internal(raw)
    if legacy != raw or INTERNAL_RE.fullmatch(raw):
        return legacy
    if PUBLIC_RE.fullmatch(raw):
        with _connect(db_path) as conn:
            row = conn.execute("SELECT listing_id FROM listing_public_ids WHERE public_id=?", (raw.upper(),)).fetchone()
        return str(row[0]) if row else ""
    return raw

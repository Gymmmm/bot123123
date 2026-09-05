"""Stable location-aware public listing IDs with legacy lookup support."""
from __future__ import annotations

import re
import secrets
import sqlite3
from pathlib import Path

from .config import DB_PATH

LETTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ"
DIGITS = "23456789"
RANDOM_RE = r"[A-HJ-NP-Z][2-9][A-HJ-NP-Z][2-9]"
NEW_PUBLIC_RE = re.compile(rf"^QL-([A-HJ-NP-Z2-9]{{2,4}})-({RANDOM_RE})$", re.I)
LEGACY_PUBLIC_RE = re.compile(r"^QL-([A-HJ-NP-Z2-9]{6})$", re.I)
LEGACY_ALTERNATING_RE = re.compile(r"^QL-([A-HJ-NP-Z][2-9]){3}$", re.I)
LEGACY_RE = re.compile(r"^(?:QC|QJ)[_-]?(\d+)$", re.I)
INTERNAL_RE = re.compile(r"^L[_-]?(\d+)$", re.I)

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


def _connect(db_path: str | Path | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path or DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE IF NOT EXISTS listing_public_ids (
        listing_id TEXT PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""")
    return conn


def normalize_public_id(value: object) -> str | None:
    """Normalize supported QL forms; accept compact and lowercase input."""
    raw = re.sub(r"\s+", "", str(value or "")).upper()
    if not raw.startswith("QL"):
        return None
    tail = raw[2:].lstrip("-")
    if not tail:
        return None
    with_hyphens = "QL-" + tail
    if NEW_PUBLIC_RE.fullmatch(with_hyphens):
        return with_hyphens
    compact = tail.replace("-", "")
    known_codes = {code for code, _aliases in LOCATION_CODES} | {"PP"}
    for location in sorted(known_codes, key=len, reverse=True):
        if compact.startswith(location) and re.fullmatch(RANDOM_RE, compact[len(location):]):
            return f"QL-{location}-{compact[len(location):]}"
    legacy = "QL-" + compact
    if LEGACY_ALTERNATING_RE.fullmatch(legacy):
        return legacy
    if 6 <= len(compact) <= 8:
        location, random_code = compact[:-4], compact[-4:]
        candidate = f"QL-{location}-{random_code}"
        if NEW_PUBLIC_RE.fullmatch(candidate):
            return candidate
    if LEGACY_PUBLIC_RE.fullmatch(legacy):
        return legacy
    return None


def legacy_to_internal(value: object) -> str | None:
    raw = str(value or "").strip()
    match = LEGACY_RE.fullmatch(raw) or INTERNAL_RE.fullmatch(raw)
    return f"l_{int(match.group(1))}" if match else None


def _location_code(conn: sqlite3.Connection, listing_id: str) -> str:
    values: list[str] = []
    for table in ("listings", "drafts"):
        try:
            columns = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
            wanted = [name for name in ("project", "community", "area", "title") if name in columns]
            if not wanted or "listing_id" not in columns:
                continue
            row = conn.execute(
                f"SELECT {','.join(wanted)} FROM {table} WHERE listing_id=? ORDER BY rowid DESC LIMIT 1",
                (listing_id,),
            ).fetchone()
            if row:
                values.extend(str(row[name] or "") for name in wanted)
        except sqlite3.Error:
            continue
    haystack = " ".join(values).lower()
    for code, aliases in LOCATION_CODES:
        if any(alias.lower() in haystack for alias in aliases):
            return code
    return "PP"


def public_listing_id(listing_id: object, *, db_path: str | Path | None = None) -> str:
    raw = str(listing_id or "").strip()
    if not raw:
        return ""
    normalized = normalize_public_id(raw)
    if normalized:
        return normalized
    internal = legacy_to_internal(raw)
    if not internal:
        return ""
    with _connect(db_path) as conn:
        row = conn.execute("SELECT public_id FROM listing_public_ids WHERE listing_id=?", (internal,)).fetchone()
        if row:
            return normalize_public_id(row[0]) or str(row[0]).upper()
        location = _location_code(conn, internal)
        for _ in range(64):
            random_code = "".join((secrets.choice(LETTERS), secrets.choice(DIGITS), secrets.choice(LETTERS), secrets.choice(DIGITS)))
            candidate = f"QL-{location}-{random_code}"
            try:
                conn.execute("INSERT INTO listing_public_ids(listing_id, public_id) VALUES (?, ?)", (internal, candidate))
                conn.commit()
                return candidate
            except sqlite3.IntegrityError:
                row = conn.execute("SELECT public_id FROM listing_public_ids WHERE listing_id=?", (internal,)).fetchone()
                if row:
                    return normalize_public_id(row[0]) or str(row[0]).upper()
        raise RuntimeError("public_listing_id_collision_limit")


def resolve_listing_id(value: object, *, db_path: str | Path | None = None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    internal = legacy_to_internal(raw)
    if internal:
        return internal
    normalized = normalize_public_id(raw)
    if not normalized:
        return None
    with _connect(db_path) as conn:
        row = conn.execute("SELECT listing_id FROM listing_public_ids WHERE UPPER(public_id)=?", (normalized,)).fetchone()
    return str(row[0]) if row else None

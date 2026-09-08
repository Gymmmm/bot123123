"""Stable QL public listing IDs owned by v3_listings."""
from __future__ import annotations

import re
import secrets
import sqlite3

LETTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ"
DIGITS = "23456789"
RANDOM_RE = r"[A-HJ-NP-Z][2-9][A-HJ-NP-Z][2-9]"
NEW_PUBLIC_RE = re.compile(rf"^QL-([A-HJ-NP-Z2-9]{{2,4}})-({RANDOM_RE})$", re.I)

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


def normalize_public_id(value: object) -> str | None:
    raw = re.sub(r"\s+", "", str(value or "")).upper()
    if not raw.startswith("QL"):
        return None
    tail = raw[2:].lstrip("-")
    if not tail:
        return None
    direct = "QL-" + tail
    if NEW_PUBLIC_RE.fullmatch(direct):
        return direct
    compact = tail.replace("-", "")
    for location in sorted({code for code, _ in LOCATION_CODES} | {"PP"}, key=len, reverse=True):
        if compact.startswith(location) and re.fullmatch(RANDOM_RE, compact[len(location):]):
            return f"QL-{location}-{compact[len(location):]}"
    return None


def _location_code(row: sqlite3.Row) -> str:
    values = " ".join(str(row[key] or "") for key in (
        "canonical_area_key", "public_location_key", "public_location_display", "project_name", "project_alias"
    )).lower()
    for code, aliases in LOCATION_CODES:
        if any(alias.lower() in values for alias in aliases):
            return code
    return "PP"


def assign_public_listing_id(conn: sqlite3.Connection, listing_id: int) -> str:
    row = conn.execute("SELECT * FROM v3_listings WHERE id=?", (int(listing_id),)).fetchone()
    if row is None:
        raise LookupError("v3_listing_not_found")
    existing = normalize_public_id(row["public_listing_id"])
    if existing:
        return existing
    location = _location_code(row)
    for _ in range(128):
        random_code = "".join((secrets.choice(LETTERS), secrets.choice(DIGITS), secrets.choice(LETTERS), secrets.choice(DIGITS)))
        candidate = f"QL-{location}-{random_code}"
        try:
            conn.execute(
                "UPDATE v3_listings SET public_listing_id=?,updated_at=CURRENT_TIMESTAMP WHERE id=? AND public_listing_id IS NULL",
                (candidate, int(listing_id)),
            )
            current = conn.execute("SELECT public_listing_id FROM v3_listings WHERE id=?", (int(listing_id),)).fetchone()[0]
            normalized = normalize_public_id(current)
            if normalized:
                return normalized
        except sqlite3.IntegrityError:
            continue
    raise RuntimeError("public_listing_id_collision_limit")


__all__ = ["assign_public_listing_id", "normalize_public_id"]

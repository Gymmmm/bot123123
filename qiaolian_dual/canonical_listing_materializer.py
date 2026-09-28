"""Canonical materialization of a publishable listing.

The canonical facts JSON is the only business-fact source for new data. Legacy
columns remain as a deterministic read projection for compatibility; they are
never used to reconstruct facts.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from qiaolian_dual.canonical_fact_projection import listing_projection, package_gate

DRAFT_COLUMNS = {
    "canonical_facts_hash": "TEXT",
    "canonical_facts_schema": "TEXT",
    "public_location_key": "TEXT",
    "public_location_display": "TEXT",
    "publication_location_level": "TEXT",
    "canonical_area_key": "TEXT",
    "property_subtype": "TEXT",
    "project_brand": "TEXT",
    "canonical_projection_hash": "TEXT",
    "canonical_provenance_json": "TEXT",
    "quality_json": "TEXT",
}
LISTING_COLUMNS = dict(DRAFT_COLUMNS)
PACKAGE_COLUMNS = {
    "canonical_facts_hash": "TEXT",
    "canonical_facts_schema": "TEXT",
    "publication_location_level": "TEXT",
    "canonical_projection_hash": "TEXT",
    "canonical_provenance_json": "TEXT",
    "quality_json": "TEXT",
}

LISTINGS_DDL = """
CREATE TABLE IF NOT EXISTS listings (
    listing_id TEXT PRIMARY KEY, title TEXT NOT NULL, property_type TEXT NOT NULL,
    area TEXT NOT NULL, community TEXT NOT NULL, price INTEGER NOT NULL,
    currency TEXT NOT NULL DEFAULT 'USD', layout TEXT NOT NULL DEFAULT '',
    size_sqm TEXT NOT NULL DEFAULT '', tags_json TEXT NOT NULL DEFAULT '[]',
    highlights TEXT NOT NULL DEFAULT '', hidden_costs TEXT NOT NULL DEFAULT '',
    drawbacks TEXT NOT NULL DEFAULT '', deposit_rule TEXT NOT NULL DEFAULT '',
    available_date TEXT NOT NULL DEFAULT '', media_file_id TEXT NOT NULL DEFAULT '',
    media_type TEXT NOT NULL DEFAULT '', channel_message_id INTEGER,
    source_post_url TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'pending',
    canonical_facts_hash TEXT, canonical_facts_schema TEXT,
    public_location_key TEXT, public_location_display TEXT,
    publication_location_level TEXT, canonical_area_key TEXT,
    property_subtype TEXT, project_brand TEXT,
    canonical_projection_hash TEXT, canonical_provenance_json TEXT, quality_json TEXT,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
)
"""


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _ensure_columns(conn: sqlite3.Connection, table: str, expected: dict[str, str]) -> None:
    current = _table_columns(conn, table)
    if not current:
        return
    for name, sql_type in expected.items():
        if name not in current:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {sql_type}")


def ensure_canonical_projection_schema(conn: sqlite3.Connection) -> None:
    conn.execute(LISTINGS_DDL)
    _ensure_columns(conn, "drafts", DRAFT_COLUMNS)
    _ensure_columns(conn, "listings", LISTING_COLUMNS)
    _ensure_columns(conn, "publication_packages", PACKAGE_COLUMNS)


def canonical_projection_hash(facts: dict[str, Any], projection: dict[str, Any] | None = None) -> str:
    """Hash only deterministic business projection fields, excluding DB metadata."""
    projected = projection if projection is not None else listing_projection(facts, listing_id="", source_post_url="")
    payload = {str(k): projected.get(k) for k in sorted(projected) if k not in {"listing_id", "source_post_url"}}
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def canonical_provenance(facts: dict[str, Any]) -> dict[str, Any]:
    evidence = facts.get("evidence") if isinstance(facts.get("evidence"), dict) else {}
    return {
        "source_identity": facts.get("source_identity") or {},
        "manual_overrides": facts.get("manual_overrides") or [],
        "evidence_fields": sorted(str(k) for k in evidence),
        "canonical_facts_hash": facts.get("canonical_facts_hash"),
        "schema_version": facts.get("schema_version"),
    }


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def materialize_draft_facts(conn: sqlite3.Connection, *, draft_id: str, facts: dict[str, Any]) -> None:
    """Write a complete deterministic legacy projection from canonical facts."""
    _ensure_columns(conn, "drafts", DRAFT_COLUMNS)
    projection = listing_projection(facts, listing_id="", source_post_url="")
    provenance = canonical_provenance(facts)
    updates = {
        "title": projection.get("title") or "待确认房源",
        "property_type": projection.get("property_type") or "未知",
        "project": projection.get("project") or "",
        "community": projection.get("community") or "",
        "area": projection.get("area") or "",
        "price": projection.get("price") or "",
        "layout": projection.get("layout") or "",
        "size": projection.get("size") or "",
        "floor": projection.get("floor") or "",
        "deposit": projection.get("deposit") or "",
        "highlights": projection.get("highlights") or "",
        "available_date": projection.get("available_date") or "",
        "cost_notes": projection.get("cost_notes") or "",
        "extracted_data": _json(facts),
        "normalized_data": _json(facts),
        "queue_score": projection.get("quality_score"),
        "canonical_facts_hash": facts.get("canonical_facts_hash"),
        "canonical_facts_schema": facts.get("schema_version"),
        "public_location_key": facts.get("public_location_key"),
        "public_location_display": facts.get("public_location_display"),
        "publication_location_level": facts.get("publication_location_level"),
        "canonical_area_key": facts.get("canonical_area_key"),
        "property_subtype": facts.get("property_subtype"),
        "project_brand": facts.get("project_brand"),
        "canonical_projection_hash": canonical_projection_hash(facts, projection),
        "canonical_provenance_json": _json(provenance),
        "quality_json": _json(facts.get("quality") or {}),
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    assignments = ", ".join(f"{key}=?" for key in updates)
    conn.execute(f"UPDATE drafts SET {assignments} WHERE draft_id=?", (*updates.values(), draft_id))


def materialize_listing(conn: sqlite3.Connection, *, draft_id: str, listing_id: str,
                        facts: dict[str, Any], media_count: int, source_post_url: str = "") -> dict[str, Any]:
    """Create/update listing only if canonical gate passes; no legacy fields are read."""
    ensure_canonical_projection_schema(conn)
    gate = package_gate(facts, media_count)
    if not gate["ok"]:
        raise ValueError("canonical_listing_gate_blocked:" + ",".join(gate["errors"]))
    values = listing_projection(facts, listing_id=listing_id, source_post_url=source_post_url)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    row = {
        "listing_id": listing_id, "title": values["title"], "property_type": values["property_type"],
        "area": values["area"], "community": values["community"], "price": int(values["price"]),
        "currency": values["currency"], "layout": values["layout"], "size_sqm": str(values["size_sqm"] or ""),
        "tags_json": values["tags_json"], "highlights": values["highlights"], "hidden_costs": values["hidden_costs"],
        "drawbacks": "", "deposit_rule": values["deposit_rule"], "available_date": values["available_date"],
        "media_file_id": "", "media_type": str(facts.get("media_summary", {}).get("media_type") or "image"),
        "channel_message_id": None, "source_post_url": source_post_url, "status": "pending",
        "canonical_facts_hash": facts.get("canonical_facts_hash"), "canonical_facts_schema": facts.get("schema_version"),
        "public_location_key": values["public_location_key"], "public_location_display": values["public_location_display"],
        "publication_location_level": values["publication_location_level"], "canonical_area_key": values["canonical_area_key"],
        "property_subtype": values["property_subtype"], "project_brand": values["project_brand"],
        "canonical_projection_hash": canonical_projection_hash(facts, values),
        "canonical_provenance_json": _json(canonical_provenance(facts)), "quality_json": _json(facts.get("quality") or {}),
        "created_at": now, "updated_at": now,
    }
    cols = list(row)
    placeholders = ",".join("?" for _ in cols)
    updates = ",".join(f"{c}=excluded.{c}" for c in cols if c not in {"listing_id", "created_at"})
    conn.execute(f"INSERT INTO listings ({','.join(cols)}) VALUES ({placeholders}) ON CONFLICT(listing_id) DO UPDATE SET {updates}", tuple(row[c] for c in cols))
    conn.execute("UPDATE drafts SET listing_id=? WHERE draft_id=?", (listing_id, draft_id))
    return {"listing_id": listing_id, "gate": gate, "projection": values}


def verify_draft_listing_consistency(conn: sqlite3.Connection, draft_id: str) -> list[str]:
    ensure_canonical_projection_schema(conn)
    row = conn.execute("SELECT d.listing_id,d.normalized_data,d.canonical_projection_hash AS dh,l.canonical_projection_hash AS lh FROM drafts d LEFT JOIN listings l ON l.listing_id=d.listing_id WHERE d.draft_id=?", (draft_id,)).fetchone()
    if not row:
        return ["draft_not_found"]
    if not row[0]:
        return ["draft_listing_missing"]
    if row[3] is None:
        return ["listing_not_found"]
    errors: list[str] = []
    try:
        facts = json.loads(row[1] or "{}")
        expected = canonical_projection_hash(facts)
        if row[2] != expected or row[3] != expected:
            errors.append("canonical_projection_hash_mismatch")
    except Exception:
        errors.append("canonical_facts_json_invalid")
    return errors


__all__ = ["ensure_canonical_projection_schema", "canonical_projection_hash", "canonical_provenance", "materialize_draft_facts", "materialize_listing", "verify_draft_listing_consistency"]

"""V3 Phase-1 additive shadow persistence.

This module does not own schema creation. The migration in
migrations/001_v3_phase1_core.sql must be applied first. If it has not been
applied, shadow writes are skipped so the V2.2 production path remains intact.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from typing import Any

from qiaolian_dual.canonical_facts import draft_projection


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _facts_hash(facts: dict[str, Any]) -> str:
    payload = {key: value for key, value in facts.items() if key != "canonical_facts_hash"}
    return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()


def normalize_v3_facts(facts: dict[str, Any]) -> dict[str, Any]:
    """Move valid sale out of the legacy non-rental rejection boundary.

    V2.2 canonical parsing already extracts sale facts correctly, but its quality
    object adds ``non_rental_source`` as a hard blocker. V3 stores real-estate
    sale facts and blocks them only at publication eligibility.
    """
    result = dict(facts or {})
    if str(result.get("deal_type") or "") != "sale":
        return result
    quality = dict(result.get("quality") or {})
    for key in ("hard_flags", "blocking_flags", "all_flags"):
        quality[key] = [str(x) for x in (quality.get(key) or []) if str(x) != "non_rental_source"]
    result["quality"] = quality
    result["canonical_facts_hash"] = _facts_hash(result)
    return result


def _quality_status(facts: dict[str, Any]) -> str:
    quality = dict(facts.get("quality") or {})
    if quality.get("hard_flags"):
        return "invalid"
    if quality.get("review_flags"):
        return "needs_review"
    return "valid"


def _offer_types(facts: dict[str, Any]) -> list[str]:
    deal_type = str(facts.get("deal_type") or "unknown")
    if deal_type == "rent":
        return ["rent"] if facts.get("monthly_rent_usd") else []
    if deal_type == "sale":
        return ["sale"] if facts.get("sale_price_usd") else []
    if deal_type == "mixed":
        out: list[str] = []
        if facts.get("monthly_rent_usd"):
            out.append("rent")
        if facts.get("sale_price_usd"):
            out.append("sale")
        return out
    return []


def _listing_id(source_post_id: int) -> str:
    return f"V3SRC_{int(source_post_id):010d}"


def _ensure_legacy_listing_row(conn: sqlite3.Connection, source_post_id: int, facts: dict[str, Any]) -> str:
    """Create a pending compatibility listing row for the V3 shadow inventory.

    Phase 1 intentionally does not redesign the legacy ``listings`` table yet.
    Sale rows therefore use price=0 only in this compatibility projection; the
    authoritative transaction price lives in ``listing_offers.sale_price_usd``.
    """
    listing_id = _listing_id(source_post_id)
    projection = draft_projection(facts)
    row = {
        "listing_id": listing_id,
        "title": projection.get("title") or "待确认房源",
        "property_type": projection.get("property_type") or "未知",
        "area": projection.get("public_location_display") or "",
        "community": projection.get("community") or "",
        "price": int(projection.get("price") or 0),
        "currency": "USD",
        "layout": projection.get("layout") or "",
        "size_sqm": str(projection.get("size") or ""),
        "tags_json": "[]",
        "highlights": "\n".join(projection.get("highlights") or []),
        "hidden_costs": projection.get("cost_notes") or "",
        "drawbacks": "",
        "deposit_rule": projection.get("deposit") or "",
        "available_date": projection.get("available_date") or "",
        "media_file_id": "",
        "media_type": str((facts.get("media_summary") or {}).get("media_type") or "image"),
        "channel_message_id": None,
        "source_post_url": "",
        "status": "pending",
        "canonical_facts_hash": facts.get("canonical_facts_hash"),
        "canonical_facts_schema": facts.get("schema_version"),
        "public_location_key": facts.get("public_location_key"),
        "public_location_display": facts.get("public_location_display"),
        "publication_location_level": facts.get("publication_location_level"),
        "canonical_area_key": facts.get("canonical_area_key"),
        "property_subtype": facts.get("property_subtype"),
        "project_brand": facts.get("project_brand"),
    }
    columns = {str(item[1]) for item in conn.execute("PRAGMA table_info(listings)")}
    payload = {key: value for key, value in row.items() if key in columns}
    if "created_at" in columns:
        payload["created_at"] = conn.execute("SELECT CURRENT_TIMESTAMP").fetchone()[0]
    if "updated_at" in columns:
        payload["updated_at"] = conn.execute("SELECT CURRENT_TIMESTAMP").fetchone()[0]
    keys = list(payload)
    placeholders = ",".join("?" for _ in keys)
    updates = ",".join(f"{key}=excluded.{key}" for key in keys if key not in {"listing_id", "created_at"})
    conn.execute(
        f"INSERT INTO listings ({','.join(keys)}) VALUES ({placeholders}) "
        f"ON CONFLICT(listing_id) DO UPDATE SET {updates}",
        tuple(payload[key] for key in keys),
    )
    return listing_id


def shadow_write_v3(conn: sqlite3.Connection, *, source_post_id: int, facts: dict[str, Any]) -> dict[str, Any]:
    """Dual-write canonical facts and offers without changing V2 publisher flow."""
    try:
        conn.execute("SELECT 1 FROM canonical_records LIMIT 1")
        conn.execute("SELECT 1 FROM listing_offers LIMIT 1")
    except sqlite3.OperationalError:
        return {"status": "schema_not_applied"}

    source = conn.execute(
        "SELECT COALESCE(revision,1) FROM source_posts WHERE id=?",
        (int(source_post_id),),
    ).fetchone()
    source_revision = int(source[0] if source else 1)
    quality = dict(facts.get("quality") or {})
    facts_hash = str(facts.get("canonical_facts_hash") or _facts_hash(facts))

    existing = conn.execute(
        "SELECT id FROM canonical_records WHERE source_post_id=? AND source_revision=? AND facts_hash=? LIMIT 1",
        (int(source_post_id), source_revision, facts_hash),
    ).fetchone()
    if existing:
        canonical_id = str(existing[0])
    else:
        previous = conn.execute(
            "SELECT id FROM canonical_records WHERE source_post_id=? AND is_current=1 ORDER BY created_at DESC LIMIT 1",
            (int(source_post_id),),
        ).fetchone()
        previous_id = str(previous[0]) if previous else None
        if previous_id:
            conn.execute("UPDATE canonical_records SET is_current=0 WHERE source_post_id=?", (int(source_post_id),))
        canonical_id = f"CAN_{uuid.uuid4().hex.upper()}"
        conn.execute(
            """
            INSERT INTO canonical_records (
                id, source_post_id, source_revision, schema_version, parser_revision,
                facts_json, facts_hash, deal_type, quality_score, quality_status,
                hard_flags_json, review_flags_json, supersedes_id, is_current
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                canonical_id,
                int(source_post_id),
                source_revision,
                str(facts.get("schema_version") or ""),
                str(facts.get("parser_revision") or ""),
                _json(facts),
                facts_hash,
                str(facts.get("deal_type") or "unknown"),
                int(quality.get("score") or 0),
                _quality_status(facts),
                _json(quality.get("hard_flags") or []),
                _json(quality.get("review_flags") or []),
                previous_id,
            ),
        )

    offer_types = _offer_types(facts)
    listing_id = None
    offer_ids: list[str] = []
    if offer_types:
        listing_id = _ensure_legacy_listing_row(conn, source_post_id, facts)
        for offer_type in offer_types:
            offer_id = f"OFF_{uuid.uuid4().hex.upper()}"
            if offer_type == "sale":
                publishable = 0
                block_reason = "sale_not_enabled_for_rent_channel"
            else:
                publishable = int(
                    str(facts.get("deal_type") or "") == "rent"
                    and not (quality.get("hard_flags") or [])
                    and not (quality.get("review_flags") or [])
                    and bool(facts.get("monthly_rent_usd"))
                )
                block_reason = "" if publishable else "canonical_not_publishable"
            conn.execute(
                """
                INSERT INTO listing_offers (
                    id, listing_id, canonical_record_id, offer_type, currency,
                    monthly_rent_usd, sale_price_usd, original_price_usd,
                    deposit_terms, payment_terms, contract_term, available_date,
                    offer_status, publishable, publish_block_reason
                ) VALUES (?, ?, ?, ?, 'USD', ?, ?, ?, ?, ?, ?, ?, 'stored', ?, ?)
                ON CONFLICT(listing_id, canonical_record_id, offer_type) DO UPDATE SET
                    monthly_rent_usd=excluded.monthly_rent_usd,
                    sale_price_usd=excluded.sale_price_usd,
                    original_price_usd=excluded.original_price_usd,
                    deposit_terms=excluded.deposit_terms,
                    payment_terms=excluded.payment_terms,
                    contract_term=excluded.contract_term,
                    available_date=excluded.available_date,
                    publishable=excluded.publishable,
                    publish_block_reason=excluded.publish_block_reason,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    offer_id,
                    listing_id,
                    canonical_id,
                    offer_type,
                    facts.get("monthly_rent_usd") if offer_type == "rent" else None,
                    facts.get("sale_price_usd") if offer_type == "sale" else None,
                    facts.get("original_monthly_rent_usd") if offer_type == "rent" else None,
                    str(facts.get("deposit_payment_terms") or ""),
                    str(facts.get("deposit_payment_terms") or ""),
                    str(facts.get("contract_term_display") or ""),
                    str(facts.get("available_date") or ""),
                    publishable,
                    block_reason,
                ),
            )
            stored = conn.execute(
                "SELECT id FROM listing_offers WHERE listing_id=? AND canonical_record_id=? AND offer_type=?",
                (listing_id, canonical_id, offer_type),
            ).fetchone()
            if stored:
                offer_ids.append(str(stored[0]))

    conn.commit()
    return {
        "status": "written",
        "canonical_record_id": canonical_id,
        "listing_id": listing_id,
        "offer_ids": offer_ids,
        "offer_types": offer_types,
    }


__all__ = ["normalize_v3_facts", "shadow_write_v3"]

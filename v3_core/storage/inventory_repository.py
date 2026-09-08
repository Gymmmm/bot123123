"""Repository for the migration-safe V3 canonical inventory tables."""
from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any

from v3_core.inventory.materializer_v3 import (
    listing_projection_v3,
    offer_projections_v3,
)
from .schema import ensure_v3_schema


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class InventoryRepository:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        with self._connect() as conn:
            ensure_v3_schema(conn)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def store_canonical(
        self,
        *,
        source_post_id: object,
        facts: dict[str, Any],
        supersedes_id: str = "",
    ) -> sqlite3.Row:
        facts = dict(facts or {})
        facts_hash = str(facts.get("canonical_facts_hash") or "").strip()
        schema_version = str(facts.get("schema_version") or "").strip()
        if not facts_hash or not schema_version:
            raise ValueError("canonical facts require schema_version and canonical_facts_hash")
        source_key = str(source_post_id or "").strip()
        if not source_key:
            raise ValueError("source_post_id is required")
        quality = facts.get("quality") if isinstance(facts.get("quality"), dict) else {}
        review_status = "needs_review" if (quality.get("all_flags") or []) else "valid"

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT * FROM canonical_records
                   WHERE source_post_id=? AND facts_hash=?""",
                (source_key, facts_hash),
            ).fetchone()
            if row is None:
                canonical_record_id = "CAN_" + uuid.uuid4().hex
                conn.execute(
                    """INSERT INTO canonical_records
                       (canonical_record_id,source_post_id,schema_version,parser_revision,
                        deal_type,facts_json,facts_hash,quality_score,quality_json,
                        review_status,supersedes_id)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        canonical_record_id,
                        source_key,
                        schema_version,
                        str(facts.get("parser_revision") or ""),
                        str(facts.get("deal_type") or "unknown"),
                        _json(facts),
                        facts_hash,
                        int(quality.get("score") or 0),
                        _json(quality),
                        review_status,
                        str(supersedes_id or "") or None,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM canonical_records WHERE canonical_record_id=?",
                    (canonical_record_id,),
                ).fetchone()
            conn.commit()
        assert row is not None
        return row

    def canonical_facts(self, canonical_record_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT facts_json FROM canonical_records WHERE canonical_record_id=?",
                (str(canonical_record_id),),
            ).fetchone()
        if row is None:
            raise KeyError(canonical_record_id)
        value = json.loads(str(row["facts_json"]))
        if not isinstance(value, dict):
            raise ValueError("canonical facts payload is not an object")
        return value

    def upsert_listing(
        self,
        *,
        listing_id: str,
        canonical_record_id: str,
        public_listing_id: str,
        facts: dict[str, Any],
    ) -> sqlite3.Row:
        values = listing_projection_v3(facts)
        listing_key = str(listing_id or "").strip()
        if not listing_key:
            raise ValueError("listing_id is required")
        public_id = str(public_listing_id or "").strip()
        if not public_id:
            raise ValueError("public_listing_id is required")

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """INSERT INTO listings_v3
                   (listing_id,public_listing_id,canonical_record_id,
                    project_name,project_alias,project_brand,property_type,
                    property_subtype,public_location_key,public_location_display,
                    canonical_area_key,canonical_area_display,publication_location_level,
                    layout,bedrooms,bathrooms,size_sqm,floor,display_title,
                    canonical_facts_hash,canonical_facts_schema,inventory_status,data_status)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'active','current')
                   ON CONFLICT(listing_id) DO UPDATE SET
                    public_listing_id=excluded.public_listing_id,
                    canonical_record_id=excluded.canonical_record_id,
                    project_name=excluded.project_name,
                    project_alias=excluded.project_alias,
                    project_brand=excluded.project_brand,
                    property_type=excluded.property_type,
                    property_subtype=excluded.property_subtype,
                    public_location_key=excluded.public_location_key,
                    public_location_display=excluded.public_location_display,
                    canonical_area_key=excluded.canonical_area_key,
                    canonical_area_display=excluded.canonical_area_display,
                    publication_location_level=excluded.publication_location_level,
                    layout=excluded.layout,
                    bedrooms=excluded.bedrooms,
                    bathrooms=excluded.bathrooms,
                    size_sqm=excluded.size_sqm,
                    floor=excluded.floor,
                    display_title=excluded.display_title,
                    canonical_facts_hash=excluded.canonical_facts_hash,
                    canonical_facts_schema=excluded.canonical_facts_schema,
                    data_status='current',updated_at=CURRENT_TIMESTAMP""",
                (
                    listing_key,
                    public_id,
                    str(canonical_record_id),
                    values["project_name"],
                    values["project_alias"],
                    values["project_brand"],
                    values["property_type"],
                    values["property_subtype"],
                    values["public_location_key"],
                    values["public_location_display"],
                    values["canonical_area_key"],
                    values["canonical_area_display"],
                    values["publication_location_level"],
                    values["layout"],
                    values["bedrooms"],
                    values["bathrooms"],
                    values["size_sqm"],
                    values["floor"],
                    values["display_title"],
                    values["canonical_facts_hash"],
                    values["canonical_facts_schema"],
                ),
            )
            row = conn.execute(
                "SELECT * FROM listings_v3 WHERE listing_id=?", (listing_key,)
            ).fetchone()
            conn.commit()
        assert row is not None
        return row

    def sync_offers(
        self, *, listing_id: str, facts: dict[str, Any]
    ) -> list[sqlite3.Row]:
        projections = offer_projections_v3(facts)
        listing_key = str(listing_id or "").strip()
        wanted_types = {str(item["offer_type"]) for item in projections}

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                "SELECT * FROM listing_offers WHERE listing_id=? AND offer_status='active'",
                (listing_key,),
            ).fetchall()
            for row in current:
                if str(row["offer_type"]) not in wanted_types:
                    conn.execute(
                        """UPDATE listing_offers SET offer_status='inactive',publishable=0,
                           publish_block_reason='canonical_offer_removed',updated_at=CURRENT_TIMESTAMP
                           WHERE offer_id=?""",
                        (str(row["offer_id"]),),
                    )

            output: list[sqlite3.Row] = []
            for item in projections:
                existing = conn.execute(
                    """SELECT * FROM listing_offers
                       WHERE listing_id=? AND offer_type=? AND offer_status='active'
                       ORDER BY created_at DESC LIMIT 1""",
                    (listing_key, str(item["offer_type"])),
                ).fetchone()
                if existing is None:
                    offer_id = "OFF_" + uuid.uuid4().hex
                    conn.execute(
                        """INSERT INTO listing_offers
                           (offer_id,listing_id,offer_type,currency,monthly_rent_usd,
                            sale_price_usd,deposit_terms,payment_terms,contract_term,
                            available_date,offer_status,publication_policy,publishable,
                            publish_block_reason)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?, ?,?,?)""",
                        (
                            offer_id,
                            listing_key,
                            item["offer_type"],
                            item["currency"],
                            item["monthly_rent_usd"],
                            item["sale_price_usd"],
                            item["deposit_terms"],
                            item["payment_terms"],
                            item["contract_term"],
                            item["available_date"],
                            item["offer_status"],
                            item["publication_policy"],
                            int(bool(item["publishable"])),
                            item["publish_block_reason"],
                        ),
                    )
                else:
                    offer_id = str(existing["offer_id"])
                    conn.execute(
                        """UPDATE listing_offers SET currency=?,monthly_rent_usd=?,
                           sale_price_usd=?,deposit_terms=?,payment_terms=?,contract_term=?,
                           available_date=?,publication_policy=?,publishable=?,
                           publish_block_reason=?,updated_at=CURRENT_TIMESTAMP
                           WHERE offer_id=?""",
                        (
                            item["currency"],
                            item["monthly_rent_usd"],
                            item["sale_price_usd"],
                            item["deposit_terms"],
                            item["payment_terms"],
                            item["contract_term"],
                            item["available_date"],
                            item["publication_policy"],
                            int(bool(item["publishable"])),
                            item["publish_block_reason"],
                            offer_id,
                        ),
                    )
                row = conn.execute(
                    "SELECT * FROM listing_offers WHERE offer_id=?", (offer_id,)
                ).fetchone()
                assert row is not None
                output.append(row)
            conn.commit()
        return output

    def set_offer_publication(
        self, *, offer_id: str, publishable: bool, block_reason: str = ""
    ) -> sqlite3.Row:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT offer_type,publication_policy FROM listing_offers WHERE offer_id=?",
                (str(offer_id),),
            ).fetchone()
            if row is None:
                raise KeyError(offer_id)
            if publishable and (
                str(row["offer_type"]) != "rent"
                or str(row["publication_policy"]) != "telegram_rent"
            ):
                raise ValueError("only telegram_rent offers may become publishable")
            conn.execute(
                """UPDATE listing_offers SET publishable=?,publish_block_reason=?,
                   updated_at=CURRENT_TIMESTAMP WHERE offer_id=?""",
                (int(bool(publishable)), str(block_reason or ""), str(offer_id)),
            )
            updated = conn.execute(
                "SELECT * FROM listing_offers WHERE offer_id=?", (str(offer_id),)
            ).fetchone()
            conn.commit()
        assert updated is not None
        return updated

    def create_review_item(
        self,
        *,
        canonical_record_id: str,
        listing_id: str = "",
        offer_id: str = "",
        review_score: int = 0,
        review_note: str = "",
    ) -> sqlite3.Row:
        review_id = "REV_" + uuid.uuid4().hex
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO review_items
                   (review_id,canonical_record_id,listing_id,offer_id,review_status,
                    review_score,review_note)
                   VALUES (?,?,?,?, 'pending',?,?)""",
                (
                    review_id,
                    str(canonical_record_id),
                    str(listing_id or ""),
                    str(offer_id or ""),
                    int(review_score or 0),
                    str(review_note or ""),
                ),
            )
            row = conn.execute(
                "SELECT * FROM review_items WHERE review_id=?", (review_id,)
            ).fetchone()
            conn.commit()
        assert row is not None
        return row

    def approve_review(self, *, review_id: str, operator_user_id: str) -> sqlite3.Row:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                """UPDATE review_items SET review_status='approved',operator_user_id=?,
                   approved_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
                   WHERE review_id=? AND review_status IN ('pending','approved')""",
                (str(operator_user_id or ""), str(review_id)),
            )
            if cur.rowcount != 1:
                raise KeyError(review_id)
            row = conn.execute(
                "SELECT * FROM review_items WHERE review_id=?", (str(review_id),)
            ).fetchone()
            conn.commit()
        assert row is not None
        return row

    def add_override(
        self,
        *,
        canonical_record_id: str,
        field_name: str,
        old_value: Any,
        new_value: Any,
        reason: str,
        operator_id: str,
    ) -> sqlite3.Row:
        override_id = "OVR_" + uuid.uuid4().hex
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO canonical_overrides
                   (override_id,canonical_record_id,field_name,old_value_json,
                    new_value_json,reason,operator_id)
                   VALUES (?,?,?,?,?,?,?)""",
                (
                    override_id,
                    str(canonical_record_id),
                    str(field_name),
                    _json(old_value),
                    _json(new_value),
                    str(reason or ""),
                    str(operator_id or ""),
                ),
            )
            row = conn.execute(
                "SELECT * FROM canonical_overrides WHERE override_id=?",
                (override_id,),
            ).fetchone()
            conn.commit()
        assert row is not None
        return row


__all__ = ["InventoryRepository"]

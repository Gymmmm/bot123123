"""Align collected V3 inventory onto shared sale offers.

New collector rows already pass through parse -> identity -> materialize.
This module repairs the existing shared database so sale facts become sale
offers without touching rent publication flags. A full scan is enough once
per Asia/Phnom_Penh day.
"""
from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import sqlite3
import uuid
from typing import Any
from zoneinfo import ZoneInfo

from v3_core.inventory.canonical_facts import canonicalize_source
from v3_core.inventory.identity import IdentityService
from v3_core.inventory.materializer_v3 import offer_projections_v3
from v3_core.inventory.service import InventoryMaterializationService
from v3_core.ops.runtime_state import RuntimeStateRepository
from v3_core.parser.authoritative_enrichment import enrich_authoritative_facts
from v3_core.storage.inventory_repository import InventoryRepository

SALE_ALIGN_COMPONENT = "sale_align"
SALE_ALIGN_TIMEZONE = ZoneInfo("Asia/Phnom_Penh")


def _positive_int(value: Any) -> int | None:
    try:
        number = int(float(value))
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _load_facts(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    try:
        payload = json.loads(str(raw or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def local_scan_date(now: datetime | None = None) -> str:
    current = now or datetime.now(SALE_ALIGN_TIMEZONE)
    if current.tzinfo is None:
        current = current.replace(tzinfo=SALE_ALIGN_TIMEZONE)
    return current.astimezone(SALE_ALIGN_TIMEZONE).date().isoformat()


def due_for_daily_scan(
    runtime: RuntimeStateRepository,
    *,
    now: datetime | None = None,
) -> bool:
    today = local_scan_date(now)
    row = runtime.component_rows().get(SALE_ALIGN_COMPONENT) or {}
    try:
        meta = json.loads(str(row.get("meta_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        meta = {}
    if not isinstance(meta, dict):
        meta = {}
    return str(meta.get("last_scan_date") or "") != today


def mark_daily_scan(
    runtime: RuntimeStateRepository,
    stats: dict[str, Any],
    *,
    now: datetime | None = None,
) -> None:
    meta = {"last_scan_date": local_scan_date(now), **dict(stats or {})}
    runtime.heartbeat(SALE_ALIGN_COMPONENT, state="running", event=True, meta=meta)


class SaleInventoryAligner:
    """Write-path helper that keeps sale inventory aligned to collected facts."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()
        self.inventory = InventoryRepository(self.db_path)
        self.identities = IdentityService(self.db_path)
        self.materializer = InventoryMaterializationService(self.inventory)
        self.runtime = RuntimeStateRepository(self.db_path)

    def _connect(self) -> sqlite3.Connection:
        if not self.db_path.is_file():
            raise FileNotFoundError(self.db_path)
        uri = f"file:{self.db_path.as_posix()}?mode=rw"
        conn = sqlite3.connect(uri, uri=True, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def enforce_sale_policy(self) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """UPDATE listing_offers
                   SET publication_policy='store_only',
                       publishable=0,
                       publish_block_reason='sale_not_enabled_for_telegram',
                       updated_at=CURRENT_TIMESTAMP
                   WHERE offer_type='sale'
                     AND (
                       COALESCE(publication_policy,'')<>'store_only'
                       OR COALESCE(publishable,1)<>0
                       OR COALESCE(publish_block_reason,'')<>'sale_not_enabled_for_telegram'
                     )"""
            )
            conn.commit()
            return int(cur.rowcount or 0)

    def _upsert_sale_offer(self, conn: sqlite3.Connection, listing_id: str, sale_price: int) -> str:
        listing_key = str(listing_id)
        existing = conn.execute(
            """SELECT offer_id FROM listing_offers
               WHERE listing_id=? AND offer_type='sale' AND offer_status='active'
               ORDER BY created_at DESC LIMIT 1""",
            (listing_key,),
        ).fetchone()
        if existing is None:
            offer_id = "OFF_" + uuid.uuid4().hex
            conn.execute(
                """INSERT INTO listing_offers
                   (offer_id,listing_id,offer_type,currency,monthly_rent_usd,
                    sale_price_usd,deposit_terms,payment_terms,contract_term,
                    available_date,offer_status,publication_policy,publishable,
                    publish_block_reason)
                   VALUES (?,?,'sale','USD',NULL,?, '','','','',
                           'active','store_only',0,'sale_not_enabled_for_telegram')""",
                (offer_id, listing_key, int(sale_price)),
            )
            return offer_id
        offer_id = str(existing["offer_id"])
        conn.execute(
            """UPDATE listing_offers
               SET sale_price_usd=?,
                   publication_policy='store_only',
                   publishable=0,
                   publish_block_reason='sale_not_enabled_for_telegram',
                   offer_status='active',
                   updated_at=CURRENT_TIMESTAMP
               WHERE offer_id=?""",
            (int(sale_price), offer_id),
        )
        return offer_id

    def _inactivate_sale_offers(self, conn: sqlite3.Connection, listing_id: str) -> int:
        cur = conn.execute(
            """UPDATE listing_offers
               SET offer_status='inactive',
                   publishable=0,
                   publish_block_reason='canonical_offer_removed',
                   updated_at=CURRENT_TIMESTAMP
               WHERE listing_id=? AND offer_type='sale' AND offer_status='active'""",
            (str(listing_id),),
        )
        return int(cur.rowcount or 0)

    def harvest_sale_sources(self) -> dict[str, int]:
        """Re-read collected source text and project explicit sale prices."""
        stats = {"sources_seen": 0, "sources_with_sale": 0}
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT id, raw_text,
                          COALESCE(json_extract(raw_meta_json,'$.sanitized_text'), raw_text) AS sanitized_text
                   FROM source_posts
                   WHERE raw_text GLOB '*售价*'
                      OR raw_text GLOB '*出售*'
                      OR raw_text GLOB '*卖价*'
                      OR lower(raw_text) GLOB '*sale price*'"""
            ).fetchall()
        for row in rows:
            stats["sources_seen"] += 1
            raw_text = str(row["raw_text"] or "")
            sanitized = str(row["sanitized_text"] or raw_text)
            try:
                facts = canonicalize_source(
                    raw_text=raw_text,
                    sanitized_text=sanitized,
                    source_identity={"source_post_id": int(row["id"])},
                    media_summary={"image_count": 0, "video_count": 0, "media_type": "none"},
                )
                facts = enrich_authoritative_facts(sanitized, facts)
            except Exception:
                continue
            sale_price = _positive_int(facts.get("sale_price_usd"))
            if sale_price is None:
                continue
            stats["sources_with_sale"] += 1
            canonical = self.inventory.store_canonical(
                source_post_id=int(row["id"]),
                facts=facts,
            )
            with self._connect() as conn:
                existing = conn.execute(
                    """SELECT l.listing_id,l.public_listing_id
                       FROM listings_v3 l
                       JOIN canonical_records c
                         ON c.canonical_record_id=l.canonical_record_id
                       WHERE c.source_post_id=?
                       ORDER BY l.updated_at DESC LIMIT 1""",
                    (str(row["id"]),),
                ).fetchone()
            if existing is None:
                identity = self.identities.allocate(
                    canonical_record_id=str(canonical["canonical_record_id"]),
                    facts=facts,
                )
                listing_id = identity.listing_id
                public_id = identity.public_listing_id
            else:
                listing_id = str(existing["listing_id"])
                public_id = str(existing["public_listing_id"])
            self.inventory.upsert_listing(
                listing_id=listing_id,
                public_listing_id=public_id,
                canonical_record_id=str(canonical["canonical_record_id"]),
                facts=facts,
            )
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                self._upsert_sale_offer(conn, listing_id, sale_price)
                conn.commit()
        return stats

    def align_existing_listings(self) -> dict[str, int]:
        stats = {"listings_seen": 0, "sale_upserted": 0, "sale_inactivated": 0}
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT listing_id,canonical_record_id
                   FROM listings_v3
                   WHERE data_status='current'"""
            ).fetchall()
            conn.execute("BEGIN IMMEDIATE")
            for row in rows:
                stats["listings_seen"] += 1
                listing_id = str(row["listing_id"])
                facts_row = conn.execute(
                    "SELECT facts_json FROM canonical_records WHERE canonical_record_id=?",
                    (str(row["canonical_record_id"]),),
                ).fetchone()
                facts = _load_facts(None if facts_row is None else facts_row["facts_json"])
                sale_price = _positive_int(facts.get("sale_price_usd"))
                if sale_price is None:
                    stats["sale_inactivated"] += self._inactivate_sale_offers(conn, listing_id)
                    continue
                self._upsert_sale_offer(conn, listing_id, sale_price)
                stats["sale_upserted"] += 1
            conn.commit()
        return stats

    def materialize_orphaned_sale_records(self) -> dict[str, int]:
        created = 0
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT c.canonical_record_id,c.facts_json
                   FROM canonical_records c
                   LEFT JOIN listings_v3 l
                     ON l.canonical_record_id=c.canonical_record_id
                    AND l.data_status='current'
                   WHERE l.listing_id IS NULL"""
            ).fetchall()
        for row in rows:
            facts = _load_facts(row["facts_json"])
            projections = offer_projections_v3(facts)
            if not any(item["offer_type"] == "sale" for item in projections):
                continue
            identity = self.identities.allocate(
                canonical_record_id=str(row["canonical_record_id"]),
                facts=facts,
            )
            self.materializer.materialize(
                canonical_record_id=str(row["canonical_record_id"]),
                listing_id=identity.listing_id,
                public_listing_id=identity.public_listing_id,
                create_review=False,
            )
            created += 1
        return {"orphans_materialized": created}

    def align(self) -> dict[str, int]:
        stats = {"policy_repaired": self.enforce_sale_policy()}
        stats.update(self.harvest_sale_sources())
        stats.update(self.align_existing_listings())
        stats.update(self.materialize_orphaned_sale_records())
        stats["policy_repaired"] += self.enforce_sale_policy()
        return stats

    def align_if_due(
        self,
        *,
        force: bool = False,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        if not force and not due_for_daily_scan(self.runtime, now=now):
            return {"skipped": 1, "reason": "already_scanned_today"}
        stats = self.align()
        mark_daily_scan(self.runtime, stats, now=now)
        return {"skipped": 0, **stats}


__all__ = [
    "SALE_ALIGN_COMPONENT",
    "SaleInventoryAligner",
    "due_for_daily_scan",
    "local_scan_date",
    "mark_daily_scan",
]

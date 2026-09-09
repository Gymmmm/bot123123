"""Read-only quality audit for the future V3 sale inventory.

The audit is deliberately diagnostic only. It never creates schema, reparses
sources, changes listing/offers, or repairs publication policy.
"""
from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Any


class SaleInventoryAudit:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self) -> sqlite3.Connection:
        if not self.db_path.is_file():
            raise FileNotFoundError(self.db_path)
        uri = f"file:{self.db_path.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    @staticmethod
    def _scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
        row = conn.execute(sql, params).fetchone()
        return int(row[0] or 0)

    @staticmethod
    def _public_ids(conn: sqlite3.Connection, sql: str, *, limit: int = 20) -> list[str]:
        rows = conn.execute(sql, (max(1, int(limit)),)).fetchall()
        result: list[str] = []
        for row in rows:
            value = str(row[0] or "").strip()
            result.append(value or "（缺少公开编号）")
        return result

    def report(self, *, sample_limit: int = 20) -> dict[str, Any]:
        with self._connect() as conn:
            active_sale = self._scalar(
                conn,
                """SELECT COUNT(*) FROM listing_offers
                   WHERE offer_type='sale' AND offer_status='active' AND sale_price_usd>0""",
            )
            policy_violations = self._scalar(
                conn,
                """SELECT COUNT(*) FROM listing_offers
                   WHERE offer_type='sale' AND offer_status='active'
                     AND (publication_policy<>'store_only' OR publishable<>0)""",
            )
            duplicate_active_sale_listings = self._scalar(
                conn,
                """SELECT COUNT(*) FROM (
                       SELECT listing_id FROM listing_offers
                       WHERE offer_type='sale' AND offer_status='active'
                       GROUP BY listing_id HAVING COUNT(*)>1
                   )""",
            )
            missing_public_id = self._scalar(
                conn,
                """SELECT COUNT(DISTINCT l.listing_id)
                   FROM listings_v3 l
                   JOIN listing_offers o ON o.listing_id=l.listing_id
                   WHERE o.offer_type='sale' AND o.offer_status='active'
                     AND TRIM(COALESCE(l.public_listing_id,''))=''""",
            )
            noncurrent_listing = self._scalar(
                conn,
                """SELECT COUNT(DISTINCT l.listing_id)
                   FROM listings_v3 l
                   JOIN listing_offers o ON o.listing_id=l.listing_id
                   WHERE o.offer_type='sale' AND o.offer_status='active'
                     AND l.data_status<>'current'""",
            )
            unavailable_status = self._scalar(
                conn,
                """SELECT COUNT(DISTINCT l.listing_id)
                   FROM listings_v3 l
                   JOIN listing_offers o ON o.listing_id=l.listing_id
                   WHERE o.offer_type='sale' AND o.offer_status='active'
                     AND l.inventory_status NOT IN ('active','reserved')""",
            )
            missing_location = self._scalar(
                conn,
                """SELECT COUNT(DISTINCT l.listing_id)
                   FROM listings_v3 l
                   JOIN listing_offers o ON o.listing_id=l.listing_id
                   WHERE o.offer_type='sale' AND o.offer_status='active'
                     AND l.data_status='current'
                     AND TRIM(COALESCE(l.project_name,''))=''
                     AND TRIM(COALESCE(l.public_location_display,''))=''
                     AND TRIM(COALESCE(l.canonical_area_display,''))=''""",
            )
            missing_media = self._scalar(
                conn,
                """SELECT COUNT(DISTINCT l.listing_id)
                   FROM listings_v3 l
                   JOIN canonical_records c ON c.canonical_record_id=l.canonical_record_id
                   JOIN listing_offers o ON o.listing_id=l.listing_id
                   WHERE o.offer_type='sale' AND o.offer_status='active'
                     AND l.data_status='current'
                     AND NOT EXISTS (
                         SELECT 1 FROM media_assets m
                         WHERE m.owner_type='source_post'
                           AND CAST(m.owner_ref_id AS TEXT)=CAST(c.source_post_id AS TEXT)
                           AND m.asset_type='photo' AND m.status='active'
                           AND TRIM(COALESCE(m.local_path,''))<>''
                     )""",
            )
            dual_offer = self._scalar(
                conn,
                """SELECT COUNT(DISTINCT s.listing_id)
                   FROM listing_offers s
                   JOIN listing_offers r ON r.listing_id=s.listing_id
                   WHERE s.offer_type='sale' AND s.offer_status='active'
                     AND r.offer_type='rent' AND r.offer_status='active'""",
            )
            basic_ready = self._scalar(
                conn,
                """SELECT COUNT(DISTINCT l.listing_id)
                   FROM listings_v3 l
                   JOIN canonical_records c ON c.canonical_record_id=l.canonical_record_id
                   JOIN listing_offers o ON o.listing_id=l.listing_id
                   WHERE o.offer_type='sale' AND o.offer_status='active'
                     AND o.sale_price_usd>0
                     AND o.publication_policy='store_only' AND o.publishable=0
                     AND l.data_status='current'
                     AND l.inventory_status IN ('active','reserved')
                     AND TRIM(COALESCE(l.public_listing_id,''))<>''
                     AND (
                       TRIM(COALESCE(l.project_name,''))<>'' OR
                       TRIM(COALESCE(l.public_location_display,''))<>'' OR
                       TRIM(COALESCE(l.canonical_area_display,''))<>''
                     )
                     AND EXISTS (
                         SELECT 1 FROM media_assets m
                         WHERE m.owner_type='source_post'
                           AND CAST(m.owner_ref_id AS TEXT)=CAST(c.source_post_id AS TEXT)
                           AND m.asset_type='photo' AND m.status='active'
                           AND TRIM(COALESCE(m.local_path,''))<>''
                     )""",
            )
            bad_policy_ids = self._public_ids(
                conn,
                """SELECT l.public_listing_id
                   FROM listing_offers o
                   LEFT JOIN listings_v3 l ON l.listing_id=o.listing_id
                   WHERE o.offer_type='sale' AND o.offer_status='active'
                     AND (o.publication_policy<>'store_only' OR o.publishable<>0)
                   ORDER BY l.public_listing_id LIMIT ?""",
                limit=sample_limit,
            )
            incomplete_ids = self._public_ids(
                conn,
                """SELECT DISTINCT l.public_listing_id
                   FROM listings_v3 l
                   JOIN canonical_records c ON c.canonical_record_id=l.canonical_record_id
                   JOIN listing_offers o ON o.listing_id=l.listing_id
                   WHERE o.offer_type='sale' AND o.offer_status='active'
                     AND (
                       TRIM(COALESCE(l.public_listing_id,''))='' OR
                       (TRIM(COALESCE(l.project_name,''))='' AND TRIM(COALESCE(l.public_location_display,''))='' AND TRIM(COALESCE(l.canonical_area_display,''))='') OR
                       NOT EXISTS (
                         SELECT 1 FROM media_assets m
                         WHERE m.owner_type='source_post'
                           AND CAST(m.owner_ref_id AS TEXT)=CAST(c.source_post_id AS TEXT)
                           AND m.asset_type='photo' AND m.status='active'
                           AND TRIM(COALESCE(m.local_path,''))<>''
                       )
                     )
                   ORDER BY l.public_listing_id LIMIT ?""",
                limit=sample_limit,
            )
        return {
            "mode": "read_only",
            "active_sale_offers": active_sale,
            "basic_catalog_ready": basic_ready,
            "dual_rent_sale_listings": dual_offer,
            "issues": {
                "sale_policy_violations": policy_violations,
                "duplicate_active_sale_listings": duplicate_active_sale_listings,
                "missing_public_id": missing_public_id,
                "noncurrent_listing": noncurrent_listing,
                "unavailable_status": unavailable_status,
                "missing_project_or_location": missing_location,
                "missing_media": missing_media,
            },
            "samples": {
                "sale_policy_violations": bad_policy_ids,
                "incomplete_sale_listings": incomplete_ids,
            },
        }


__all__ = ["SaleInventoryAudit"]

"""Read-only sale inventory projection for the future Qiaolian property website.

The sale catalog deliberately reuses the shared V3 truth tables. It does not
create a second inventory, alter publication policy, or expose internal ids and
filesystem paths to clients.
"""
from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Any
from urllib.parse import quote


_BASE_SELECT = """
SELECT
    l.public_listing_id,
    l.project_name,
    l.display_title,
    l.public_location_display,
    l.canonical_area_display,
    l.property_type,
    l.property_subtype,
    l.layout,
    l.bedrooms,
    l.bathrooms,
    l.size_sqm,
    l.floor,
    l.inventory_status,
    l.updated_at,
    o.sale_price_usd,
    c.source_post_id
FROM listings_v3 l
JOIN listing_offers o ON o.listing_id=l.listing_id
JOIN canonical_records c ON c.canonical_record_id=l.canonical_record_id
WHERE o.offer_type='sale'
  AND o.offer_status='active'
  AND o.sale_price_usd IS NOT NULL
  AND o.sale_price_usd > 0
  AND l.data_status='current'
  AND l.inventory_status IN ('active','reserved')
  AND TRIM(COALESCE(l.public_listing_id,''))<>''
"""


class SaleCatalogRepository:
    """Read-only facade over sale offers in the shared V3 database."""

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
    def _positive_int(value: Any, *, default: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return parsed if parsed > 0 else default

    def _media_for_source(self, conn: sqlite3.Connection, source_post_id: Any) -> list[dict[str, str]]:
        try:
            source_pk = int(str(source_post_id))
        except (TypeError, ValueError):
            return []
        rows = conn.execute(
            """SELECT asset_id,local_path,mime_type
               FROM media_assets
               WHERE owner_type='source_post'
                 AND owner_ref_id=?
                 AND asset_type='photo'
                 AND status='active'
               ORDER BY is_cover DESC,sort_order ASC,id ASC""",
            (source_pk,),
        ).fetchall()
        result: list[dict[str, str]] = []
        seen: set[str] = set()
        for row in rows:
            asset_id = str(row["asset_id"] or "").strip()
            local_path = str(row["local_path"] or "").strip()
            if not asset_id or asset_id in seen or not local_path:
                continue
            if not Path(local_path).expanduser().is_file():
                continue
            seen.add(asset_id)
            result.append(
                {
                    "asset_id": asset_id,
                    "url": f"/api/v3/sale/media/{quote(asset_id, safe='')}",
                    "mime_type": str(row["mime_type"] or "image/jpeg"),
                }
            )
        return result

    @staticmethod
    def _public_row(row: sqlite3.Row, media: list[dict[str, str]]) -> dict[str, Any]:
        location = str(row["public_location_display"] or row["canonical_area_display"] or "").strip()
        project = str(row["project_name"] or "").strip()
        title = str(row["display_title"] or "").strip()
        if not title:
            parts = [part for part in (project, str(row["layout"] or "").strip()) if part]
            title = " · ".join(parts) or "金边置业房源"
        gallery = [item["url"] for item in media]
        return {
            "public_id": str(row["public_listing_id"]),
            "project": project,
            "title": title,
            "location": location,
            "property_type": str(row["property_type"] or "").strip(),
            "property_subtype": str(row["property_subtype"] or "").strip(),
            "layout": str(row["layout"] or "").strip(),
            "bedrooms": row["bedrooms"],
            "bathrooms": row["bathrooms"],
            "size_sqm": row["size_sqm"],
            "floor": str(row["floor"] or "").strip(),
            "sale_price_usd": int(row["sale_price_usd"]),
            "status": "可咨询" if str(row["inventory_status"]) == "active" else "已预留",
            "cover_url": gallery[0] if gallery else "",
            "gallery_urls": gallery,
            "updated_at": str(row["updated_at"] or ""),
        }

    def list_sale_listings(
        self,
        *,
        area: str = "",
        property_type: str = "",
        min_price: int | None = None,
        max_price: int | None = None,
        limit: int = 60,
        offset: int = 0,
    ) -> dict[str, Any]:
        clauses: list[str] = []
        params: list[Any] = []
        area = str(area or "").strip()
        property_type = str(property_type or "").strip()
        if area:
            clauses.append("AND (l.public_location_display LIKE ? OR l.canonical_area_display LIKE ?)")
            params.extend([f"%{area}%", f"%{area}%"])
        if property_type:
            clauses.append("AND l.property_type=?")
            params.append(property_type)
        if min_price is not None:
            clauses.append("AND o.sale_price_usd>=?")
            params.append(max(0, int(min_price)))
        if max_price is not None:
            clauses.append("AND o.sale_price_usd<=?")
            params.append(max(0, int(max_price)))

        limit = min(100, self._positive_int(limit, default=60))
        offset = max(0, int(offset or 0))
        where = "\n".join(clauses)
        with self._connect() as conn:
            count = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM ({_BASE_SELECT} {where})",
                    tuple(params),
                ).fetchone()[0]
            )
            rows = conn.execute(
                _BASE_SELECT
                + "\n"
                + where
                + "\nORDER BY l.updated_at DESC,l.created_at DESC,l.public_listing_id ASC LIMIT ? OFFSET ?",
                tuple(params + [limit, offset]),
            ).fetchall()
            items = [
                self._public_row(row, self._media_for_source(conn, row["source_post_id"]))
                for row in rows
            ]
        return {"items": items, "total": count, "limit": limit, "offset": offset}

    def get_sale_listing(self, public_id: str) -> dict[str, Any] | None:
        public_id = str(public_id or "").strip()
        if not public_id:
            return None
        with self._connect() as conn:
            row = conn.execute(
                _BASE_SELECT + "\nAND l.public_listing_id=? LIMIT 1",
                (public_id,),
            ).fetchone()
            if row is None:
                return None
            return self._public_row(row, self._media_for_source(conn, row["source_post_id"]))

    def media_asset(self, asset_id: str) -> tuple[Path, str] | None:
        """Resolve only media belonging to a currently visible sale listing."""
        asset_id = str(asset_id or "").strip()
        if not asset_id:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """SELECT DISTINCT m.local_path,m.mime_type
                   FROM media_assets m
                   JOIN source_posts s ON s.id=m.owner_ref_id
                   JOIN canonical_records c ON CAST(c.source_post_id AS TEXT)=CAST(s.id AS TEXT)
                   JOIN listings_v3 l ON l.canonical_record_id=c.canonical_record_id
                   JOIN listing_offers o ON o.listing_id=l.listing_id
                   WHERE m.asset_id=?
                     AND m.owner_type='source_post'
                     AND m.asset_type='photo'
                     AND m.status='active'
                     AND o.offer_type='sale'
                     AND o.offer_status='active'
                     AND o.sale_price_usd>0
                     AND l.data_status='current'
                     AND l.inventory_status IN ('active','reserved')
                     AND TRIM(COALESCE(l.public_listing_id,''))<>''
                   LIMIT 1""",
                (asset_id,),
            ).fetchone()
        if row is None:
            return None
        path = Path(str(row["local_path"] or "")).expanduser().resolve()
        if not path.is_file():
            return None
        return path, str(row["mime_type"] or "image/jpeg")


__all__ = ["SaleCatalogRepository"]

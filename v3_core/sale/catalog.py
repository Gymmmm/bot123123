"""Read-only public sale inventory over the shared V3 truth tables.

This module never creates schema, reparses source posts, changes offers, or
publishes anything. Sale inventory is produced by the existing V3 collector /
canonical / inventory pipeline. The catalog only projects safe public fields.
"""
from __future__ import annotations

from pathlib import Path
import sqlite3
from statistics import median
from typing import Any
from urllib.parse import quote


# Sale website is a showcase, not the rent confirmation workflow.
# pending / active / reserved all display; offline/sold stay hidden.
_PUBLIC_STATUS = {
    "pending": "在售",
    "active": "在售",
    "reserved": "已预留",
}

_SORT_SQL = {
    "newest": "l.updated_at DESC,l.created_at DESC,l.public_listing_id ASC",
    "price_asc": "o.sale_price_usd ASC,l.updated_at DESC,l.public_listing_id ASC",
    "price_desc": "o.sale_price_usd DESC,l.updated_at DESC,l.public_listing_id ASC",
    "area": "COALESCE(NULLIF(l.public_location_display,''),l.canonical_area_display) ASC,l.updated_at DESC",
}

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
    l.created_at,
    l.updated_at,
    o.sale_price_usd,
    c.source_post_id
FROM listings_v3 l
JOIN canonical_records c ON c.canonical_record_id=l.canonical_record_id
JOIN listing_offers o ON o.offer_id=(
    SELECT o2.offer_id
    FROM listing_offers o2
    WHERE o2.listing_id=l.listing_id
      AND o2.offer_type='sale'
      AND o2.offer_status='active'
      AND o2.publication_policy='store_only'
    ORDER BY o2.updated_at DESC,o2.created_at DESC,o2.offer_id DESC
    LIMIT 1
)
WHERE l.data_status='current'
  AND l.inventory_status IN ('pending','active','reserved')
  AND TRIM(COALESCE(l.public_listing_id,''))<>''

"""


class SaleCatalogRepository:
    """Read-only facade for public sale browsing."""

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

    @staticmethod
    def _status_key(value: str) -> str:
        value = str(value or "").strip().lower()
        if value in {"available", "active", "pending", "可咨询", "可售", "在售"}:
            # Showcase filter "在售" covers collected pending + active.
            return "showcase"
        if value in {"reserved", "hold", "已预留"}:
            return "reserved"
        return ""

    @staticmethod
    def _where(
        *,
        q: str,
        area: str,
        property_type: str,
        status: str,
        min_price: int | None,
        max_price: int | None,
    ) -> tuple[str, list[Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        q = str(q or "").strip()
        area = str(area or "").strip()
        property_type = str(property_type or "").strip()
        status_key = SaleCatalogRepository._status_key(status)
        if q:
            needle = f"%{q}%"
            clauses.append(
                "AND (l.public_listing_id LIKE ? OR l.project_name LIKE ? OR "
                "l.display_title LIKE ? OR l.public_location_display LIKE ? OR "
                "l.canonical_area_display LIKE ? OR l.layout LIKE ? OR l.property_type LIKE ?)"
            )
            params.extend([needle] * 7)
        if area:
            clauses.append("AND (l.public_location_display LIKE ? OR l.canonical_area_display LIKE ?)")
            params.extend([f"%{area}%", f"%{area}%"])
        if property_type:
            clauses.append("AND l.property_type=?")
            params.append(property_type)
        if status_key == "showcase":
            clauses.append("AND l.inventory_status IN ('pending','active')")
        elif status_key:
            clauses.append("AND l.inventory_status=?")
            params.append(status_key)
        if min_price is not None:
            clauses.append("AND o.sale_price_usd>=?")
            params.append(max(0, int(min_price)))
        if max_price is not None:
            clauses.append("AND o.sale_price_usd<=?")
            params.append(max(0, int(max_price)))
        return "\n".join(clauses), params

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
            mime_type = str(row["mime_type"] or "image/jpeg").strip().lower()
            if not asset_id or asset_id in seen or not local_path:
                continue
            if not mime_type.startswith("image/") or mime_type == "image/svg+xml":
                continue
            if not Path(local_path).expanduser().is_file():
                continue
            seen.add(asset_id)
            result.append(
                {
                    "asset_id": asset_id,
                    "url": f"/api/v3/sale/media/{quote(asset_id, safe='')}",
                    "mime_type": mime_type,
                }
            )
        return result

    @staticmethod
    def _public_row(row: sqlite3.Row, media: list[dict[str, str]]) -> dict[str, Any]:
        location = str(row["public_location_display"] or row["canonical_area_display"] or "").strip()
        project = str(row["project_name"] or "").strip()
        layout = str(row["layout"] or "").strip()
        title = str(row["display_title"] or "").strip()
        if not title:
            title = " · ".join(part for part in (project, layout) if part) or "金边置业房源"
        gallery = [item["url"] for item in media]
        return {
            "public_id": str(row["public_listing_id"]),
            "project": project,
            "title": title,
            "location": location,
            "property_type": str(row["property_type"] or "").strip(),
            "property_subtype": str(row["property_subtype"] or "").strip(),
            "layout": layout,
            "bedrooms": row["bedrooms"],
            "bathrooms": row["bathrooms"],
            "size_sqm": row["size_sqm"],
            "floor": str(row["floor"] or "").strip(),
            "sale_price_usd": None if row["sale_price_usd"] is None else int(row["sale_price_usd"]),
            "status": _PUBLIC_STATUS.get(str(row["inventory_status"]), "在售"),
            "cover_url": gallery[0] if gallery else "",
            "gallery_urls": gallery,
            "updated_at": str(row["updated_at"] or ""),
        }

    def list_sale_listings(
        self,
        *,
        q: str = "",
        area: str = "",
        property_type: str = "",
        status: str = "",
        min_price: int | None = None,
        max_price: int | None = None,
        sort: str = "newest",
        limit: int = 24,
        offset: int = 0,
    ) -> dict[str, Any]:
        where, params = self._where(
            q=q,
            area=area,
            property_type=property_type,
            status=status,
            min_price=min_price,
            max_price=max_price,
        )
        limit = min(60, self._positive_int(limit, default=24))
        offset = max(0, int(offset or 0))
        order_sql = _SORT_SQL.get(str(sort or "").strip().lower(), _SORT_SQL["newest"])
        with self._connect() as conn:
            count = int(conn.execute(f"SELECT COUNT(*) FROM ({_BASE_SELECT} {where})", tuple(params)).fetchone()[0])
            rows = conn.execute(
                _BASE_SELECT + "\n" + where + f"\nORDER BY {order_sql} LIMIT ? OFFSET ?",
                tuple(params + [limit, offset]),
            ).fetchall()
            items = [self._public_row(row, self._media_for_source(conn, row["source_post_id"])) for row in rows]
        return {
            "items": items,
            "total": count,
            "limit": limit,
            "offset": offset,
            "has_more": offset + len(items) < count,
        }

    def catalog_meta(self) -> dict[str, Any]:
        """Public-safe facets and availability counts for the sale page."""
        with self._connect() as conn:
            row = conn.execute(
                f"""SELECT COUNT(*) AS total,
                           SUM(CASE WHEN inventory_status IN ('pending','active') THEN 1 ELSE 0 END) AS available,
                           SUM(CASE WHEN inventory_status='reserved' THEN 1 ELSE 0 END) AS reserved,
                           MIN(sale_price_usd) AS min_price,
                           MAX(sale_price_usd) AS max_price
                    FROM ({_BASE_SELECT})"""
            ).fetchone()
            area_rows = conn.execute(
                f"""SELECT DISTINCT TRIM(COALESCE(NULLIF(public_location_display,''),canonical_area_display)) AS value
                    FROM ({_BASE_SELECT})
                    WHERE TRIM(COALESCE(NULLIF(public_location_display,''),canonical_area_display))<>''
                    ORDER BY value ASC LIMIT 100"""
            ).fetchall()
            type_rows = conn.execute(
                f"""SELECT DISTINCT TRIM(property_type) AS value
                    FROM ({_BASE_SELECT})
                    WHERE TRIM(COALESCE(property_type,''))<>''
                    ORDER BY value ASC LIMIT 50"""
            ).fetchall()
        return {
            "total": int(row["total"] or 0),
            "available": int(row["available"] or 0),
            "reserved": int(row["reserved"] or 0),
            "min_price_usd": None if row["min_price"] is None else int(row["min_price"]),
            "max_price_usd": None if row["max_price"] is None else int(row["max_price"]),
            "areas": [str(item["value"]) for item in area_rows if str(item["value"] or "").strip()],
            "property_types": [str(item["value"]) for item in type_rows if str(item["value"] or "").strip()],
        }

    @staticmethod
    def _sale_reference_from_rows(
        rows: list[sqlite3.Row],
        *,
        scope: str,
        label: str,
        current_price: int | None,
        current_size_sqm: float | None,
    ) -> dict[str, Any] | None:
        prices = sorted(
            int(row["sale_price_usd"])
            for row in rows
            if row["sale_price_usd"] is not None and int(row["sale_price_usd"]) > 0
        )
        if len(prices) < 2:
            return None

        ppsm_values: list[int] = []
        for row in rows:
            price = row["sale_price_usd"]
            size = row["size_sqm"]
            try:
                price_value = int(price)
                size_value = float(size)
            except (TypeError, ValueError):
                continue
            if price_value > 0 and size_value > 0:
                ppsm_values.append(int(round(price_value / size_value)))
        ppsm_values.sort()

        median_price = int(round(float(median(prices))))
        current_vs_median_pct: float | None = None
        if current_price and median_price > 0:
            current_vs_median_pct = round((int(current_price) - median_price) / median_price * 100, 1)

        current_ppsm: int | None = None
        if current_price and current_size_sqm:
            try:
                if float(current_size_sqm) > 0:
                    current_ppsm = int(round(int(current_price) / float(current_size_sqm)))
            except (TypeError, ValueError):
                current_ppsm = None

        return {
            "scope": scope,
            "label": label,
            "count": len(prices),
            "min_sale_price_usd": prices[0],
            "median_sale_price_usd": median_price,
            "max_sale_price_usd": prices[-1],
            "ppsm_count": len(ppsm_values),
            "min_price_per_sqm_usd": ppsm_values[0] if ppsm_values else None,
            "median_price_per_sqm_usd": int(round(float(median(ppsm_values)))) if ppsm_values else None,
            "max_price_per_sqm_usd": ppsm_values[-1] if ppsm_values else None,
            "current_price_per_sqm_usd": current_ppsm,
            "current_vs_median_pct": current_vs_median_pct,
            "basis": "current_public_sale_inventory",
        }

    def _sale_reference_for_row(
        self,
        conn: sqlite3.Connection,
        sale_row: sqlite3.Row,
    ) -> dict[str, Any] | None:
        """Compare only current public sale inventory; never reads rent offers."""
        project = str(sale_row["project_name"] or "").strip()
        location = str(
            sale_row["public_location_display"]
            or sale_row["canonical_area_display"]
            or ""
        ).strip()
        property_type = str(sale_row["property_type"] or "").strip()
        bedrooms = sale_row["bedrooms"]
        current_price = None if sale_row["sale_price_usd"] is None else int(sale_row["sale_price_usd"])
        current_size = sale_row["size_sqm"]

        base = """
            SELECT l.public_listing_id,l.project_name,l.public_location_display,
                   l.canonical_area_display,l.property_type,l.bedrooms,l.size_sqm,
                   l.updated_at,o.sale_price_usd
            FROM listings_v3 l
            JOIN listing_offers o ON o.offer_id=(
                SELECT o2.offer_id
                FROM listing_offers o2
                WHERE o2.listing_id=l.listing_id
                  AND o2.offer_type='sale'
                  AND o2.offer_status='active'
                  AND o2.publication_policy='store_only'
                ORDER BY o2.updated_at DESC,o2.created_at DESC,o2.offer_id DESC
                LIMIT 1
            )
            WHERE l.data_status='current'
              AND l.inventory_status IN ('pending','active','reserved')
              AND TRIM(COALESCE(l.public_listing_id,''))<>''
        """

        candidates: list[tuple[str, str, str, list[Any], int]] = []
        if project and bedrooms is not None:
            candidates.append((
                "same_project_layout",
                "同项目同户型",
                " AND TRIM(l.project_name)=? AND l.bedrooms=?",
                [project, int(bedrooms)],
                2,
            ))
        if project:
            candidates.append((
                "same_project",
                "同项目",
                " AND TRIM(l.project_name)=?",
                [project],
                2,
            ))
        if location and property_type and bedrooms is not None:
            candidates.append((
                "same_area_type_layout",
                "同区域同类型同户型",
                """ AND (l.public_location_display LIKE ? OR l.canonical_area_display LIKE ?)
                    AND l.property_type=? AND l.bedrooms=?""",
                [f"%{location}%", f"%{location}%", property_type, int(bedrooms)],
                3,
            ))
        if location and property_type:
            candidates.append((
                "same_area_type",
                "同区域同类型",
                """ AND (l.public_location_display LIKE ? OR l.canonical_area_display LIKE ?)
                    AND l.property_type=?""",
                [f"%{location}%", f"%{location}%", property_type],
                3,
            ))

        for scope, label, extra_sql, params, minimum in candidates:
            rows = conn.execute(
                base + extra_sql + " ORDER BY l.updated_at DESC LIMIT 80",
                tuple(params),
            ).fetchall()
            if len(rows) < minimum:
                continue
            reference = self._sale_reference_from_rows(
                list(rows),
                scope=scope,
                label=label,
                current_price=current_price,
                current_size_sqm=current_size,
            )
            if reference is not None:
                return reference
        return None

    def seo_entries(self) -> tuple[dict[str, str], ...]:
        """Canonical public sale URLs for sitemap generation."""
        with self._connect() as conn:
            rows = conn.execute(
                f"""SELECT public_listing_id,updated_at
                    FROM ({_BASE_SELECT})
                    ORDER BY updated_at DESC,public_listing_id ASC"""
            ).fetchall()
        return tuple(
            {"public_id": str(row["public_listing_id"] or "").strip(),
             "updated_at": str(row["updated_at"] or "").strip()}
            for row in rows
            if str(row["public_listing_id"] or "").strip()
        )

    def get_sale_listing(self, public_id: str) -> dict[str, Any] | None:
        public_id = str(public_id or "").strip().upper()
        if not public_id:
            return None
        with self._connect() as conn:
            row = conn.execute(_BASE_SELECT + "\nAND UPPER(l.public_listing_id)=? LIMIT 1", (public_id,)).fetchone()
            if row is None:
                return None
            media = self._media_for_source(conn, row["source_post_id"])
            item = self._public_row(row, media)
            item["sale_market_reference"] = self._sale_reference_for_row(conn, row)
            return item

    def media_asset(self, asset_id: str) -> tuple[Path, str] | None:
        """Resolve only image media belonging to a currently public sale listing."""
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
                     AND o.publication_policy='store_only'
                     AND l.data_status='current'
                     AND l.inventory_status IN ('pending','active','reserved')
                     AND TRIM(COALESCE(l.public_listing_id,''))<>''
                   LIMIT 1""",
                (asset_id,),
            ).fetchone()
        if row is None:
            return None
        mime_type = str(row["mime_type"] or "image/jpeg").strip().lower()
        if not mime_type.startswith("image/") or mime_type == "image/svg+xml":
            return None
        path = Path(str(row["local_path"] or "")).expanduser().resolve()
        if not path.is_file():
            return None
        return path, mime_type


__all__ = ["SaleCatalogRepository"]

"""Published-only renter search for the side-by-side V3 User Bot.

Search is deliberately stricter than public deep-link resolution: a historical
published listing may still expose details/photos, but renter search results must
be currently bookable. Therefore every search hit requires an active/reserved
listing, an active rent offer, the telegram_rent publication policy, an approved
or published frozen package, and a durable published Telegram publication
instance.

This module is read-only. It opens SQLite with ``mode=ro`` and never initializes
schema or writes user/lead state.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3

from .public_inventory import PublicInventoryReader, PublishedListingView
from .search_query import SearchCriteria
from .search_results import augment_strict_single_result


@dataclass(frozen=True)
class SearchExecution:
    items: tuple[PublishedListingView, ...]
    mode: str
    has_similar: bool = False


class PublicSearchReader:
    """Read current, durably published rent inventory without side effects."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()
        self.public_inventory = PublicInventoryReader(self.db_path)

    def _connect(self) -> sqlite3.Connection:
        if not self.db_path.is_file():
            raise FileNotFoundError(self.db_path)
        uri = f"file:{self.db_path.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def search(
        self,
        *,
        property_type: str = "",
        location_keys: tuple[str, ...] = (),
        budget_min: int | None = None,
        budget_max: int | None = None,
        limit: int = 3,
    ) -> tuple[PublishedListingView, ...]:
        clauses = [
            "l.inventory_status IN ('active','reserved')",
            "o.offer_type='rent'",
            "o.offer_status='active'",
            "o.publication_policy='telegram_rent'",
            "pi.platform='telegram'",
            "pi.publish_status='published'",
            "pp.status IN ('approved','published')",
        ]
        params: list[object] = []

        clean_type = str(property_type or "").strip()
        if clean_type:
            clauses.append("l.property_type=?")
            params.append(clean_type)

        clean_locations = tuple(
            dict.fromkeys(
                str(value or "").strip()
                for value in location_keys
                if str(value or "").strip()
            )
        )
        if clean_locations:
            placeholders = ",".join("?" for _ in clean_locations)
            clauses.append(
                f"(l.public_location_key IN ({placeholders}) "
                f"OR l.canonical_area_key IN ({placeholders}))"
            )
            params.extend(clean_locations)
            params.extend(clean_locations)

        if budget_min is not None:
            clauses.append("o.monthly_rent_usd>=?")
            params.append(int(budget_min))
        if budget_max is not None:
            clauses.append("o.monthly_rent_usd<=?")
            params.append(int(budget_max))

        cap = max(1, int(limit or 3))
        sql = (
            "SELECT l.public_listing_id, "
            "MAX(CASE l.inventory_status WHEN 'active' THEN 1 ELSE 0 END) AS active_rank, "
            "MAX(pi.updated_at) AS latest_publication "
            "FROM listings_v3 l "
            "JOIN listing_offers o ON o.listing_id=l.listing_id "
            "JOIN publication_instances pi "
            "  ON pi.listing_id=l.listing_id AND pi.offer_id=o.offer_id "
            "JOIN publication_packages_v3 pp "
            "  ON pp.package_id=pi.package_id "
            " AND pp.listing_id=l.listing_id "
            " AND pp.offer_id=o.offer_id "
            "WHERE "
            + " AND ".join(clauses)
            + " GROUP BY l.listing_id,l.public_listing_id "
            "ORDER BY active_rank DESC, latest_publication DESC, l.public_listing_id ASC "
            "LIMIT ?"
        )
        params.append(cap)

        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()

        items: list[PublishedListingView] = []
        for row in rows:
            public_id = str(row["public_listing_id"] or "").strip()
            if not public_id:
                continue
            view = self.public_inventory.resolve(public_id)
            if view is None or not view.bookable:
                continue
            items.append(view)
        return tuple(items)


class PublicSearchService:
    """Preserve fixed-SHA strict, single-result UX and explicit similar rules."""

    def __init__(self, reader: PublicSearchReader):
        self.reader = reader

    def strict(self, criteria: SearchCriteria, *, limit: int = 3) -> SearchExecution:
        items = self.reader.search(
            property_type=criteria.property_type,
            location_keys=criteria.location_keys,
            budget_min=criteria.budget_min,
            budget_max=criteria.budget_max,
            limit=limit,
        )
        return SearchExecution(items=items, mode="strict" if items else "no_match")

    def similar(self, criteria: SearchCriteria, *, limit: int = 3) -> SearchExecution:
        attempts = (
            (
                "no_type",
                dict(
                    location_keys=criteria.location_keys,
                    budget_min=criteria.budget_min,
                    budget_max=criteria.budget_max,
                ),
            ),
            (
                "no_area",
                dict(
                    property_type=criteria.property_type,
                    budget_min=criteria.budget_min,
                    budget_max=criteria.budget_max,
                ),
            ),
            (
                "budget_only",
                dict(
                    budget_min=criteria.budget_min,
                    budget_max=criteria.budget_max,
                ),
            ),
        )
        for mode, kwargs in attempts:
            items = self.reader.search(limit=limit, **kwargs)
            if items:
                return SearchExecution(items=items, mode=mode)
        return SearchExecution(items=(), mode="no_match")

    def strict_for_results(
        self,
        criteria: SearchCriteria,
        *,
        limit: int = 5,
    ) -> SearchExecution:
        """Apply the production one-result carousel enhancement only after a hit.

        A strict no-match stays a no-match. When strict returns exactly one
        listing, the same explicit-similar search policy is queried and unique
        published views may be appended so previous/next navigation remains
        useful. Tracking mode remains ``strict`` because the first result is the
        actual strict match.
        """
        strict = self.strict(criteria, limit=limit)
        if strict.mode != "strict" or len(strict.items) != 1:
            return strict

        similar = self.similar(criteria, limit=limit)
        augmented = augment_strict_single_result(
            strict.items,
            similar.items,
            match_mode="strict",
            limit=limit,
        )
        return SearchExecution(
            items=tuple(augmented.items),
            mode="strict",
            has_similar=augmented.has_similar,
        )


__all__ = [
    "PublicSearchReader",
    "PublicSearchService",
    "SearchExecution",
]

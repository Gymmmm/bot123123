"""Strict read-only public inventory view for the V3 User Bot.

A public listing is resolvable only when it is backed by a durable published
Telegram publication instance for a rent offer.  Guessing a public QL id must
not expose inventory that has never crossed the approved publication boundary.
The adapter opens SQLite with ``mode=ro`` and never initializes schema.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3
from typing import Any

from v3_core.publishing.public_ids import normalize_public_id


@dataclass(frozen=True)
class PublishedListingView:
    listing: dict[str, Any]
    offer: dict[str, Any]
    publication: dict[str, Any]
    package: dict[str, Any]

    @property
    def public_listing_id(self) -> str:
        return str(self.listing.get("public_listing_id") or "")

    @property
    def listing_id(self) -> str:
        return str(self.listing.get("listing_id") or "")

    @property
    def gallery(self) -> tuple[str, ...]:
        raw = self.package.get("gallery_json")
        try:
            value = json.loads(str(raw or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            return ()
        if not isinstance(value, list):
            return ()
        return tuple(str(item) for item in value if str(item or "").strip())

    @property
    def bookable(self) -> bool:
        listing_status = str(self.listing.get("inventory_status") or "").lower()
        offer_status = str(self.offer.get("offer_status") or "").lower()
        return (
            listing_status in {"active", "reserved"}
            and offer_status == "active"
            and str(self.offer.get("offer_type") or "").lower() == "rent"
            and str(self.offer.get("publication_policy") or "") == "telegram_rent"
        )

    def action_allowed(self, action: str) -> bool:
        clean = str(action or "").strip().lower()
        if clean == "book":
            return self.bookable
        if clean == "details":
            return True
        if clean == "photos":
            return bool(self.gallery)
        return False


class PublicInventoryReader:
    """Read published V3 rent inventory without any schema/file side effects."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self) -> sqlite3.Connection:
        if not self.db_path.is_file():
            raise FileNotFoundError(self.db_path)
        uri = f"file:{self.db_path.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def resolve(self, public_listing_id: object) -> PublishedListingView | None:
        public_id = normalize_public_id(public_listing_id)
        if public_id is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """SELECT
                       l.listing_id AS _listing_id,
                       o.offer_id AS _offer_id,
                       pi.instance_id AS _instance_id,
                       pi.package_id AS _package_id
                   FROM listings_v3 l
                   JOIN listing_offers o
                     ON o.listing_id=l.listing_id
                    AND o.offer_type='rent'
                    AND o.publication_policy='telegram_rent'
                   JOIN publication_instances pi
                     ON pi.listing_id=l.listing_id
                    AND pi.offer_id=o.offer_id
                    AND pi.platform='telegram'
                    AND pi.publish_status='published'
                   JOIN publication_packages_v3 pp
                     ON pp.package_id=pi.package_id
                    AND pp.listing_id=l.listing_id
                    AND pp.offer_id=o.offer_id
                    AND pp.status IN ('approved','published')
                   WHERE l.public_listing_id=?
                   ORDER BY pi.updated_at DESC,pi.id DESC
                   LIMIT 1""",
                (public_id,),
            ).fetchone()
            if row is None:
                return None
            listing = conn.execute(
                "SELECT * FROM listings_v3 WHERE listing_id=?",
                (str(row["_listing_id"]),),
            ).fetchone()
            offer = conn.execute(
                "SELECT * FROM listing_offers WHERE offer_id=?",
                (str(row["_offer_id"]),),
            ).fetchone()
            publication = conn.execute(
                "SELECT * FROM publication_instances WHERE instance_id=?",
                (str(row["_instance_id"]),),
            ).fetchone()
            package = conn.execute(
                "SELECT * FROM publication_packages_v3 WHERE package_id=?",
                (str(row["_package_id"]),),
            ).fetchone()
        if not all((listing, offer, publication, package)):
            return None
        return PublishedListingView(
            listing=dict(listing),
            offer=dict(offer),
            publication=dict(publication),
            package=dict(package),
        )


__all__ = ["PublicInventoryReader", "PublishedListingView"]

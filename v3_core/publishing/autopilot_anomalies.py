"""No-offer anomaly support for the production V3 autopilot.

Canonical materialization deliberately creates no rent offer when rent is
missing.  Those review rows still need an operator-visible exception without
inventing a fake price or a second inventory pipeline.
"""
from __future__ import annotations

import json
from typing import Any

from .autopilot import ERROR_LABELS
from .autopilot_policy import ProductionAutoPublishRepository, ProductionAutoPublishService


DDL = """
CREATE TABLE IF NOT EXISTS publisher_review_exceptions_v3 (
    review_id TEXT PRIMARY KEY,
    listing_id TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    reason_text TEXT NOT NULL,
    ignored INTEGER NOT NULL DEFAULT 0 CHECK(ignored IN (0,1)),
    resolved INTEGER NOT NULL DEFAULT 0 CHECK(resolved IN (0,1)),
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_publisher_review_exceptions_v3_open
ON publisher_review_exceptions_v3(resolved,ignored,updated_at);
"""


def _positive(value: Any) -> bool:
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


class FinalAutoPublishRepository(ProductionAutoPublishRepository):
    """Production repository including reviews that have no materialized offer."""

    @staticmethod
    def _no_offer_reason(canonical: dict[str, Any], listing: dict[str, Any]) -> str:
        try:
            facts = json.loads(str(canonical.get("facts_json") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            facts = {}
        deal_type = str(canonical.get("deal_type") or facts.get("deal_type") or "").lower()
        if deal_type == "sale":
            return "sale_store_only"
        if not _positive(facts.get("monthly_rent_usd")):
            return "missing_rent"
        project = str(listing.get("project_name") or facts.get("project_name") or "").strip()
        location = str(
            listing.get("public_location_display")
            or facts.get("public_location_display")
            or ""
        ).strip()
        if not project and not location:
            return "missing_location"
        return "missing_listing_info"

    def sync_review_exceptions(self) -> int:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """SELECT r.review_id,r.listing_id,c.facts_json,c.deal_type,
                          l.project_name,l.public_location_display
                   FROM review_items r
                   JOIN listings_v3 l ON l.listing_id=r.listing_id
                   JOIN canonical_records c ON c.canonical_record_id=r.canonical_record_id
                   WHERE COALESCE(r.offer_id,'')=''"""
            ).fetchall()
            seen: set[str] = set()
            for row in rows:
                review_id = str(row["review_id"])
                seen.add(review_id)
                active_offer = conn.execute(
                    "SELECT 1 FROM listing_offers WHERE listing_id=? AND offer_status='active' LIMIT 1",
                    (str(row["listing_id"]),),
                ).fetchone()
                if active_offer is not None:
                    conn.execute(
                        """INSERT INTO publisher_review_exceptions_v3
                           (review_id,listing_id,reason_code,reason_text,resolved,updated_at)
                           VALUES (?,?, '', '',1,CURRENT_TIMESTAMP)
                           ON CONFLICT(review_id) DO UPDATE SET resolved=1,updated_at=CURRENT_TIMESTAMP""",
                        (review_id, str(row["listing_id"])),
                    )
                    continue
                reason = self._no_offer_reason(dict(row), dict(row))
                conn.execute(
                    """INSERT INTO publisher_review_exceptions_v3
                       (review_id,listing_id,reason_code,reason_text,resolved,updated_at)
                       VALUES (?,?,?,?,0,CURRENT_TIMESTAMP)
                       ON CONFLICT(review_id) DO UPDATE SET
                         listing_id=excluded.listing_id,reason_code=excluded.reason_code,
                         reason_text=excluded.reason_text,resolved=0,updated_at=CURRENT_TIMESTAMP""",
                    (
                        review_id,
                        str(row["listing_id"]),
                        reason,
                        ERROR_LABELS.get(reason, reason),
                    ),
                )
            if seen:
                placeholders = ",".join("?" for _ in seen)
                conn.execute(
                    f"UPDATE publisher_review_exceptions_v3 SET resolved=1,updated_at=CURRENT_TIMESTAMP WHERE review_id NOT IN ({placeholders})",
                    tuple(sorted(seen)),
                )
            else:
                conn.execute(
                    "UPDATE publisher_review_exceptions_v3 SET resolved=1,updated_at=CURRENT_TIMESTAMP"
                )
            conn.commit()
        return len(rows)

    def sync_candidates(self) -> int:
        count = super().sync_candidates()
        self.sync_review_exceptions()
        return count

    def exception_count(self) -> int:
        self.sync_review_exceptions()
        with self._connect() as conn:
            auto_count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM publisher_auto_items_v3 WHERE state='exception' AND ignored=0"
                ).fetchone()[0]
            )
            review_count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM publisher_review_exceptions_v3 WHERE resolved=0 AND ignored=0"
                ).fetchone()[0]
            )
        return auto_count + review_count

    def exception_rows(self, *, limit: int = 20) -> list[dict[str, Any]]:
        self.sync_review_exceptions()
        rows = list(super().exception_rows(limit=limit))
        with self._connect() as conn:
            extra = conn.execute(
                """SELECT e.review_id,e.listing_id,e.reason_code,e.reason_text,e.updated_at,
                          l.public_listing_id,l.display_title,l.project_name
                   FROM publisher_review_exceptions_v3 e
                   JOIN listings_v3 l ON l.listing_id=e.listing_id
                   WHERE e.resolved=0 AND e.ignored=0
                   ORDER BY e.updated_at DESC LIMIT ?""",
                (max(1, int(limit)),),
            ).fetchall()
        for row in extra:
            item = dict(row)
            item["offer_id"] = "review:" + str(row["review_id"])
            rows.append(item)
        rows.sort(key=lambda value: str(value.get("updated_at") or ""), reverse=True)
        return rows[: max(1, int(limit))]

    def review_exception_detail(self, token: str) -> dict[str, Any]:
        review_id = str(token).removeprefix("review:")
        self.sync_review_exceptions()
        with self._connect() as conn:
            row = conn.execute(
                """SELECT e.*,r.canonical_record_id,c.source_post_id,
                          l.public_listing_id,l.display_title,l.project_name
                   FROM publisher_review_exceptions_v3 e
                   JOIN review_items r ON r.review_id=e.review_id
                   JOIN listings_v3 l ON l.listing_id=e.listing_id
                   JOIN canonical_records c ON c.canonical_record_id=r.canonical_record_id
                   WHERE e.review_id=? AND e.resolved=0""",
                (review_id,),
            ).fetchone()
        if row is None:
            raise KeyError(token)
        data = dict(row)
        data["offer_id"] = "review:" + review_id
        return data

    def find_rent_offer_for_listing(self, listing_id: str) -> str:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT offer_id FROM listing_offers
                   WHERE listing_id=? AND offer_type='rent' AND offer_status='active'
                   ORDER BY created_at DESC LIMIT 1""",
                (str(listing_id),),
            ).fetchone()
        return str(row["offer_id"]) if row else ""

    def ignore(self, offer_id: str) -> None:
        token = str(offer_id)
        if token.startswith("review:"):
            review_id = token.removeprefix("review:")
            with self._connect() as conn:
                conn.execute(
                    "UPDATE publisher_review_exceptions_v3 SET ignored=1,updated_at=CURRENT_TIMESTAMP WHERE review_id=?",
                    (review_id,),
                )
                conn.commit()
            return
        super().ignore(token)


class FinalAutoPublishService(ProductionAutoPublishService):
    repository: FinalAutoPublishRepository


__all__ = ["DDL", "FinalAutoPublishRepository", "FinalAutoPublishService"]

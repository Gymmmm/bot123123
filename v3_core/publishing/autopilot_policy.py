"""Production policy hardening for the V3 autopilot.

This layer adds only candidate-version tracking and an exact canonical duplicate
guard.  Publication still delegates to ``AutoPublishService`` and therefore to
the existing frozen-package/delivery coordinator chain.
"""
from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Any

from .autopilot import (
    AutoPublishRepository,
    AutoPublishResult,
    AutoPublishService,
    ERROR_LABELS,
)


DDL = """
CREATE TABLE IF NOT EXISTS publisher_auto_versions_v3 (
    offer_id TEXT PRIMARY KEY,
    canonical_facts_hash TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class ProductionAutoPublishRepository(AutoPublishRepository):
    """Autopilot state with canonical-revision awareness and exact dedupe."""

    def sync_candidates(self) -> int:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """SELECT r.review_id,r.review_status,r.listing_id,r.offer_id,
                          o.offer_type,l.canonical_facts_hash
                   FROM review_items r
                   JOIN listing_offers o ON o.offer_id=r.offer_id
                   JOIN listings_v3 l ON l.listing_id=r.listing_id
                   WHERE o.offer_status='active' AND r.offer_id<>''"""
            ).fetchall()
            for row in rows:
                offer_id = str(row["offer_id"])
                offer_type = str(row["offer_type"])
                current_hash = str(row["canonical_facts_hash"] or "")
                previous = conn.execute(
                    "SELECT canonical_facts_hash FROM publisher_auto_versions_v3 WHERE offer_id=?",
                    (offer_id,),
                ).fetchone()
                previous_hash = str(previous["canonical_facts_hash"] or "") if previous else ""
                changed = bool(previous is not None and previous_hash != current_hash)
                existing = conn.execute(
                    "SELECT state,ignored FROM publisher_auto_items_v3 WHERE offer_id=?",
                    (offer_id,),
                ).fetchone()
                reason_code = "sale_store_only" if offer_type == "sale" else ""
                reason_text = ERROR_LABELS.get(reason_code, "")
                if existing is None:
                    state = "exception" if reason_code else "queued"
                    conn.execute(
                        """INSERT INTO publisher_auto_items_v3
                           (offer_id,listing_id,review_id,state,reason_code,reason_text,origin)
                           VALUES (?,?,?,?,?,?,'collector')""",
                        (
                            offer_id,
                            str(row["listing_id"]),
                            str(row["review_id"]),
                            state,
                            reason_code,
                            reason_text,
                        ),
                    )
                else:
                    current_state = str(existing["state"] or "")
                    ignored = bool(int(existing["ignored"] or 0))
                    if offer_type == "sale" and current_state != "published":
                        state = "exception"
                        code = "sale_store_only"
                        text = ERROR_LABELS[code]
                    elif current_state == "published" or ignored:
                        state = current_state
                        code = ""
                        text = ""
                    elif changed:
                        # A source edit created a new canonical projection.  It
                        # must be checked again rather than staying stuck on an
                        # exception raised for the old facts.
                        state = "queued"
                        code = ""
                        text = ""
                    else:
                        state = current_state or "queued"
                        code = None
                        text = None
                    if code is None:
                        conn.execute(
                            """UPDATE publisher_auto_items_v3 SET listing_id=?,review_id=?,
                               state=?,updated_at=CURRENT_TIMESTAMP WHERE offer_id=?""",
                            (str(row["listing_id"]), str(row["review_id"]), state, offer_id),
                        )
                    else:
                        conn.execute(
                            """UPDATE publisher_auto_items_v3 SET listing_id=?,review_id=?,
                               state=?,reason_code=?,reason_text=?,updated_at=CURRENT_TIMESTAMP
                               WHERE offer_id=?""",
                            (str(row["listing_id"]), str(row["review_id"]), state, code, text, offer_id),
                        )
                conn.execute(
                    """INSERT INTO publisher_auto_versions_v3(offer_id,canonical_facts_hash,updated_at)
                       VALUES (?,?,CURRENT_TIMESTAMP)
                       ON CONFLICT(offer_id) DO UPDATE SET
                         canonical_facts_hash=excluded.canonical_facts_hash,
                         updated_at=CURRENT_TIMESTAMP""",
                    (offer_id, current_hash),
                )
            conn.commit()
        return len(rows)

    def probable_duplicate(self, listing_id: str, offer_id: str) -> bool:
        """Block another active rental projected from the exact same facts.

        Same-source revisions reuse the same listing identity, so this catches
        cross-source exact duplicates without guessing from only price/layout.
        """
        with self._connect() as conn:
            current = conn.execute(
                """SELECT l.canonical_facts_hash FROM listings_v3 l
                   JOIN listing_offers o ON o.listing_id=l.listing_id
                   WHERE l.listing_id=? AND o.offer_id=? AND o.offer_type='rent'""",
                (str(listing_id), str(offer_id)),
            ).fetchone()
            if current is None or not str(current["canonical_facts_hash"] or ""):
                return False
            row = conn.execute(
                """SELECT 1 FROM listings_v3 l
                   JOIN listing_offers o ON o.listing_id=l.listing_id
                   WHERE l.listing_id<>? AND l.canonical_facts_hash=?
                     AND l.inventory_status IN ('active','reserved')
                     AND o.offer_type='rent' AND o.offer_status='active'
                   LIMIT 1""",
                (str(listing_id), str(current["canonical_facts_hash"])),
            ).fetchone()
        return row is not None


class ProductionAutoPublishService(AutoPublishService):
    repository: ProductionAutoPublishRepository

    async def process_one(
        self,
        *,
        bot: Any,
        force_offer_id: str = "",
        origin: str | None = None,
    ) -> AutoPublishResult:
        self.repository.sync_candidates()
        if force_offer_id:
            with self.repository._connect() as conn:
                row = conn.execute(
                    "SELECT listing_id FROM publisher_auto_items_v3 WHERE offer_id=?",
                    (str(force_offer_id),),
                ).fetchone()
            if row is not None and self.repository.probable_duplicate(
                str(row["listing_id"]), str(force_offer_id)
            ):
                text = ERROR_LABELS["duplicate_listing"]
                self.repository.set_item(
                    str(force_offer_id),
                    state="exception",
                    reason_code="duplicate_listing",
                    reason_text=text,
                )
                return AutoPublishResult(
                    "exception",
                    offer_id=str(force_offer_id),
                    listing_id=str(row["listing_id"]),
                    reason_code="duplicate_listing",
                    reason_text=text,
                )
        else:
            item = self.repository.next_item()
            if item is not None and self.repository.probable_duplicate(
                str(item["listing_id"]), str(item["offer_id"])
            ):
                text = ERROR_LABELS["duplicate_listing"]
                self.repository.set_item(
                    str(item["offer_id"]),
                    state="exception",
                    reason_code="duplicate_listing",
                    reason_text=text,
                )
                return AutoPublishResult(
                    "exception",
                    offer_id=str(item["offer_id"]),
                    listing_id=str(item["listing_id"]),
                    reason_code="duplicate_listing",
                    reason_text=text,
                )
        return await super().process_one(
            bot=bot,
            force_offer_id=force_offer_id,
            origin=origin,
        )


__all__ = [
    "DDL",
    "ProductionAutoPublishRepository",
    "ProductionAutoPublishService",
]

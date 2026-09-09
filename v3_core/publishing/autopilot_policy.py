"""Production policy hardening for the V3 autopilot.

This layer adds candidate-version tracking, exact canonical duplicate protection,
and validation that continues while channel delivery is paused or outside the
posting window. Publication still delegates to ``AutoPublishService`` and
therefore to the existing frozen-package/delivery coordinator chain.
"""
from __future__ import annotations

import asyncio
from datetime import datetime
import os
from typing import Any
import uuid

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
CREATE TABLE IF NOT EXISTS publisher_auto_validations_v3 (
    offer_id TEXT PRIMARY KEY,
    revision_token TEXT NOT NULL DEFAULT '',
    validation_status TEXT NOT NULL DEFAULT 'ready',
    validated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_publisher_auto_validations_v3_status
ON publisher_auto_validations_v3(validation_status,validated_at);
"""


_CANDIDATE_SELECT = """
SELECT a.*,r.review_status,o.offer_type,o.publication_policy,o.offer_status,
       o.monthly_rent_usd,l.inventory_status,l.public_listing_id,l.project_name,
       l.public_location_display,l.layout,l.property_type,l.canonical_record_id,
       l.canonical_facts_hash,
       (COALESCE(l.canonical_facts_hash,'') || ':' || COALESCE(s.dedupe_hash,'')) AS revision_token
FROM publisher_auto_items_v3 a
JOIN review_items r ON r.review_id=a.review_id
JOIN listing_offers o ON o.offer_id=a.offer_id
JOIN listings_v3 l ON l.listing_id=a.listing_id
LEFT JOIN canonical_records c ON c.canonical_record_id=l.canonical_record_id
LEFT JOIN source_posts s ON CAST(s.id AS TEXT)=CAST(c.source_post_id AS TEXT)
"""


class ProductionAutoPublishRepository(AutoPublishRepository):
    """Autopilot state with canonical/media revision awareness and exact dedupe."""

    @staticmethod
    def _token(facts_hash: Any, source_hash: Any) -> str:
        return f"{str(facts_hash or '')}:{str(source_hash or '')}"

    def ensure_defaults(self) -> None:
        """Apply environment defaults only once; DB changes win after that."""
        defaults = {
            "enabled": os.getenv("V3_AUTO_PUBLISH_ENABLED", "true"),
            "min_media": os.getenv("V3_AUTO_PUBLISH_MIN_MEDIA", "4"),
            "timezone": os.getenv("V3_AUTO_PUBLISH_TIMEZONE", "Asia/Phnom_Penh"),
            "interval_seconds": os.getenv("V3_AUTO_PUBLISH_INTERVAL_SECONDS", "1800"),
        }
        default_windows = str(
            os.getenv("V3_AUTO_PUBLISH_WINDOWS", "09:00-21:00")
        ).strip()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            marker = conn.execute(
                "SELECT 1 FROM publisher_autopilot_settings_v3 WHERE setting_key='windows_initialized'"
            ).fetchone()
            for key, value in defaults.items():
                conn.execute(
                    "INSERT OR IGNORE INTO publisher_autopilot_settings_v3(setting_key,setting_value) VALUES (?,?)",
                    (key, str(value)),
                )
            conn.execute(
                "INSERT OR IGNORE INTO publisher_autopilot_settings_v3(setting_key,setting_value) VALUES ('windows_initialized','1')"
            )
            if marker is None:
                count = int(
                    conn.execute("SELECT COUNT(*) FROM publisher_post_windows_v3").fetchone()[0]
                )
                if count == 0:
                    for part in default_windows.split(","):
                        if "-" not in part:
                            continue
                        start, end = [x.strip() for x in part.split("-", 1)]
                        try:
                            start = datetime.strptime(start, "%H:%M").strftime("%H:%M")
                            end = datetime.strptime(end, "%H:%M").strftime("%H:%M")
                        except ValueError:
                            continue
                        if start >= end:
                            continue
                        conn.execute(
                            "INSERT INTO publisher_post_windows_v3(window_id,start_time,end_time,enabled) VALUES (?,?,?,1)",
                            ("WIN_" + uuid.uuid4().hex[:16], start, end),
                        )
            conn.commit()

    def sync_candidates(self) -> int:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """SELECT r.review_id,r.review_status,r.listing_id,r.offer_id,
                          o.offer_type,l.canonical_facts_hash,s.dedupe_hash AS source_dedupe_hash
                   FROM review_items r
                   JOIN listing_offers o ON o.offer_id=r.offer_id
                   JOIN listings_v3 l ON l.listing_id=r.listing_id
                   LEFT JOIN canonical_records c ON c.canonical_record_id=l.canonical_record_id
                   LEFT JOIN source_posts s ON CAST(s.id AS TEXT)=CAST(c.source_post_id AS TEXT)
                   WHERE o.offer_status='active' AND r.offer_id<>''"""
            ).fetchall()
            for row in rows:
                offer_id = str(row["offer_id"])
                offer_type = str(row["offer_type"])
                current_token = self._token(
                    row["canonical_facts_hash"], row["source_dedupe_hash"]
                )
                previous = conn.execute(
                    "SELECT canonical_facts_hash FROM publisher_auto_versions_v3 WHERE offer_id=?",
                    (offer_id,),
                ).fetchone()
                previous_token = str(previous["canonical_facts_hash"] or "") if previous else ""
                changed = bool(previous is not None and previous_token != current_token)
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
                        state, code, text = "exception", "sale_store_only", ERROR_LABELS["sale_store_only"]
                    elif current_state == "published" or ignored:
                        state, code, text = current_state, "", ""
                    elif changed:
                        state, code, text = "queued", "", ""
                    else:
                        state, code, text = current_state or "queued", None, None
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
                    (offer_id, current_token),
                )
            conn.commit()
        return len(rows)

    def _candidate(self, suffix: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(_CANDIDATE_SELECT + suffix, params).fetchone()
        return dict(row) if row else None

    def candidate_for_offer(self, offer_id: str) -> dict[str, Any] | None:
        return self._candidate(" WHERE a.offer_id=? LIMIT 1", (str(offer_id),))

    def next_unvalidated_item(self) -> dict[str, Any] | None:
        return self._candidate(
            """ LEFT JOIN publisher_auto_validations_v3 v ON v.offer_id=a.offer_id
                WHERE a.state='queued' AND a.ignored=0
                  AND (v.offer_id IS NULL OR v.revision_token<>(COALESCE(l.canonical_facts_hash,'') || ':' || COALESCE(s.dedupe_hash,''))
                       OR v.validation_status<>'ready')
                ORDER BY a.created_at ASC,a.offer_id ASC LIMIT 1"""
        )

    def next_ready_item(self) -> dict[str, Any] | None:
        return self._candidate(
            """ JOIN publisher_auto_validations_v3 v ON v.offer_id=a.offer_id
                WHERE a.state='queued' AND a.ignored=0
                  AND v.validation_status='ready'
                  AND v.revision_token=(COALESCE(l.canonical_facts_hash,'') || ':' || COALESCE(s.dedupe_hash,''))
                ORDER BY a.created_at ASC,a.offer_id ASC LIMIT 1"""
        )

    def mark_validated(self, offer_id: str, revision_token: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO publisher_auto_validations_v3
                   (offer_id,revision_token,validation_status,validated_at)
                   VALUES (?,?,'ready',CURRENT_TIMESTAMP)
                   ON CONFLICT(offer_id) DO UPDATE SET
                     revision_token=excluded.revision_token,
                     validation_status='ready',validated_at=CURRENT_TIMESTAMP""",
                (str(offer_id), str(revision_token)),
            )
            conn.commit()

    def probable_duplicate(self, listing_id: str, offer_id: str) -> bool:
        """Block later active rentals projected from the exact same facts."""
        with self._connect() as conn:
            current = conn.execute(
                """SELECT l.canonical_facts_hash FROM listings_v3 l
                   JOIN listing_offers o ON o.listing_id=l.listing_id
                   WHERE l.listing_id=? AND o.offer_id=? AND o.offer_type='rent'""",
                (str(listing_id), str(offer_id)),
            ).fetchone()
            if current is None or not str(current["canonical_facts_hash"] or ""):
                return False
            winner = conn.execute(
                """SELECT l.listing_id FROM listings_v3 l
                   JOIN listing_offers o ON o.listing_id=l.listing_id
                   WHERE l.canonical_facts_hash=?
                     AND l.inventory_status IN ('active','reserved')
                     AND o.offer_type='rent' AND o.offer_status='active'
                   ORDER BY l.created_at ASC,
                     CASE WHEN l.listing_id GLOB 'l_[0-9]*'
                          THEN CAST(SUBSTR(l.listing_id,3) AS INTEGER)
                          ELSE 2147483647 END ASC,
                     l.listing_id ASC
                   LIMIT 1""",
                (str(current["canonical_facts_hash"]),),
            ).fetchone()
        return winner is not None and str(winner["listing_id"]) != str(listing_id)


class ProductionAutoPublishService(AutoPublishService):
    repository: ProductionAutoPublishRepository

    async def _validate_item(self, item: dict[str, Any]) -> AutoPublishResult:
        offer_id = str(item["offer_id"])
        listing_id = str(item["listing_id"])

        existing = self.repository.publication_for_offer(offer_id, self.channel_chat_id)
        if existing:
            self.repository.set_item(
                offer_id,
                state="published",
                channel_message_id=str(existing.get("channel_message_id") or ""),
            )
            return AutoPublishResult(
                "already_published",
                offer_id=offer_id,
                listing_id=listing_id,
                channel_message_id=str(existing.get("channel_message_id") or ""),
            )

        unsafe = self.repository.unsafe_delivery_state(offer_id, self.channel_chat_id)
        if unsafe in {"sending", "sent", "unknown"}:
            return self._mark_exception(offer_id, "telegram_unknown")

        if self.repository.probable_duplicate(listing_id, offer_id):
            return self._mark_exception(offer_id, "duplicate_listing")

        detail = self.workflow.review_detail(str(item["review_id"]))
        if str(detail.review.get("review_status") or "") in {"hold", "rejected"}:
            return self._mark_exception(offer_id, "admin_hold")
        facts = dict(detail.canonical.get("facts") or {})
        try:
            media = await asyncio.to_thread(
                self.workflow.review_media, review_id=str(item["review_id"])
            )
        except Exception:
            return self._mark_exception(offer_id, "unreadable_media")
        blockers = self._strict_blockers(item, facts, media)
        if blockers:
            return self._mark_exception(offer_id, blockers[0])

        self.repository.mark_validated(offer_id, str(item.get("revision_token") or ""))
        return AutoPublishResult("ready", offer_id=offer_id, listing_id=listing_id)

    async def validate_pending(self, *, limit: int = 20) -> int:
        """Classify new/revised candidates even while sending is paused."""
        self.repository.sync_candidates()
        checked = 0
        for _ in range(max(1, int(limit))):
            item = self.repository.next_unvalidated_item()
            if item is None:
                break
            await self._validate_item(item)
            checked += 1
        return checked

    async def process_one(
        self,
        *,
        bot: Any,
        force_offer_id: str = "",
        origin: str | None = None,
    ) -> AutoPublishResult:
        self.repository.sync_candidates()
        if force_offer_id:
            self.repository.requeue(force_offer_id)
            item = self.repository.candidate_for_offer(force_offer_id)
            if item is None:
                return AutoPublishResult("idle")
            checked = await self._validate_item(item)
            if checked.status != "ready":
                return checked
            return await super().process_one(
                bot=bot,
                force_offer_id=str(force_offer_id),
                origin=origin,
            )

        item = self.repository.next_ready_item()
        if item is None:
            return AutoPublishResult("idle")
        return await super().process_one(
            bot=bot,
            force_offer_id=str(item["offer_id"]),
            origin=origin,
        )

    async def scheduled_tick(self, context: Any) -> None:
        self.repository.heartbeat("publisher", state="running")
        await self.validate_pending(limit=20)
        cfg = self.repository.config()
        if not cfg.enabled or not self.repository.within_window():
            return
        if not self.repository.interval_ready(cfg.interval_seconds):
            return
        if not self.repository.acquire_lock(
            self.owner, ttl_seconds=max(180, cfg.interval_seconds)
        ):
            return
        try:
            await self.process_one(bot=context.bot)
        finally:
            self.repository.release_lock(self.owner)


__all__ = [
    "DDL",
    "ProductionAutoPublishRepository",
    "ProductionAutoPublishService",
]

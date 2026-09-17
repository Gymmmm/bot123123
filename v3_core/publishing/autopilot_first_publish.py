"""First-publish status commit for automatic Telegram delivery.

Pending listings may enter review → frozen package → approval → delivery.
The durable listing stays pending until Telegram delivery commits. The frozen
caption/keyboard uses official active first-publish semantics. Failed or
unknown delivery leaves the listing pending. Replay does not send again.
"""
from __future__ import annotations

import asyncio
from typing import Any

from .autopilot import AutoPublishResult, AutoPublishService
from .delivery_state import DeliveryBlocked
from .package_store import FrozenPackage
from .telegram_adapter import TelegramChannelAdapter, deliver_approved_package


class AutoFirstPublishService(AutoPublishService):
    def _activate_listing_after_successful_publish(self, listing_id: str) -> None:
        with self.repository._connect() as conn:
            conn.execute(
                """UPDATE listings_v3
                   SET inventory_status='active',updated_at=CURRENT_TIMESTAMP
                   WHERE listing_id=? AND inventory_status='pending'""",
                (str(listing_id),),
            )
            conn.commit()

    async def process_one(self, *, bot: Any, force_offer_id: str = "", origin: str | None = None) -> AutoPublishResult:
        self.repository.sync_candidates()
        if force_offer_id:
            self.repository.requeue(force_offer_id)
            with self.repository._connect() as conn:
                row = conn.execute(
                    """SELECT a.*,r.review_status,o.offer_type,o.publication_policy,o.offer_status,
                              o.monthly_rent_usd,l.inventory_status,l.public_listing_id,l.project_name,
                              l.public_location_display,l.layout,l.property_type,l.canonical_record_id
                       FROM publisher_auto_items_v3 a JOIN review_items r ON r.review_id=a.review_id
                       JOIN listing_offers o ON o.offer_id=a.offer_id JOIN listings_v3 l ON l.listing_id=a.listing_id
                       WHERE a.offer_id=?""",
                    (str(force_offer_id),),
                ).fetchone()
            item = dict(row) if row else None
        else:
            item = self.repository.next_item()
        if item is None:
            return AutoPublishResult("idle")
        offer_id = str(item["offer_id"])
        listing_id = str(item["listing_id"])
        if origin:
            self.repository.set_item(offer_id, state="queued", origin=origin)
        existing = self.repository.publication_for_offer(offer_id, self.channel_chat_id)
        if existing:
            self.repository.set_item(
                offer_id, state="published", reason_code="", reason_text="",
                channel_message_id=str(existing.get("channel_message_id") or ""), origin=origin,
            )
            self._activate_listing_after_successful_publish(listing_id)
            return AutoPublishResult(
                "already_published", offer_id=offer_id, listing_id=listing_id,
                channel_message_id=str(existing.get("channel_message_id") or ""),
            )
        unsafe = self.repository.unsafe_delivery_state(offer_id, self.channel_chat_id)
        if unsafe in {"sending", "sent", "unknown"}:
            return self._mark_exception(offer_id, "telegram_unknown")
        approved = self.repository.approved_package(offer_id)
        detail = self.workflow.review_detail(str(item["review_id"]))
        if str(detail.review.get("review_status") or "") in {"hold", "rejected"}:
            return self._mark_exception(offer_id, "admin_hold")
        facts = dict(detail.canonical.get("facts") or {})
        try:
            media = await asyncio.to_thread(self.workflow.review_media, review_id=str(item["review_id"]))
        except Exception:
            return self._mark_exception(offer_id, "unreadable_media")
        blockers = self._strict_blockers(item, facts, media, allow_pending=True)
        if blockers:
            return self._mark_exception(offer_id, blockers[0])
        if approved is None:
            self.repository.mark_listing_pending_for_auto_publish(listing_id)
            item["inventory_status"] = "pending"
        if str(detail.review.get("review_status") or "") != "approved":
            await asyncio.to_thread(
                self.workflow.approve_review,
                review_id=str(item["review_id"]),
                operator_user_id="system:auto_publish",
            )
        package: FrozenPackage
        if approved and str(approved.get("status")) == "published":
            existing = self.repository.publication_for_offer(offer_id, self.channel_chat_id)
            if existing:
                self.repository.set_item(
                    offer_id, state="published", channel_message_id=str(existing.get("channel_message_id") or ""), origin=origin,
                )
                self._activate_listing_after_successful_publish(listing_id)
                return AutoPublishResult("already_published", offer_id=offer_id, listing_id=listing_id)
            return self._mark_exception(offer_id, "telegram_unknown")
        if approved and str(approved.get("status")) == "approved":
            package = self.workflow.package(str(approved["package_id"]))
            if package.canonical_facts_hash != str(detail.canonical.get("facts_hash") or ""):
                return self._mark_exception(offer_id, "canonical_error")
        else:
            self.repository.set_listing_status(listing_id, "active")
            try:
                package = await asyncio.to_thread(
                    self.workflow.build_package_for_review,
                    review_id=str(item["review_id"]),
                )
                package = await asyncio.to_thread(
                    self.workflow.approve_package,
                    package_id=package.package_id,
                    approved_by="system:auto_publish",
                )
            finally:
                self.repository.mark_listing_pending_for_auto_publish(listing_id)
        self.repository.set_item(offer_id, state="sending", package_id=package.package_id, origin=origin)
        try:
            result = await deliver_approved_package(
                coordinator=self.workflow.delivery,
                adapter=TelegramChannelAdapter(bot),
                package_id=package.package_id,
                channel_chat_id=self.channel_chat_id,
                inventory_status_override="active",
            )
        except DeliveryBlocked as exc:
            code = "telegram_unknown" if "unknown" in str(exc).lower() or "sending" in str(exc).lower() or "sent" in str(exc).lower() else "telegram_failed"
            return self._mark_exception(offer_id, code)
        except Exception:
            return self._mark_exception(offer_id, "telegram_unknown")
        message_id = str(result.publication.channel_message_id)
        self.repository.set_item(
            offer_id, state="published", package_id=package.package_id,
            channel_message_id=message_id, origin=origin,
        )
        self._activate_listing_after_successful_publish(listing_id)
        return AutoPublishResult(
            "published", offer_id=offer_id, listing_id=listing_id,
            package_id=package.package_id, channel_message_id=message_id,
        )


__all__ = ["AutoFirstPublishService"]

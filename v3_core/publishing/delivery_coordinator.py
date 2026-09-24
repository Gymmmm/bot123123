"""Coordinate one approved frozen package across the Telegram send boundary.

No Telegram client is imported here.  The adapter asks for a send command,
performs the external API call, then records the durable receipt.  Recovery can
resume from a saved ``sent`` attempt without sending again.
"""
from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from typing import Any

from v3_core.storage.inventory_reader import InventoryReader
from .delivery_state import (
    DeliveryAttempt,
    DeliveryBlocked,
    PublicationDeliveryStateRepository,
)
from .channel_contract import assert_channel_identity
from .package_store import FrozenPackageStore
from .publication_instances import (
    PublicationInstance,
    PublicationInstanceRepository,
)


@dataclass(frozen=True)
class TelegramSendCommand:
    attempt_id: str
    package_id: str
    channel_chat_id: str
    cover_path: str
    caption: str
    actions: dict[str, str]
    inventory_status: str
    area_key: str = ""


@dataclass(frozen=True)
class PublicationCommitResult:
    attempt: DeliveryAttempt
    publication: PublicationInstance


class PublicationDeliveryCoordinator:
    def __init__(
        self,
        *,
        reader: InventoryReader,
        packages: FrozenPackageStore,
        deliveries: PublicationDeliveryStateRepository,
        publications: PublicationInstanceRepository,
    ):
        self.reader = reader
        self.packages = packages
        self.deliveries = deliveries
        self.publications = publications

    def prepare_send(
        self,
        *,
        package_id: str,
        channel_chat_id: str,
        inventory_status_override: str | None = None,
    ) -> TelegramSendCommand:
        package = self.packages.verify_frozen(package_id)
        if package.status != "approved":
            if package.status == "published":
                raise DeliveryBlocked("package is already published")
            raise DeliveryBlocked("package is not approved")

        offer = self.reader.offer(package.offer_id)
        if str(offer.get("offer_type") or "") != "rent":
            raise DeliveryBlocked("only rent offers may be delivered")
        if str(offer.get("publication_policy") or "") != "telegram_rent":
            raise DeliveryBlocked("offer publication policy is not telegram_rent")
        if int(offer.get("publishable") or 0) != 1:
            raise DeliveryBlocked("offer is not publishable")
        if str(offer.get("offer_status") or "") != "active":
            raise DeliveryBlocked("offer is not active")

        existing_publication = self.publications.get_published_for_listing(
            package.listing_id,
            platform="telegram",
            channel_chat_id=str(channel_chat_id),
        )
        if existing_publication is not None:
            raise DeliveryBlocked(
                "listing already has a published Telegram publication; use publication management"
            )

        attempt = self.deliveries.prepare(
            package_id=package.package_id,
            listing_id=package.listing_id,
            offer_id=package.offer_id,
            channel_chat_id=str(channel_chat_id),
        )
        if attempt.state == "committed":
            raise DeliveryBlocked("delivery is already committed")
        if attempt.state == "sent":
            raise DeliveryBlocked("delivery receipt already exists; finalize without resending")
        if attempt.state not in {"prepared", "failed_before_send"}:
            raise DeliveryBlocked(f"delivery attempt is not sendable: {attempt.state}")

        listing = self.reader.listing(package.listing_id)
        snapshot_listing_id = str(package.snapshot.get("listing_id") or "").strip()
        snapshot_public_id = str(package.snapshot.get("public_listing_id") or "").strip()
        live_public_id = str(listing.get("public_listing_id") or "").strip()
        if snapshot_listing_id != package.listing_id:
            raise DeliveryBlocked("channel_identity_mismatch:snapshot_listing_id")
        if not snapshot_public_id or snapshot_public_id != live_public_id:
            raise DeliveryBlocked("channel_identity_mismatch:public_listing_id")
        try:
            assert_channel_identity(
                caption=package.post_text,
                actions=package.actions,
                public_listing_id=snapshot_public_id,
            )
        except ValueError as exc:
            raise DeliveryBlocked(f"channel_identity_mismatch:{exc}") from exc

        # Caption/facts remain frozen in the approved package. Normally the live
        # inventory status drives Telegram booking UI. Manual first-publish may
        # explicitly supply the status that will become live only after Telegram
        # acknowledges the send, so the first keyboard and frozen caption cannot
        # disagree while the DB remains safely pending until success.
        live_status = str(listing.get("inventory_status") or "pending").strip().lower()
        override = str(inventory_status_override or "").strip().lower()
        allowed = {"active", "reserved", "high_demand", "pending", "rented", "inactive", "offline"}
        if override and override not in allowed:
            raise ValueError(f"invalid_inventory_status_override:{inventory_status_override}")
        effective_status = override or live_status
        return TelegramSendCommand(
            attempt_id=attempt.attempt_id,
            package_id=package.package_id,
            channel_chat_id=str(channel_chat_id),
            cover_path=package.cover_path,
            caption=package.post_text,
            actions=dict(package.actions),
            inventory_status=effective_status,
            area_key=str(listing.get("public_location_key") or listing.get("canonical_area_key") or "").strip(),
        )

    def mark_sending(self, attempt_id: str) -> None:
        self.deliveries.mark_sending(attempt_id)

    def mark_failed_before_send(self, attempt_id: str, reason: str) -> None:
        self.deliveries.mark_failed_before_send(attempt_id, reason)

    def mark_unknown(
        self,
        attempt_id: str,
        error: str,
        telegram_result: dict[str, Any] | None = None,
    ) -> None:
        self.deliveries.mark_unknown(attempt_id, error, telegram_result)

    def record_sent(
        self, *, attempt_id: str, telegram_result: dict[str, Any]
    ) -> PublicationCommitResult:
        self.deliveries.mark_sent(attempt_id, telegram_result)
        return self.finalize_saved_receipt(attempt_id=attempt_id)

    def reconcile_unknown_as_sent(
        self, *, attempt_id: str, telegram_result: dict[str, Any]
    ) -> PublicationCommitResult:
        attempt = self.deliveries.get(attempt_id)
        if attempt.state not in {"unknown", "sent", "committed"}:
            raise DeliveryBlocked(
                f"cannot reconcile {attempt.state} delivery as sent; expected unknown"
            )
        if attempt.state == "unknown":
            self.deliveries.reconcile_unknown_as_sent(attempt_id, telegram_result)
        result = self.finalize_saved_receipt(attempt_id=attempt_id)
        self._mark_auto_item_published(
            offer_id=str(result.attempt.offer_id or result.publication.offer_id or ""),
            package_id=str(result.attempt.package_id),
            channel_message_id=str(result.publication.channel_message_id or ""),
        )
        return result

    def reconcile_unknown_as_not_sent(self, *, attempt_id: str, reason: str) -> DeliveryAttempt:
        attempt = self.deliveries.reconcile_unknown_as_not_sent(attempt_id, reason)
        self._clear_telegram_unknown_auto_item(offer_id=str(attempt.offer_id or ""))
        return attempt

    def _mark_auto_item_published(
        self, *, offer_id: str, package_id: str, channel_message_id: str
    ) -> None:
        clean_offer = str(offer_id or "").strip()
        if not clean_offer:
            return
        with sqlite3.connect(self.deliveries.db_path) as conn:
            conn.execute(
                """UPDATE publisher_auto_items_v3
                      SET state='published',
                          reason_code='',
                          reason_text='',
                          package_id=CASE WHEN ?<>'' THEN ? ELSE package_id END,
                          channel_message_id=CASE WHEN ?<>'' THEN ? ELSE channel_message_id END,
                          published_at=COALESCE(published_at, CURRENT_TIMESTAMP),
                          updated_at=CURRENT_TIMESTAMP
                    WHERE offer_id=?""",
                (
                    str(package_id or ""),
                    str(package_id or ""),
                    str(channel_message_id or ""),
                    str(channel_message_id or ""),
                    clean_offer,
                ),
            )
            conn.commit()

    def _clear_telegram_unknown_auto_item(self, *, offer_id: str) -> None:
        clean_offer = str(offer_id or "").strip()
        if not clean_offer:
            return
        with sqlite3.connect(self.deliveries.db_path) as conn:
            conn.execute(
                """UPDATE publisher_auto_items_v3
                      SET state='queued',
                          reason_code='',
                          reason_text='',
                          ignored=0,
                          updated_at=CURRENT_TIMESTAMP
                    WHERE offer_id=?
                      AND (
                            (state='exception' AND reason_code='telegram_unknown')
                            OR state='sending'
                          )""",
                (clean_offer,),
            )
            conn.commit()

    def finalize_saved_receipt(self, *, attempt_id: str) -> PublicationCommitResult:
        attempt = self.deliveries.get(attempt_id)
        if attempt.state == "committed":
            existing = self.publications.get_for_package(
                attempt.package_id,
                platform="telegram",
                channel_chat_id=attempt.channel_chat_id,
            )
            if existing is None:
                raise DeliveryBlocked(
                    "committed delivery is missing its publication instance"
                )
            return PublicationCommitResult(attempt=attempt, publication=existing)
        if attempt.state != "sent" or not attempt.telegram_result:
            raise DeliveryBlocked(
                f"delivery attempt has no durable sent receipt: {attempt.state}"
            )

        package = self.packages.verify_frozen(attempt.package_id)
        if package.status not in {"approved", "published"}:
            raise DeliveryBlocked(
                f"cannot finalize package in {package.status} state"
            )
        if (
            package.listing_id != attempt.listing_id
            or package.offer_id != attempt.offer_id
        ):
            raise DeliveryBlocked("delivery identity does not match frozen package")

        publication = self.publications.record_telegram_publication(
            package_id=package.package_id,
            listing_id=package.listing_id,
            offer_id=package.offer_id,
            channel_chat_id=attempt.channel_chat_id,
            telegram_result=attempt.telegram_result,
        )
        self.packages.mark_published(package.package_id)
        committed = self.deliveries.mark_committed(attempt.attempt_id)
        return PublicationCommitResult(
            attempt=committed,
            publication=publication,
        )


__all__ = [
    "PublicationCommitResult",
    "PublicationDeliveryCoordinator",
    "TelegramSendCommand",
]

"""Coordinate frozen V3 publications across the Telegram boundary.

New sends and existing-post edits both produce ``TelegramSendCommand`` objects.
The external message identity for edits always comes from ``publication_instances``;
message-id arithmetic is never part of the publication contract.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from v3_core.storage.inventory_reader import InventoryReader
from .delivery_state import (
    DeliveryAttempt,
    DeliveryBlocked,
    PublicationDeliveryStateRepository,
)
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

    def _command_for_package(
        self,
        *,
        package_id: str,
        channel_chat_id: str,
        attempt_id: str,
    ) -> TelegramSendCommand:
        package = self.packages.verify_frozen(package_id)
        listing = self.reader.listing(package.listing_id)
        return TelegramSendCommand(
            attempt_id=str(attempt_id),
            package_id=package.package_id,
            channel_chat_id=str(channel_chat_id),
            cover_path=package.cover_path,
            caption=package.post_text,
            actions=dict(package.actions),
            inventory_status=str(
                listing.get("inventory_status") or "pending"
            ).strip().lower(),
        )

    def prepare_send(
        self, *, package_id: str, channel_chat_id: str
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

        return self._command_for_package(
            package_id=package.package_id,
            channel_chat_id=str(channel_chat_id),
            attempt_id=attempt.attempt_id,
        )

    def prepare_edit(
        self,
        *,
        channel_chat_id: str,
        channel_message_id: str | int,
    ) -> tuple[PublicationInstance, TelegramSendCommand]:
        """Resolve one existing Telegram post and prepare the normal frozen output.

        The package may already be published or archived/superseded: the durable
        publication instance is the authority for which frozen product belongs to
        the external Telegram message. Only inventory status is live at edit time.
        """
        publication = self.publications.get_for_channel_message(
            channel_chat_id=str(channel_chat_id),
            channel_message_id=str(channel_message_id),
            platform="telegram",
        )
        if publication is None:
            raise DeliveryBlocked("telegram publication instance not found")
        package = self.packages.verify_frozen(publication.package_id)
        if package.listing_id != publication.listing_id:
            raise DeliveryBlocked("publication listing does not match frozen package")
        if publication.offer_id and package.offer_id != publication.offer_id:
            raise DeliveryBlocked("publication offer does not match frozen package")
        command = self._command_for_package(
            package_id=package.package_id,
            channel_chat_id=publication.channel_chat_id,
            attempt_id=f"edit:{publication.instance_id}",
        )
        return publication, command

    def record_edit(self, instance_id: str, *, post_text: str) -> PublicationInstance:
        return self.publications.record_edit(instance_id, post_text=post_text)

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

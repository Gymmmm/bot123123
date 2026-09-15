"""Admin-facing V3 review/package workflow.

This service is intentionally Telegram-agnostic.  It coordinates already
extracted V3 repositories so an admin UI can review/correct canonical inventory,
choose publication media, build a frozen package, approve it, and hand it to the
delivery boundary.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from v3_core.media.cover_service import CoverRenderService
from v3_core.media.service import MediaPreparationService, PreparedSourceMedia
from v3_core.storage.inventory_reader import InventoryReader
from v3_core.storage.inventory_repository import InventoryRepository
from .admin_operations import PublisherAdminOperations, QueueCounts
from .delivery_coordinator import PublicationDeliveryCoordinator, TelegramSendCommand
from .package_service import PackageApprovalService, PackageBuildService
from .package_store import FrozenPackage, FrozenPackageStore


@dataclass(frozen=True)
class ReviewDetail:
    review: dict[str, Any]
    listing: dict[str, Any]
    offer: dict[str, Any]
    canonical: dict[str, Any]


class PublisherWorkflowService:
    def __init__(
        self,
        *,
        reader: InventoryReader,
        inventory: InventoryRepository,
        media: MediaPreparationService,
        covers: CoverRenderService,
        packages: FrozenPackageStore,
        package_builder: PackageBuildService,
        package_approver: PackageApprovalService,
        delivery: PublicationDeliveryCoordinator,
        admin_ops: PublisherAdminOperations | None = None,
    ):
        self.reader = reader
        self.inventory = inventory
        self.media = media
        self.covers = covers
        self.packages = packages
        self.package_builder = package_builder
        self.package_approver = package_approver
        self.delivery = delivery
        self.admin_ops = admin_ops or PublisherAdminOperations(
            db_path=str(reader.db_path),
            reader=reader,
            inventory=inventory,
        )

    def pending_reviews(self, *, limit: int = 20) -> list[dict[str, Any]]:
        return self.reader.reviews_by_status("pending", limit=limit)

    def approved_reviews(self, *, limit: int = 20) -> list[dict[str, Any]]:
        return self.reader.reviews_by_status("approved", limit=limit)

    def reviews(self, status: str, *, limit: int = 20) -> list[dict[str, Any]]:
        return self.reader.reviews_by_status(str(status), limit=limit)

    def review_detail(self, review_id: str) -> ReviewDetail:
        review = self.reader.review(review_id)
        listing_id = str(review.get("listing_id") or "")
        offer_id = str(review.get("offer_id") or "")
        if not listing_id or not offer_id:
            raise ValueError("review_has_no_materialized_offer")
        listing = self.reader.listing(listing_id)
        offer = self.reader.offer(offer_id)
        canonical = self.reader.canonical(str(review["canonical_record_id"]))
        if str(listing["canonical_record_id"]) != str(review["canonical_record_id"]):
            raise ValueError("review_listing_canonical_mismatch")
        if str(offer["listing_id"]) != listing_id:
            raise ValueError("review_offer_listing_mismatch")
        return ReviewDetail(
            review=review,
            listing=listing,
            offer=offer,
            canonical=canonical,
        )

    def approve_review(self, *, review_id: str, operator_user_id: str) -> ReviewDetail:
        detail = self.review_detail(review_id)
        status = str(detail.review.get("review_status") or "")
        if status == "approved":
            return detail
        if status != "pending":
            raise ValueError(f"review_not_pending:{status}")
        self.inventory.approve_review(
            review_id=str(review_id),
            operator_user_id=str(operator_user_id),
        )
        return self.review_detail(review_id)

    def set_review_status(
        self,
        *,
        review_id: str,
        status: str,
        operator_user_id: str,
        note: str = "",
    ) -> ReviewDetail:
        self.admin_ops.set_review_status(
            review_id=review_id,
            status=status,
            operator_user_id=operator_user_id,
            note=note,
        )
        return self.review_detail(review_id)

    def edit_review_field(
        self,
        *,
        review_id: str,
        field_name: str,
        value: Any,
        operator_user_id: str,
    ) -> ReviewDetail:
        self.admin_ops.edit_review_field(
            review_id=review_id,
            field_name=field_name,
            raw_value=value,
            operator_user_id=operator_user_id,
        )
        return self.review_detail(review_id)

    def review_media(
        self,
        *,
        review_id: str,
        manual_cover_path: str | None = None,
    ) -> PreparedSourceMedia:
        detail = self.review_detail(review_id)
        source_post_id = str(detail.canonical.get("source_post_id") or "").strip()
        if not source_post_id:
            raise ValueError("canonical_record_missing_source_post_id")
        return self.media.prepare(
            source_post_id=source_post_id,
            manual_cover_path=manual_cover_path,
        )

    def build_package_for_review(
        self,
        *,
        review_id: str,
        cover_style: str | None = None,
        manual_cover_path: str | None = None,
        excluded_gallery_paths: Iterable[str] = (),
    ) -> FrozenPackage:
        detail = self.review_detail(review_id)
        if str(detail.review.get("review_status") or "") != "approved":
            raise ValueError("review_must_be_approved_before_package")
        if str(detail.offer.get("offer_type") or "") != "rent":
            raise ValueError("publisher_only_builds_rent_packages")
        if str(detail.offer.get("publication_policy") or "") != "telegram_rent":
            raise ValueError("publisher_offer_policy_is_not_telegram_rent")

        source_post_id = str(detail.canonical.get("source_post_id") or "").strip()
        if not source_post_id:
            raise ValueError("canonical_record_missing_source_post_id")
        media = self.media.prepare(
            source_post_id=source_post_id,
            manual_cover_path=manual_cover_path,
        )
        excluded = {str(path) for path in excluded_gallery_paths if str(path).strip()}
        gallery = [path for path in media.gallery_paths if str(path) not in excluded]
        if not gallery:
            raise ValueError("publisher_gallery_cannot_be_empty")
        if str(media.cover_source_path) in excluded:
            raise ValueError("publisher_cover_cannot_be_excluded_from_gallery")
        rendered = self.covers.render(
            listing_id=str(detail.listing["listing_id"]),
            offer_id=str(detail.offer["offer_id"]),
            media=media,
            style=cover_style,
        )
        return self.package_builder.build(
            listing_id=str(detail.listing["listing_id"]),
            offer_id=str(detail.offer["offer_id"]),
            cover_style=rendered.style,
            cover_path=rendered.output_path,
            gallery=gallery,
            source_identity=dict(media.source_identity),
        )

    def package(self, package_id: str) -> FrozenPackage:
        package = self.packages.get(package_id)
        if package is None:
            raise KeyError(package_id)
        return package

    def approve_package(self, *, package_id: str, approved_by: str) -> FrozenPackage:
        return self.package_approver.approve(
            package_id=str(package_id),
            approved_by=str(approved_by),
        )

    def prepare_send(self, *, package_id: str, channel_chat_id: str) -> TelegramSendCommand:
        return self.delivery.prepare_send(
            package_id=str(package_id),
            channel_chat_id=str(channel_chat_id),
        )

    def queue_counts(self) -> QueueCounts:
        return self.admin_ops.queue_counts()

    def package_rows(self, status: str, *, limit: int = 10, offset: int = 0) -> list[dict[str, Any]]:
        return self.admin_ops.package_rows(status, limit=limit, offset=offset)

    def delivery_rows(self, state: str, *, limit: int = 10, offset: int = 0) -> list[dict[str, Any]]:
        return self.admin_ops.delivery_rows(state, limit=limit, offset=offset)


__all__ = ["PublisherWorkflowService", "ReviewDetail"]

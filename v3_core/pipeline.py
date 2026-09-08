"""Side-by-side V3 core facade.

This is intentionally not wired to production systemd services yet. It gives
migration code one explicit API for the extracted architecture without routing
through drafts, runtime patches, CSV publishing, or the legacy publisher.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from v3_core.ingest.intake_service import IntakeResult, IntakeService, SourceIntake
from v3_core.ingest.source_reader import SourceReader
from v3_core.ingest.source_repository import SourceRepository
from v3_core.inventory.identity import IdentityService, ListingIdentity
from v3_core.inventory.service import InventoryMaterializationService, MaterializationResult
from v3_core.media.cover_service import CoverRenderService, RenderedCover
from v3_core.media.service import MediaPreparationService, PreparedSourceMedia
from v3_core.parser.service import CanonicalParseResult, CanonicalParseService
from v3_core.publishing.delivery_coordinator import (
    PublicationDeliveryCoordinator,
    TelegramSendCommand,
)
from v3_core.publishing.delivery_state import PublicationDeliveryStateRepository
from v3_core.publishing.package_service import PackageApprovalService, PackageBuildService
from v3_core.publishing.package_store import FrozenPackage, FrozenPackageStore
from v3_core.publishing.publication_instances import PublicationInstanceRepository
from v3_core.storage.inventory_reader import InventoryReader
from v3_core.storage.inventory_repository import InventoryRepository


class V3CorePipeline:
    def __init__(
        self,
        *,
        db_path: str,
        user_bot_username: str,
        min_listing_images: int = 4,
        cover_output_dir: str | None = None,
    ):
        self.db_path = str(db_path)
        self.sources = SourceRepository(self.db_path)
        self.source_reader = SourceReader(self.db_path)
        self.inventory = InventoryRepository(self.db_path)
        self.inventory_reader = InventoryReader(self.db_path)
        self.identities = IdentityService(self.db_path)
        self.intake = IntakeService(
            self.sources,
            min_listing_images=min_listing_images,
        )
        self.parser = CanonicalParseService(self.sources, self.inventory)
        self.materializer = InventoryMaterializationService(self.inventory)
        self.media = MediaPreparationService(self.source_reader)
        cover_dir = (
            Path(cover_output_dir).expanduser().resolve()
            if cover_output_dir
            else Path(self.db_path).expanduser().resolve().parent / "covers_v3"
        )
        self.covers = CoverRenderService(
            reader=self.inventory_reader,
            output_dir=cover_dir,
        )
        self.packages = FrozenPackageStore(self.db_path)
        self.package_builder = PackageBuildService(
            reader=self.inventory_reader,
            store=self.packages,
            user_bot_username=user_bot_username,
        )
        self.package_approver = PackageApprovalService(
            reader=self.inventory_reader,
            inventory_repository=self.inventory,
            store=self.packages,
        )
        self.deliveries = PublicationDeliveryStateRepository(self.db_path)
        self.publications = PublicationInstanceRepository(self.db_path)
        self.delivery = PublicationDeliveryCoordinator(
            reader=self.inventory_reader,
            packages=self.packages,
            deliveries=self.deliveries,
            publications=self.publications,
        )

    def ingest_source(self, source: SourceIntake) -> IntakeResult:
        return self.intake.persist(source)

    def parse_source(self, source_post_id: int) -> CanonicalParseResult:
        return self.parser.parse_source_post(int(source_post_id))

    def allocate_identity(self, *, canonical_record_id: str) -> ListingIdentity:
        facts = self.inventory.canonical_facts(canonical_record_id)
        return self.identities.allocate(
            canonical_record_id=canonical_record_id,
            facts=facts,
        )

    def materialize_inventory(
        self,
        *,
        canonical_record_id: str,
        listing_id: str | None = None,
        public_listing_id: str | None = None,
        create_review: bool = True,
    ) -> MaterializationResult:
        if not listing_id or not public_listing_id:
            identity = self.allocate_identity(canonical_record_id=canonical_record_id)
            listing_id = listing_id or identity.listing_id
            public_listing_id = public_listing_id or identity.public_listing_id
        return self.materializer.materialize(
            canonical_record_id=canonical_record_id,
            listing_id=str(listing_id),
            public_listing_id=str(public_listing_id),
            create_review=create_review,
        )

    def parse_and_materialize(
        self,
        *,
        source_post_id: int,
        listing_id: str | None = None,
        public_listing_id: str | None = None,
        create_review: bool = True,
    ) -> tuple[CanonicalParseResult, MaterializationResult]:
        parsed = self.parse_source(source_post_id)
        materialized = self.materialize_inventory(
            canonical_record_id=parsed.canonical_record_id,
            listing_id=listing_id,
            public_listing_id=public_listing_id,
            create_review=create_review,
        )
        return parsed, materialized

    def prepare_media(
        self,
        *,
        source_post_id: int,
        manual_cover_path: str | None = None,
    ) -> PreparedSourceMedia:
        return self.media.prepare(
            source_post_id=source_post_id,
            manual_cover_path=manual_cover_path,
        )

    def render_cover(
        self,
        *,
        listing_id: str,
        offer_id: str,
        media: PreparedSourceMedia,
        cover_style: str = "classic_blue",
        output_path: str | None = None,
    ) -> RenderedCover:
        return self.covers.render(
            listing_id=listing_id,
            offer_id=offer_id,
            media=media,
            style=cover_style,
            output_path=output_path,
        )

    def approve_review(self, *, review_id: str, operator_user_id: str) -> dict[str, Any]:
        return self.inventory.approve_review(
            review_id=review_id,
            operator_user_id=operator_user_id,
        )

    def build_package(
        self,
        *,
        listing_id: str,
        offer_id: str,
        cover_style: str,
        media: PreparedSourceMedia,
        rendered_cover_path: str | None = None,
    ) -> FrozenPackage:
        cover_path = str(rendered_cover_path or "").strip()
        if not cover_path:
            cover_path = self.render_cover(
                listing_id=listing_id,
                offer_id=offer_id,
                media=media,
                cover_style=cover_style,
            ).output_path
        return self.package_builder.build(
            listing_id=listing_id,
            offer_id=offer_id,
            cover_style=cover_style,
            cover_path=cover_path,
            gallery=list(media.gallery_paths),
            source_identity=dict(media.source_identity),
        )

    def approve_package(self, *, package_id: str, approved_by: str) -> FrozenPackage:
        return self.package_approver.approve(
            package_id=package_id,
            approved_by=approved_by,
        )

    def prepare_send(self, *, package_id: str, channel_chat_id: str) -> TelegramSendCommand:
        return self.delivery.prepare_send(
            package_id=package_id,
            channel_chat_id=channel_chat_id,
        )


__all__ = ["V3CorePipeline"]

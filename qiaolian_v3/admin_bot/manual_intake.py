from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from qiaolian_v3.db.repositories.reviews import ReviewRepository
from qiaolian_v3.ingest.manual_intake import ManualIntakePreview, ManualIntakeService
from qiaolian_v3.publishing.delivery import DeliveryCoordinator
from qiaolian_v3.publishing.gate import GateResult
from qiaolian_v3.publishing.package import FrozenPublicationPackage, build_frozen_rent_package
from qiaolian_v3.publishing.publisher import RentPublisher


class AdminConfirmBlocked(RuntimeError):
    pass


class ExplicitAdminApprovalGate:
    """Explicit-admin-only gate; it never participates in auto-publish planning."""

    def evaluate(self, package: FrozenPublicationPackage) -> GateResult:
        reasons: list[str] = []
        if not package.frozen:
            reasons.append('package_not_frozen')
        if package.deal_type != 'rent' or package.offer_type != 'rent':
            reasons.append('admin_confirm_requires_rent')
        if package.publication_policy != 'telegram_rent':
            reasons.append('offer_not_telegram_rent')
        if package.source_mode != 'admin_import':
            reasons.append('source_not_admin_import')
        if package.quality_result != 'ADMIN_APPROVED':
            reasons.append('explicit_admin_approval_required')
        return GateResult(allowed=not reasons, reasons=tuple(reasons))


@dataclass(frozen=True)
class AdminPublicationFields:
    target_channel_id: str
    cover_path: str
    gallery: tuple[str, ...]
    caption_html: str
    bot_username: str


class AdminManualIntakeController:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.intake = ManualIntakeService(conn)
        self.reviews = ReviewRepository(conn)

    def preview(self, *, text: str, images: list[bytes], operator_id: str) -> ManualIntakePreview:
        return self.intake.import_listing(text=text, images=images, operator_id=operator_id)

    def confirm(
        self,
        *,
        preview: ManualIntakePreview,
        operator_id: str,
        fields: AdminPublicationFields,
        gateway: Any,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        review = self.reviews.get(preview.review_item_id)
        if review.status != 'open':
            raise AdminConfirmBlocked('review_item_not_open')
        if preview.deal_type != 'rent' or preview.listing_id is None or preview.offer_id is None:
            raise AdminConfirmBlocked('only_rent_can_be_confirmed_for_rent_channel')
        canonical_row = self.conn.execute('SELECT * FROM canonical_records WHERE id=?', (preview.canonical_record_id,)).fetchone()
        listing_row = self.conn.execute('SELECT * FROM v3_listings WHERE id=?', (preview.listing_id,)).fetchone()
        offer_row = self.conn.execute('SELECT * FROM listing_offers WHERE id=?', (preview.offer_id,)).fetchone()
        if canonical_row is None or listing_row is None or offer_row is None:
            raise AdminConfirmBlocked('preview_state_missing')
        if str(offer_row['offer_type']) != 'rent':
            raise AdminConfirmBlocked('offer_not_rent')

        self.reviews.resolve(
            preview.review_item_id,
            status='approved',
            operator_id=str(operator_id),
            resolution={'action': 'explicit_confirm'},
        )
        self.conn.execute(
            "UPDATE listing_offers SET publication_policy='telegram_rent',updated_at=CURRENT_TIMESTAMP WHERE id=? AND offer_type='rent'",
            (preview.offer_id,),
        )
        canonical = json.loads(str(canonical_row['facts_json']))
        package = build_frozen_rent_package(
            listing_id=preview.listing_id,
            offer_id=preview.offer_id,
            canonical_record_id=preview.canonical_record_id,
            target_channel_id=str(fields.target_channel_id),
            canonical=canonical,
            offer={'offer_type': 'rent', 'publication_policy': 'telegram_rent'},
            source_mode='admin_import',
            quality_result='ADMIN_APPROVED',
            canonical_hash=str(canonical_row['facts_hash']),
            cover_path=str(fields.cover_path),
            gallery=fields.gallery,
            caption_html=str(fields.caption_html),
            bot_username=str(fields.bot_username),
            public_listing_id=str(listing_row['public_listing_id']),
        )
        self.conn.execute(
            '''INSERT INTO v3_publication_packages(
                package_id,idempotency_key,listing_id,offer_id,canonical_record_id,package_version,
                target_channel_id,status,approval_mode,approved_by,approved_at,
                cover_path,gallery_json,caption_html,keyboard_json,content_hash,canonical_hash
            ) VALUES (?,?,?,?,?,1,?,'frozen','admin',?,CURRENT_TIMESTAMP,?,?,?,?,?,?)''',
            (
                package.package_id, package.package_id, package.listing_id, package.offer_id,
                package.canonical_record_id, package.target_channel_id, str(operator_id),
                package.cover_path, json.dumps(list(package.gallery), ensure_ascii=False),
                package.caption_html,
                json.dumps([{'text': text, 'url': url} for text, url in package.keyboard], ensure_ascii=False),
                package.content_hash, package.canonical_hash,
            ),
        )
        self.conn.commit()
        publisher = RentPublisher(
            gateway=gateway,
            delivery=DeliveryCoordinator(self.conn),
            dry_run=bool(dry_run),
            gate=ExplicitAdminApprovalGate(),
        )
        result = publisher.publish(package)
        return {'package': package, 'publish_result': result, 'explicit_confirm': True}


__all__ = [
    'AdminConfirmBlocked', 'AdminManualIntakeController', 'AdminPublicationFields',
    'ExplicitAdminApprovalGate',
]

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from typing import Sequence

from qiaolian_v3.db.repositories.canonical import CanonicalRecordRepository
from qiaolian_v3.db.repositories.reviews import ReviewRepository
from qiaolian_v3.listing.dedupe import DedupeDecision
from qiaolian_v3.listing.materializer import CanonicalListingMaterializer
from qiaolian_v3.media.source_media import SourceMedia
from qiaolian_v3.parser.canonical import canonicalize_source
from qiaolian_v3.parser.quality_gate import QualityDecision, evaluate_quality_gate

from .source_service import SourceIngestService


@dataclass(frozen=True)
class ManualIntakePreview:
    intake_id: str
    source_post_id: int
    revision_id: int
    canonical_record_id: int
    listing_id: int | None
    offer_id: int | None
    public_listing_id: str | None
    deal_type: str
    publication_policy: str
    quality_decision: str | None
    blocking_reasons: tuple[str, ...]
    review_item_id: int
    confirmed: bool = False


class ManualIntakeService:
    """Admin text/images enter the same V3 ingest/parser/materializer/quality path.

    This service never creates a publication package and never calls Telegram.
    Explicit confirmation belongs to the Admin Bot layer.
    """

    parser = staticmethod(canonicalize_source)

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.ingest = SourceIngestService(conn)
        self.canonical = CanonicalRecordRepository(conn)
        self.materializer = CanonicalListingMaterializer(conn)
        self.reviews = ReviewRepository(conn)

    def import_listing(
        self,
        *,
        text: str,
        images: Sequence[bytes],
        operator_id: str,
        external_post_id: str | None = None,
    ) -> ManualIntakePreview:
        intake_id = str(external_post_id or f'admin-{uuid.uuid4().hex}')
        media = tuple(
            SourceMedia.from_bytes(bytes(data), sort_order=index)
            for index, data in enumerate(images)
        )
        ingest = self.ingest.ingest_telegram(
            source_name='admin_manual_intake',
            source_external_identity=f'admin:{operator_id}',
            external_post_id=intake_id,
            raw_text=str(text),
            media=media,
            source_created_at=None,
            fetched_at=None,
            source_type='admin_manual',
            source_mode='admin_import',
            source_url='',
            source_author=str(operator_id),
            raw_payload={'origin': 'admin_bot_manual_intake'},
        )
        facts = self.parser(
            raw_text=str(text),
            sanitized_text=ingest.sanitized_text,
            source_identity={
                'source_post_id': ingest.source_post_id,
                'source_type': 'admin_manual',
                'source_name': 'admin_manual_intake',
                'external_post_id': intake_id,
            },
            media_summary={'source_media_count': len(media)},
        )
        canonical = self.canonical.append(
            source_post_id=ingest.source_post_id,
            source_post_revision_id=ingest.revision_id,
            schema_version=str(facts['schema_version']),
            parser_revision=str(facts['parser_revision']),
            facts=facts,
            facts_hash=str(facts['canonical_facts_hash']),
            deal_type=str(facts.get('deal_type') or 'unknown'),
            deal_type_candidates=list(facts.get('deal_type_candidates') or []),
            quality=dict(facts.get('quality') or {}),
        )

        listing_id = None
        offer_id = None
        public_listing_id = None
        deal_type = str(facts.get('deal_type') or 'unknown')
        if deal_type in {'rent', 'sale'}:
            materialized = self.materializer.materialize(canonical.id)
            listing_id = materialized.listing_id
            offer_id = materialized.offer_ids[0]
            public_listing_id = materialized.public_listing_id

        media_summary = {
            'usable_count': len(media),
            'cover_candidates': [item.media_identity for item in media[:1]],
            'source_identity_complete': True,
        }
        quality: QualityDecision = evaluate_quality_gate(
            facts,
            source_mode='admin_import',
            media_summary=media_summary,
            dedupe_result=DedupeDecision.NEW,
        )
        reasons = list(quality.blocking_reasons)
        if 'admin_import_requires_review' not in reasons:
            reasons.append('admin_import_requires_review')
        review = self.reviews.create(
            review_type='admin_import',
            source_mode='admin_import',
            reason_codes=reasons,
            canonical_record_id=canonical.id,
            listing_id=listing_id,
            offer_id=offer_id,
            operator_id=str(operator_id),
        )
        self.conn.commit()
        return ManualIntakePreview(
            intake_id=intake_id,
            source_post_id=ingest.source_post_id,
            revision_id=ingest.revision_id,
            canonical_record_id=canonical.id,
            listing_id=listing_id,
            offer_id=offer_id,
            public_listing_id=public_listing_id,
            deal_type=deal_type,
            publication_policy=quality.publication_policy,
            quality_decision=quality.routing_decision.value if quality.routing_decision is not None else None,
            blocking_reasons=tuple(reasons),
            review_item_id=review.id,
        )


__all__ = ['ManualIntakePreview', 'ManualIntakeService']

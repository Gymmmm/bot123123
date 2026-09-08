"""Persist V3 canonical facts from an immutable SourcePostRevision."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from qiaolian_v3.db.repositories.canonical import CanonicalRecordRepository
from qiaolian_v3.db.repositories.sources import SourceRepository

from .canonical import canonicalize_source


@dataclass(frozen=True)
class ParsedCanonical:
    canonical_record_id: int
    source_post_id: int
    source_post_revision_id: int
    deal_type: str
    facts_hash: str


class CanonicalParserService:
    """Source revision -> canonical_records only; never drafts/publication."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.sources = SourceRepository(conn)
        self.canonical = CanonicalRecordRepository(conn)

    def parse_revision(self, revision_id: int) -> ParsedCanonical:
        revision = self.sources.get_revision(int(revision_id))
        if revision is None:
            raise LookupError('source_revision_not_found')
        source_post_id = int(revision['source_post_id'])
        source_post = self.sources.get_source_post(source_post_id)
        if source_post is None:
            raise LookupError('source_post_not_found')

        facts = canonicalize_source(
            str(revision['raw_text'] or ''),
            sanitized_text=str(revision['sanitized_text'] or ''),
            source_identity={
                'source_post_id': source_post_id,
                'source_post_revision_id': int(revision['id']),
                'source_identity_key': str(source_post['source_identity_key']),
                'source_mode': str(source_post['source_mode']),
            },
            media_summary={
                'images': json.loads(str(revision['raw_images_json'] or '[]')),
                'videos': json.loads(str(revision['raw_videos_json'] or '[]')),
            },
        )
        record = self.canonical.append(
            source_post_id=source_post_id,
            source_post_revision_id=int(revision['id']),
            schema_version=str(facts['schema_version']),
            parser_revision=str(facts['parser_revision']),
            facts=facts,
            facts_hash=str(facts['canonical_facts_hash']),
            deal_type=str(facts['deal_type']),
            deal_type_candidates=list(facts.get('deal_type_candidates') or []),
            quality=dict(facts.get('quality') or {}),
            processing_status='PARSED',
        )
        return ParsedCanonical(
            canonical_record_id=record.id,
            source_post_id=source_post_id,
            source_post_revision_id=int(revision['id']),
            deal_type=str(facts['deal_type']),
            facts_hash=str(facts['canonical_facts_hash']),
        )


__all__ = ['CanonicalParserService', 'ParsedCanonical']

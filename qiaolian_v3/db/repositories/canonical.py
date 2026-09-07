from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class CanonicalRecord:
    id: int
    source_post_id: int
    source_post_revision_id: int
    deal_type: str
    processing_status: str
    is_current: bool


class CanonicalRecordRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def append(
        self,
        *,
        source_post_id: int | None = None,
        source_post_revision_id: int,
        schema_version: str,
        parser_revision: str,
        facts: dict,
        facts_hash: str,
        deal_type: str,
        deal_type_candidates: list[str] | None = None,
        quality: dict | None = None,
        processing_status: str = 'PARSED',
        source_identity_id: int | None = None,
    ) -> CanonicalRecord:
        resolved_source_post_id = source_post_id if source_post_id is not None else source_identity_id
        if resolved_source_post_id is None:
            raise TypeError('source_post_id is required')

        previous = self.conn.execute(
            'SELECT id FROM canonical_records WHERE source_post_id=? AND is_current=1',
            (int(resolved_source_post_id),),
        ).fetchone()
        if previous is not None:
            # Historical relationship is represented only by is_current/supersedes_id.
            # Do not rewrite the prior record's lifecycle state to "superseded".
            self.conn.execute(
                'UPDATE canonical_records SET is_current=0 WHERE id=?',
                (int(previous['id']),),
            )

        cur = self.conn.execute(
            '''
            INSERT INTO canonical_records(
                source_post_id,source_post_revision_id,schema_version,parser_revision,
                facts_json,facts_hash,deal_type,deal_type_candidates_json,quality_json,
                processing_status,supersedes_id,is_current
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,1)
            ''',
            (
                int(resolved_source_post_id), int(source_post_revision_id), str(schema_version), str(parser_revision),
                json.dumps(facts, ensure_ascii=False, sort_keys=True), str(facts_hash), str(deal_type),
                json.dumps(deal_type_candidates or [], ensure_ascii=False, sort_keys=True),
                json.dumps(quality or {}, ensure_ascii=False, sort_keys=True), str(processing_status),
                int(previous['id']) if previous is not None else None,
            ),
        )
        return CanonicalRecord(
            id=int(cur.lastrowid),
            source_post_id=int(resolved_source_post_id),
            source_post_revision_id=int(source_post_revision_id),
            deal_type=str(deal_type),
            processing_status=str(processing_status),
            is_current=True,
        )

    def current_for_source(self, source_post_id: int):
        return self.conn.execute(
            'SELECT * FROM canonical_records WHERE source_post_id=? AND is_current=1',
            (int(source_post_id),),
        ).fetchone()

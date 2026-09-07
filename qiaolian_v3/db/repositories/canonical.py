from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class CanonicalRecord:
    id: int
    source_identity_id: int
    source_post_revision_id: int
    deal_type: str
    is_current: bool


class CanonicalRecordRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def append(
        self,
        *,
        source_identity_id: int,
        source_post_revision_id: int,
        schema_version: str,
        parser_revision: str,
        facts: dict,
        facts_hash: str,
        deal_type: str,
        deal_type_candidates: list[str] | None = None,
        quality: dict | None = None,
        processing_status: str = 'parsed',
    ) -> CanonicalRecord:
        previous = self.conn.execute(
            'SELECT id FROM canonical_records WHERE source_identity_id=? AND is_current=1',
            (int(source_identity_id),),
        ).fetchone()
        if previous is not None:
            self.conn.execute(
                "UPDATE canonical_records SET is_current=0, processing_status=CASE WHEN processing_status='invalid' THEN processing_status ELSE 'superseded' END WHERE id=?",
                (int(previous[0]),),
            )
        cur = self.conn.execute(
            '''
            INSERT INTO canonical_records(
                source_identity_id,source_post_revision_id,schema_version,parser_revision,
                facts_json,facts_hash,deal_type,deal_type_candidates_json,quality_json,
                processing_status,supersedes_id,is_current
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,1)
            ''',
            (
                int(source_identity_id), int(source_post_revision_id), str(schema_version), str(parser_revision),
                json.dumps(facts, ensure_ascii=False, sort_keys=True), str(facts_hash), str(deal_type),
                json.dumps(deal_type_candidates or [], ensure_ascii=False, sort_keys=True),
                json.dumps(quality or {}, ensure_ascii=False, sort_keys=True), str(processing_status),
                int(previous[0]) if previous is not None else None,
            ),
        )
        return CanonicalRecord(
            id=int(cur.lastrowid),
            source_identity_id=int(source_identity_id),
            source_post_revision_id=int(source_post_revision_id),
            deal_type=str(deal_type),
            is_current=True,
        )

    def current_for_source(self, source_identity_id: int):
        return self.conn.execute(
            'SELECT * FROM canonical_records WHERE source_identity_id=? AND is_current=1',
            (int(source_identity_id),),
        ).fetchone()

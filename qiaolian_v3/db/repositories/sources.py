from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass


def make_source_identity_key(source_type: str, source_name: str, external_post_id: str) -> str:
    raw = '\x1f'.join((str(source_type).strip(), str(source_name).strip(), str(external_post_id).strip()))
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


@dataclass(frozen=True)
class SourceRevision:
    id: int
    source_identity_id: int
    revision_no: int
    content_hash: str


class SourceRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def register_identity(
        self,
        *,
        legacy_source_post_id: int,
        source_type: str,
        source_name: str,
        external_post_id: str,
    ) -> int:
        key = make_source_identity_key(source_type, source_name, external_post_id)
        self.conn.execute(
            '''
            INSERT INTO source_post_identities(
                source_identity_key, legacy_source_post_id, source_type, source_name, external_post_id
            ) VALUES (?,?,?,?,?)
            ON CONFLICT(source_identity_key) DO NOTHING
            ''',
            (key, int(legacy_source_post_id), str(source_type), str(source_name), str(external_post_id)),
        )
        row = self.conn.execute(
            'SELECT id, legacy_source_post_id FROM source_post_identities WHERE source_identity_key=?',
            (key,),
        ).fetchone()
        if row is None:
            raise RuntimeError('source_identity_insert_failed')
        if int(row[1]) != int(legacy_source_post_id):
            raise sqlite3.IntegrityError('source_identity_key_collision')
        return int(row[0])

    def append_revision(
        self,
        *,
        source_identity_id: int,
        content_hash: str,
        raw_text: str,
        sanitized_text: str = '',
        raw_images: list | None = None,
        raw_videos: list | None = None,
        raw_contact: str = '',
        raw_meta: dict | None = None,
        ingest_origin: str = 'collector',
    ) -> SourceRevision:
        existing = self.conn.execute(
            '''SELECT id,source_identity_id,revision_no,content_hash
               FROM source_post_revisions
               WHERE source_identity_id=? AND content_hash=?''',
            (int(source_identity_id), str(content_hash)),
        ).fetchone()
        if existing is not None:
            return SourceRevision(int(existing[0]), int(existing[1]), int(existing[2]), str(existing[3]))
        next_revision = int(self.conn.execute(
            'SELECT COALESCE(MAX(revision_no),0)+1 FROM source_post_revisions WHERE source_identity_id=?',
            (int(source_identity_id),),
        ).fetchone()[0])
        cur = self.conn.execute(
            '''
            INSERT INTO source_post_revisions(
                source_identity_id,revision_no,content_hash,raw_text,sanitized_text,
                raw_images_json,raw_videos_json,raw_contact,raw_meta_json,ingest_origin
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            ''',
            (
                int(source_identity_id), next_revision, str(content_hash), str(raw_text), str(sanitized_text),
                json.dumps(raw_images or [], ensure_ascii=False, sort_keys=True),
                json.dumps(raw_videos or [], ensure_ascii=False, sort_keys=True),
                str(raw_contact), json.dumps(raw_meta or {}, ensure_ascii=False, sort_keys=True), str(ingest_origin),
            ),
        )
        return SourceRevision(int(cur.lastrowid), int(source_identity_id), next_revision, str(content_hash))

    def get_revision(self, revision_id: int):
        return self.conn.execute('SELECT * FROM source_post_revisions WHERE id=?', (int(revision_id),)).fetchone()

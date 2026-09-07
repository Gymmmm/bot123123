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
    source_post_id: int
    revision_no: int
    source_content_hash: str


class SourceRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def register_source_post(
        self,
        *,
        source_mode: str,
        source_type: str,
        source_name: str,
        external_post_id: str,
        source_url: str = '',
        source_author: str = '',
        dedupe_key: str = '',
        ingest_status: str = 'COLLECTED',
        parse_status: str = 'COLLECTED',
        legacy_source_post_id: int | None = None,
    ) -> int:
        key = make_source_identity_key(source_type, source_name, external_post_id)
        self.conn.execute(
            '''
            INSERT INTO v3_source_posts(
                source_identity_key,source_mode,source_type,source_name,external_post_id,
                source_url,source_author,dedupe_key,ingest_status,parse_status,legacy_source_post_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source_identity_key) DO NOTHING
            ''',
            (
                key, str(source_mode), str(source_type), str(source_name), str(external_post_id),
                str(source_url), str(source_author), str(dedupe_key), str(ingest_status), str(parse_status),
                int(legacy_source_post_id) if legacy_source_post_id is not None else None,
            ),
        )
        row = self.conn.execute(
            '''SELECT id,source_mode,source_type,source_name,external_post_id,legacy_source_post_id
               FROM v3_source_posts WHERE source_identity_key=?''',
            (key,),
        ).fetchone()
        if row is None:
            raise RuntimeError('v3_source_post_insert_failed')
        if str(row['source_mode']) != str(source_mode):
            raise sqlite3.IntegrityError('source_identity_mode_collision')
        if legacy_source_post_id is not None and row['legacy_source_post_id'] not in (None, int(legacy_source_post_id)):
            raise sqlite3.IntegrityError('source_identity_legacy_bridge_collision')
        if legacy_source_post_id is not None and row['legacy_source_post_id'] is None:
            self.conn.execute(
                'UPDATE v3_source_posts SET legacy_source_post_id=?,updated_at=CURRENT_TIMESTAMP WHERE id=?',
                (int(legacy_source_post_id), int(row['id'])),
            )
        return int(row['id'])

    def register_identity(
        self,
        *,
        source_type: str,
        source_name: str,
        external_post_id: str,
        source_mode: str = 'collector',
        legacy_source_post_id: int | None = None,
    ) -> int:
        """Compatibility name for Phase-1 callers; legacy id is optional, never required."""
        return self.register_source_post(
            source_mode=source_mode,
            source_type=source_type,
            source_name=source_name,
            external_post_id=external_post_id,
            legacy_source_post_id=legacy_source_post_id,
        )

    def append_revision(
        self,
        *,
        source_post_id: int | None = None,
        source_content_hash: str | None = None,
        raw_text: str,
        sanitized_text: str = '',
        raw_payload: dict | None = None,
        raw_images: list | None = None,
        raw_videos: list | None = None,
        raw_contact: str = '',
        raw_meta: dict | None = None,
        source_identity_id: int | None = None,
        content_hash: str | None = None,
        ingest_origin: str | None = None,
    ) -> SourceRevision:
        """Append immutable evidence.

        `source_identity_id`, `content_hash`, and `ingest_origin` are accepted only as
        Phase-1 compatibility aliases. The persisted V3 contract is source_post_id +
        source_content_hash; source_mode belongs to v3_source_posts.
        """
        del ingest_origin
        resolved_source_post_id = source_post_id if source_post_id is not None else source_identity_id
        resolved_hash = source_content_hash if source_content_hash is not None else content_hash
        if resolved_source_post_id is None or resolved_hash is None:
            raise TypeError('source_post_id and source_content_hash are required')

        existing = self.conn.execute(
            '''SELECT id,source_post_id,revision_no,source_content_hash
               FROM source_post_revisions
               WHERE source_post_id=? AND source_content_hash=?''',
            (int(resolved_source_post_id), str(resolved_hash)),
        ).fetchone()
        if existing is not None:
            return SourceRevision(
                int(existing['id']), int(existing['source_post_id']),
                int(existing['revision_no']), str(existing['source_content_hash'])
            )

        next_revision = int(self.conn.execute(
            'SELECT COALESCE(MAX(revision_no),0)+1 FROM source_post_revisions WHERE source_post_id=?',
            (int(resolved_source_post_id),),
        ).fetchone()[0])
        cur = self.conn.execute(
            '''
            INSERT INTO source_post_revisions(
                source_post_id,revision_no,source_content_hash,raw_text,sanitized_text,raw_payload_json,
                raw_images_json,raw_videos_json,raw_contact,raw_meta_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            ''',
            (
                int(resolved_source_post_id), next_revision, str(resolved_hash), str(raw_text), str(sanitized_text),
                json.dumps(raw_payload or {}, ensure_ascii=False, sort_keys=True),
                json.dumps(raw_images or [], ensure_ascii=False, sort_keys=True),
                json.dumps(raw_videos or [], ensure_ascii=False, sort_keys=True),
                str(raw_contact), json.dumps(raw_meta or {}, ensure_ascii=False, sort_keys=True),
            ),
        )
        revision_id = int(cur.lastrowid)
        self.conn.execute(
            'UPDATE v3_source_posts SET current_revision_id=?,updated_at=CURRENT_TIMESTAMP WHERE id=?',
            (revision_id, int(resolved_source_post_id)),
        )
        return SourceRevision(revision_id, int(resolved_source_post_id), next_revision, str(resolved_hash))

    def get_source_post(self, source_post_id: int):
        return self.conn.execute('SELECT * FROM v3_source_posts WHERE id=?', (int(source_post_id),)).fetchone()

    def get_revision(self, revision_id: int):
        return self.conn.execute('SELECT * FROM source_post_revisions WHERE id=?', (int(revision_id),)).fetchone()

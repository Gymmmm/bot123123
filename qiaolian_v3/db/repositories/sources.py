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
    source_created_at: str | None
    fetched_at: str


class SourceRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def register_source(
        self,
        *,
        source_type: str,
        source_name: str,
        external_identity: str,
        enabled: bool = True,
        collector_config: dict | None = None,
    ) -> int:
        self.conn.execute(
            '''
            INSERT INTO v3_sources(
                source_type,source_name,external_identity,enabled,collector_config_json
            ) VALUES (?,?,?,?,?)
            ON CONFLICT(source_type,external_identity) DO NOTHING
            ''',
            (
                str(source_type), str(source_name), str(external_identity),
                1 if enabled else 0,
                json.dumps(collector_config or {}, ensure_ascii=False, sort_keys=True),
            ),
        )
        row = self.conn.execute(
            '''SELECT id,source_name,enabled,collector_config_json
               FROM v3_sources WHERE source_type=? AND external_identity=?''',
            (str(source_type), str(external_identity)),
        ).fetchone()
        if row is None:
            raise RuntimeError('v3_source_insert_failed')
        return int(row['id'])

    def get_source(self, source_id: int):
        return self.conn.execute('SELECT * FROM v3_sources WHERE id=?', (int(source_id),)).fetchone()

    def get_source_post_by_identity(
        self,
        *,
        source_type: str,
        source_name: str,
        external_post_id: str,
    ):
        key = make_source_identity_key(source_type, source_name, external_post_id)
        return self.conn.execute(
            'SELECT * FROM v3_source_posts WHERE source_identity_key=?',
            (key,),
        ).fetchone()

    def get_current_revision_for_source_post(self, source_post_id: int):
        return self.conn.execute(
            '''SELECT r.* FROM v3_source_posts p
               JOIN source_post_revisions r ON r.id=p.current_revision_id
               WHERE p.id=?''',
            (int(source_post_id),),
        ).fetchone()

    def register_source_post(
        self,
        *,
        source_mode: str,
        source_type: str,
        source_name: str,
        external_post_id: str,
        source_id: int | None = None,
        source_external_identity: str | None = None,
        source_url: str = '',
        source_author: str = '',
        dedupe_key: str = '',
        ingest_status: str = 'COLLECTED',
        parse_status: str = 'COLLECTED',
        first_seen_at: str | None = None,
        last_seen_at: str | None = None,
        status: str = 'active',
        legacy_source_post_id: int | None = None,
    ) -> int:
        resolved_source_id = source_id
        if resolved_source_id is None:
            resolved_source_id = self.register_source(
                source_type=source_type,
                source_name=source_name,
                external_identity=(
                    str(source_external_identity)
                    if source_external_identity is not None
                    else str(source_name)
                ),
            )
        else:
            source_row = self.get_source(int(resolved_source_id))
            if source_row is None:
                raise sqlite3.IntegrityError('source_registry_not_found')

        key = make_source_identity_key(source_type, source_name, external_post_id)
        self.conn.execute(
            '''
            INSERT INTO v3_source_posts(
                source_id,source_identity_key,source_mode,source_type,source_name,external_post_id,
                source_url,source_author,dedupe_key,ingest_status,parse_status,
                first_seen_at,last_seen_at,status,legacy_source_post_id
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,
                COALESCE(?,CURRENT_TIMESTAMP),COALESCE(?,CURRENT_TIMESTAMP),?,?
            )
            ON CONFLICT(source_identity_key) DO NOTHING
            ''',
            (
                int(resolved_source_id), key, str(source_mode), str(source_type), str(source_name),
                str(external_post_id), str(source_url), str(source_author), str(dedupe_key),
                str(ingest_status), str(parse_status), first_seen_at, last_seen_at,
                str(status), int(legacy_source_post_id) if legacy_source_post_id is not None else None,
            ),
        )
        row = self.conn.execute(
            '''SELECT id,source_id,source_mode,source_type,source_name,external_post_id,
                      first_seen_at,last_seen_at,status,legacy_source_post_id
               FROM v3_source_posts WHERE source_identity_key=?''',
            (key,),
        ).fetchone()
        if row is None:
            raise RuntimeError('v3_source_post_insert_failed')
        if int(row['source_id']) != int(resolved_source_id):
            raise sqlite3.IntegrityError('source_identity_source_collision')
        if str(row['source_mode']) != str(source_mode):
            raise sqlite3.IntegrityError('source_identity_mode_collision')
        if legacy_source_post_id is not None and row['legacy_source_post_id'] not in (None, int(legacy_source_post_id)):
            raise sqlite3.IntegrityError('source_identity_legacy_bridge_collision')

        updates: list[str] = [
            'last_seen_at=COALESCE(?,CURRENT_TIMESTAMP)',
            'status=?',
            'updated_at=CURRENT_TIMESTAMP',
        ]
        params: list[object] = [last_seen_at, str(status)]
        if legacy_source_post_id is not None and row['legacy_source_post_id'] is None:
            updates.append('legacy_source_post_id=?')
            params.append(int(legacy_source_post_id))
        params.append(int(row['id']))
        self.conn.execute(
            f"UPDATE v3_source_posts SET {','.join(updates)} WHERE id=?",
            tuple(params),
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
        source_created_at: str | None = None,
        fetched_at: str | None = None,
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
            '''SELECT id,source_post_id,revision_no,source_content_hash,source_created_at,fetched_at
               FROM source_post_revisions
               WHERE source_post_id=? AND source_content_hash=?''',
            (int(resolved_source_post_id), str(resolved_hash)),
        ).fetchone()
        if existing is not None:
            return SourceRevision(
                int(existing['id']), int(existing['source_post_id']),
                int(existing['revision_no']), str(existing['source_content_hash']),
                existing['source_created_at'], str(existing['fetched_at']),
            )

        next_revision = int(self.conn.execute(
            'SELECT COALESCE(MAX(revision_no),0)+1 FROM source_post_revisions WHERE source_post_id=?',
            (int(resolved_source_post_id),),
        ).fetchone()[0])
        cur = self.conn.execute(
            '''
            INSERT INTO source_post_revisions(
                source_post_id,revision_no,source_content_hash,raw_text,sanitized_text,raw_payload_json,
                raw_images_json,raw_videos_json,raw_contact,raw_meta_json,source_created_at,fetched_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,COALESCE(?,CURRENT_TIMESTAMP))
            ''',
            (
                int(resolved_source_post_id), next_revision, str(resolved_hash), str(raw_text), str(sanitized_text),
                json.dumps(raw_payload or {}, ensure_ascii=False, sort_keys=True),
                json.dumps(raw_images or [], ensure_ascii=False, sort_keys=True),
                json.dumps(raw_videos or [], ensure_ascii=False, sort_keys=True),
                str(raw_contact), json.dumps(raw_meta or {}, ensure_ascii=False, sort_keys=True),
                source_created_at, fetched_at,
            ),
        )
        revision_id = int(cur.lastrowid)
        self.conn.execute(
            'UPDATE v3_source_posts SET current_revision_id=?,updated_at=CURRENT_TIMESTAMP WHERE id=?',
            (revision_id, int(resolved_source_post_id)),
        )
        row = self.conn.execute(
            '''SELECT source_created_at,fetched_at FROM source_post_revisions WHERE id=?''',
            (revision_id,),
        ).fetchone()
        if row is None:
            raise RuntimeError('source_revision_insert_failed')
        return SourceRevision(
            revision_id, int(resolved_source_post_id), next_revision, str(resolved_hash),
            row['source_created_at'], str(row['fetched_at']),
        )

    def get_source_post(self, source_post_id: int):
        return self.conn.execute('SELECT * FROM v3_source_posts WHERE id=?', (int(source_post_id),)).fetchone()

    def get_revision(self, revision_id: int):
        return self.conn.execute('SELECT * FROM source_post_revisions WHERE id=?', (int(revision_id),)).fetchone()

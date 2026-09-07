from __future__ import annotations

import sqlite3


class SourceMediaRepository:
    """Persistence adapter for immutable revision ↔ media-hash links."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def link_revision_media(self, *, revision_id: int, media_asset_key: str, sort_order: int) -> None:
        self.conn.execute(
            '''INSERT OR IGNORE INTO source_post_media(
               source_post_revision_id,media_asset_key,sort_order
               ) VALUES (?,?,?)''',
            (int(revision_id), str(media_asset_key), int(sort_order)),
        )

    def list_for_revision(self, revision_id: int):
        return self.conn.execute(
            '''SELECT * FROM source_post_media
               WHERE source_post_revision_id=? ORDER BY sort_order,id''',
            (int(revision_id),),
        ).fetchall()

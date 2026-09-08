from __future__ import annotations

import sqlite3


class PublicationHistoryService:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def recent(self, *, limit: int = 50):
        return self.conn.execute(
            '''SELECT p.package_id,p.status,p.listing_id,p.offer_id,p.target_channel_id,p.content_hash,
                      c.message_id,c.post_status
               FROM v3_publication_packages p
               LEFT JOIN v3_channel_posts c ON c.current_package_id=p.id
               ORDER BY p.created_at DESC,p.id DESC LIMIT ?''',
            (max(1, int(limit)),),
        ).fetchall()


__all__ = ['PublicationHistoryService']

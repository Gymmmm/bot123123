from __future__ import annotations

import sqlite3


class ListingManagementService:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def list_recent(self, *, limit: int = 50):
        return self.conn.execute(
            '''SELECT l.*,o.id AS offer_id,o.offer_type,o.publication_policy
               FROM v3_listings l
               LEFT JOIN listing_offers o ON o.listing_id=l.id AND o.is_current=1
               ORDER BY l.updated_at DESC,l.id DESC LIMIT ?''',
            (max(1, int(limit)),),
        ).fetchall()


__all__ = ['ListingManagementService']

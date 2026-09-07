from __future__ import annotations

import sqlite3

from .repositories import CanonicalRecordRepository, ListingOfferRepository, SourceRepository


class UnitOfWork:
    """Transaction boundary for V3 repositories only."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.sources = SourceRepository(conn)
        self.canonical = CanonicalRecordRepository(conn)
        self.offers = ListingOfferRepository(conn)
        self._committed = False

    def __enter__(self) -> 'UnitOfWork':
        self.conn.execute('BEGIN')
        return self

    def commit(self) -> None:
        self.conn.commit()
        self._committed = True

    def rollback(self) -> None:
        self.conn.rollback()

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None or not self._committed:
            self.conn.rollback()

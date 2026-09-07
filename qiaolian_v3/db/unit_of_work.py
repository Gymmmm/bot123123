from __future__ import annotations

import sqlite3
import uuid

from .repositories import CanonicalRecordRepository, ListingOfferRepository, SourceRepository


class UnitOfWork:
    """SAVEPOINT-backed transaction boundary for V3 repositories only.

    SAVEPOINT is deliberate: Phase 1 repository calls must compose safely with
    callers that already have an open SQLite transaction. Exiting without an
    explicit commit always rolls back V3 work to this boundary.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.sources = SourceRepository(conn)
        self.canonical = CanonicalRecordRepository(conn)
        self.offers = ListingOfferRepository(conn)
        self._savepoint = f"qiaolian_v3_uow_{uuid.uuid4().hex}"
        self._entered = False
        self._closed = False
        self._committed = False

    def __enter__(self) -> 'UnitOfWork':
        if self._entered:
            raise RuntimeError('unit_of_work_cannot_be_reentered')
        self.conn.execute(f'SAVEPOINT {self._savepoint}')
        self._entered = True
        return self

    def commit(self) -> None:
        if not self._entered or self._closed:
            raise RuntimeError('unit_of_work_not_active')
        self.conn.execute(f'RELEASE SAVEPOINT {self._savepoint}')
        self._committed = True
        self._closed = True

    def rollback(self) -> None:
        if not self._entered or self._closed:
            return
        self.conn.execute(f'ROLLBACK TO SAVEPOINT {self._savepoint}')
        self.conn.execute(f'RELEASE SAVEPOINT {self._savepoint}')
        self._closed = True

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None or not self._committed:
            self.rollback()

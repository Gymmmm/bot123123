from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class ReviewItem:
    id: int
    review_type: str
    status: str
    source_mode: str
    canonical_record_id: int | None
    listing_id: int | None
    offer_id: int | None
    reason_codes: tuple[str, ...]


class ReviewRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def create(
        self,
        *,
        review_type: str,
        source_mode: str,
        reason_codes: list[str] | tuple[str, ...],
        canonical_record_id: int | None = None,
        listing_id: int | None = None,
        offer_id: int | None = None,
        operator_id: str = '',
    ) -> ReviewItem:
        cur = self.conn.execute(
            '''INSERT INTO review_items(
                review_type,canonical_record_id,listing_id,offer_id,
                reason_codes_json,status,source_mode,operator_id
            ) VALUES (?,?,?,?,?,'open',?,?)''',
            (
                str(review_type), canonical_record_id, listing_id, offer_id,
                json.dumps(list(reason_codes), ensure_ascii=False, sort_keys=True),
                str(source_mode), str(operator_id),
            ),
        )
        return self.get(int(cur.lastrowid))

    def get(self, review_id: int) -> ReviewItem:
        row = self.conn.execute('SELECT * FROM review_items WHERE id=?', (int(review_id),)).fetchone()
        if row is None:
            raise LookupError('review_item_not_found')
        return ReviewItem(
            id=int(row['id']), review_type=str(row['review_type']), status=str(row['status']),
            source_mode=str(row['source_mode']), canonical_record_id=row['canonical_record_id'],
            listing_id=row['listing_id'], offer_id=row['offer_id'],
            reason_codes=tuple(json.loads(str(row['reason_codes_json']))),
        )

    def list_open(self) -> tuple[ReviewItem, ...]:
        rows = self.conn.execute("SELECT id FROM review_items WHERE status='open' ORDER BY id").fetchall()
        return tuple(self.get(int(row['id'])) for row in rows)

    def resolve(self, review_id: int, *, status: str, operator_id: str, resolution: dict | None = None) -> ReviewItem:
        if status not in {'approved', 'rejected', 'resolved'}:
            raise ValueError('invalid_review_resolution')
        cur = self.conn.execute(
            '''UPDATE review_items SET status=?,operator_id=?,resolution_json=?,resolved_at=CURRENT_TIMESTAMP
               WHERE id=? AND status='open' ''',
            (str(status), str(operator_id), json.dumps(resolution or {}, ensure_ascii=False, sort_keys=True), int(review_id)),
        )
        if cur.rowcount != 1:
            raise ValueError('review_item_not_open')
        return self.get(review_id)


__all__ = ['ReviewItem', 'ReviewRepository']

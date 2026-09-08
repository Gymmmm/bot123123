from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from .package import FrozenPublicationPackage


class DeliveryBlocked(RuntimeError):
    pass


@dataclass(frozen=True)
class DeliveryState:
    package_id: str
    state: str
    message_id: int | None = None


class DeliveryCoordinator:
    """Durable delivery using Phase-1 package/channel-post persistence.

    `publishing` is intentionally the unknown boundary. A retry from that state is
    blocked until explicit reconciliation supplies the Telegram message id.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def _package_row(self, package: FrozenPublicationPackage):
        return self.conn.execute(
            'SELECT * FROM v3_publication_packages WHERE package_id=?',
            (package.package_id,),
        ).fetchone()

    def begin(self, package: FrozenPublicationPackage) -> DeliveryState:
        if not package.frozen or package.deal_type != 'rent' or package.offer_type != 'rent':
            raise DeliveryBlocked('delivery_requires_frozen_rent_package')
        row = self._package_row(package)
        if row is None:
            raise DeliveryBlocked('package_not_persisted')
        if str(row['content_hash']) != package.content_hash:
            raise DeliveryBlocked('package_content_hash_mismatch')
        state = str(row['status'])
        if state == 'published':
            post = self.conn.execute(
                'SELECT message_id FROM v3_channel_posts WHERE current_package_id=?',
                (int(row['id']),),
            ).fetchone()
            return DeliveryState(package.package_id, 'published', int(post['message_id']) if post else None)
        if state == 'publishing':
            raise DeliveryBlocked('delivery_unknown_requires_reconcile')
        if state != 'frozen':
            raise DeliveryBlocked(f'package_not_sendable:{state}')
        self.conn.execute(
            "UPDATE v3_publication_packages SET status='publishing' WHERE id=? AND status='frozen'",
            (int(row['id']),),
        )
        return DeliveryState(package.package_id, 'publishing')

    def failed_before_send(self, package: FrozenPublicationPackage) -> None:
        cur = self.conn.execute(
            "UPDATE v3_publication_packages SET status='frozen' WHERE package_id=? AND status='publishing'",
            (package.package_id,),
        )
        if cur.rowcount != 1:
            raise DeliveryBlocked('failed_before_send_transition_blocked')

    def commit_sent(self, package: FrozenPublicationPackage, *, message_id: int) -> DeliveryState:
        if int(message_id) <= 0:
            raise DeliveryBlocked('invalid_message_id')
        row = self._package_row(package)
        if row is None or str(row['status']) != 'publishing':
            raise DeliveryBlocked('delivery_not_in_publishing_state')
        if str(row['content_hash']) != package.content_hash:
            raise DeliveryBlocked('package_content_hash_mismatch')
        existing = self.conn.execute(
            'SELECT * FROM v3_channel_posts WHERE current_package_id=?', (int(row['id']),)
        ).fetchone()
        if existing is not None:
            if int(existing['message_id']) != int(message_id):
                raise DeliveryBlocked('durable_receipt_conflict')
        else:
            self.conn.execute(
                '''INSERT INTO v3_channel_posts(
                    idempotency_key,channel_id,message_id,listing_id,offer_id,current_package_id,
                    publication_kind,content_hash,published_at,last_synced_at
                ) VALUES (?,?,?,?,?,?,'telegram_rent',?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)''',
                (
                    f'{package.package_id}:{package.target_channel_id}', package.target_channel_id,
                    int(message_id), package.listing_id, package.offer_id, int(row['id']), package.content_hash,
                ),
            )
        self.conn.execute(
            "UPDATE v3_publication_packages SET status='published',published_at=COALESCE(published_at,CURRENT_TIMESTAMP) WHERE id=?",
            (int(row['id']),),
        )
        return DeliveryState(package.package_id, 'published', int(message_id))

    def reconcile_unknown(self, package: FrozenPublicationPackage, *, channel_id: str, message_id: int, observed_content_hash: str) -> DeliveryState:
        if str(channel_id) != package.target_channel_id:
            raise DeliveryBlocked('reconcile_channel_mismatch')
        if str(observed_content_hash) != package.content_hash:
            raise DeliveryBlocked('reconcile_content_hash_mismatch')
        return self.commit_sent(package, message_id=int(message_id))


__all__ = ['DeliveryBlocked','DeliveryCoordinator','DeliveryState']

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from .package import FrozenPublicationPackage


class DeliveryBlocked(RuntimeError):
    pass


@dataclass(frozen=True)
class DeliveryState:
    package_id: str
    state: str
    message_id: int | None = None


class DeliveryCoordinator:
    """Durable delivery coordinator.

    The package row is the send-ownership CAS. ``publishing`` means the external
    outcome is not safe to retry blindly. A successful Telegram result is first
    persisted as a ``v3_channel_posts`` durable receipt with
    ``post_status='receipt_saved'`` and committed before the final package/post
    publication transaction.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.conn.row_factory = sqlite3.Row

    def _package_row(self, package: FrozenPublicationPackage):
        return self.conn.execute(
            'SELECT * FROM v3_publication_packages WHERE package_id=?',
            (package.package_id,),
        ).fetchone()

    def _validate_package(self, package: FrozenPublicationPackage, row) -> None:
        if not package.frozen or package.deal_type != 'rent' or package.offer_type != 'rent':
            raise DeliveryBlocked('delivery_requires_frozen_rent_package')
        if row is None:
            raise DeliveryBlocked('package_not_persisted')
        checks = (
            (str(row['content_hash']), package.content_hash, 'package_content_hash_mismatch'),
            (str(row['target_channel_id']), package.target_channel_id, 'package_channel_mismatch'),
            (int(row['listing_id']), package.listing_id, 'package_listing_mismatch'),
            (int(row['offer_id']), package.offer_id, 'package_offer_mismatch'),
        )
        for actual, expected, reason in checks:
            if actual != expected:
                raise DeliveryBlocked(reason)

    def _receipt(self, row):
        return self.conn.execute(
            'SELECT * FROM v3_channel_posts WHERE current_package_id=?',
            (int(row['id']),),
        ).fetchone()

    def begin(self, package: FrozenPublicationPackage) -> DeliveryState:
        row = self._package_row(package)
        self._validate_package(package, row)
        state = str(row['status'])
        if state == 'published':
            post = self._receipt(row)
            return DeliveryState(package.package_id, 'published', int(post['message_id']) if post else None)
        if state == 'publishing':
            receipt = self._receipt(row)
            if receipt is not None:
                return DeliveryState(package.package_id, 'receipt_saved', int(receipt['message_id']))
            raise DeliveryBlocked('delivery_unknown_requires_reconcile')
        if state != 'frozen':
            raise DeliveryBlocked(f'package_not_sendable:{state}')

        cur = self.conn.execute(
            "UPDATE v3_publication_packages SET status='publishing' WHERE id=? AND status='frozen'",
            (int(row['id']),),
        )
        self.conn.commit()
        if cur.rowcount == 1:
            return DeliveryState(package.package_id, 'publishing')

        # Another connection won the CAS. Re-read the durable state; never let a
        # CAS loser proceed to Telegram send.
        current = self._package_row(package)
        self._validate_package(package, current)
        current_state = str(current['status'])
        if current_state == 'published':
            post = self._receipt(current)
            return DeliveryState(package.package_id, 'published', int(post['message_id']) if post else None)
        if current_state == 'publishing':
            receipt = self._receipt(current)
            if receipt is not None:
                return DeliveryState(package.package_id, 'receipt_saved', int(receipt['message_id']))
            raise DeliveryBlocked('delivery_send_ownership_not_acquired')
        raise DeliveryBlocked(f'delivery_send_ownership_invalid_state:{current_state}')

    def failed_before_send(self, package: FrozenPublicationPackage) -> None:
        row = self._package_row(package)
        self._validate_package(package, row)
        if self._receipt(row) is not None:
            raise DeliveryBlocked('failed_before_send_has_durable_receipt')
        cur = self.conn.execute(
            "UPDATE v3_publication_packages SET status='frozen' WHERE id=? AND status='publishing'",
            (int(row['id']),),
        )
        self.conn.commit()
        if cur.rowcount != 1:
            raise DeliveryBlocked('failed_before_send_transition_blocked')

    def save_sent_result(self, package: FrozenPublicationPackage, *, message_id: int) -> DeliveryState:
        if int(message_id) <= 0:
            raise DeliveryBlocked('invalid_message_id')
        row = self._package_row(package)
        self._validate_package(package, row)
        if str(row['status']) == 'published':
            receipt = self._receipt(row)
            if receipt is None or int(receipt['message_id']) != int(message_id):
                raise DeliveryBlocked('durable_receipt_conflict')
            return DeliveryState(package.package_id, 'published', int(message_id))
        if str(row['status']) != 'publishing':
            raise DeliveryBlocked('delivery_not_in_publishing_state')

        existing = self._receipt(row)
        if existing is not None:
            self._validate_receipt(package, row, existing, message_id=message_id)
            return DeliveryState(package.package_id, 'receipt_saved', int(existing['message_id']))

        try:
            self.conn.execute(
                '''INSERT INTO v3_channel_posts(
                    idempotency_key,channel_id,message_id,listing_id,offer_id,current_package_id,
                    publication_kind,content_hash,post_status,last_synced_at
                ) VALUES (?,?,?,?,?,?,'telegram_rent',?,'receipt_saved',CURRENT_TIMESTAMP)''',
                (
                    f'{package.package_id}:{package.target_channel_id}', package.target_channel_id,
                    int(message_id), package.listing_id, package.offer_id, int(row['id']), package.content_hash,
                ),
            )
            # This commit is the crash boundary: the Telegram receipt is durable
            # before any final package publication state is changed.
            self.conn.commit()
        except sqlite3.IntegrityError as exc:
            self.conn.rollback()
            existing = self._receipt(row)
            if existing is None:
                raise DeliveryBlocked('durable_receipt_persist_failed') from exc
            self._validate_receipt(package, row, existing, message_id=message_id)
        return DeliveryState(package.package_id, 'receipt_saved', int(message_id))

    def _validate_receipt(self, package: FrozenPublicationPackage, row, receipt, *, message_id: int | None = None) -> None:
        expected = (
            str(package.target_channel_id), package.listing_id, package.offer_id,
            int(row['id']), package.content_hash,
        )
        actual = (
            str(receipt['channel_id']), int(receipt['listing_id']), int(receipt['offer_id']),
            int(receipt['current_package_id']), str(receipt['content_hash']),
        )
        if actual != expected:
            raise DeliveryBlocked('durable_receipt_identity_mismatch')
        if message_id is not None and int(receipt['message_id']) != int(message_id):
            raise DeliveryBlocked('durable_receipt_message_mismatch')

    def commit_saved_result(self, package: FrozenPublicationPackage, *, message_id: int | None = None) -> DeliveryState:
        row = self._package_row(package)
        self._validate_package(package, row)
        receipt = self._receipt(row)
        if receipt is None:
            raise DeliveryBlocked('durable_receipt_missing')
        self._validate_receipt(package, row, receipt, message_id=message_id)
        if str(row['status']) == 'published':
            if str(receipt['post_status']) != 'published':
                raise DeliveryBlocked('published_package_receipt_not_published')
            return DeliveryState(package.package_id, 'published', int(receipt['message_id']))
        if str(row['status']) != 'publishing':
            raise DeliveryBlocked(f'durable_receipt_package_state_invalid:{row["status"]}')

        self.conn.execute('BEGIN IMMEDIATE')
        current = self._package_row(package)
        current_receipt = self._receipt(current)
        self._validate_package(package, current)
        if current_receipt is None:
            self.conn.rollback()
            raise DeliveryBlocked('durable_receipt_missing')
        self._validate_receipt(package, current, current_receipt, message_id=message_id)
        if str(current['status']) == 'published':
            self.conn.commit()
            return DeliveryState(package.package_id, 'published', int(current_receipt['message_id']))
        if str(current['status']) != 'publishing':
            self.conn.rollback()
            raise DeliveryBlocked(f'durable_receipt_package_state_invalid:{current["status"]}')
        self.conn.execute(
            "UPDATE v3_channel_posts SET post_status='published',published_at=COALESCE(published_at,CURRENT_TIMESTAMP),last_synced_at=CURRENT_TIMESTAMP WHERE id=?",
            (int(current_receipt['id']),),
        )
        cur = self.conn.execute(
            "UPDATE v3_publication_packages SET status='published',published_at=COALESCE(published_at,CURRENT_TIMESTAMP) WHERE id=? AND status='publishing'",
            (int(current['id']),),
        )
        if cur.rowcount != 1:
            self.conn.rollback()
            raise DeliveryBlocked('delivery_final_commit_lost_ownership')
        self.conn.commit()
        return DeliveryState(package.package_id, 'published', int(current_receipt['message_id']))

    def recover_saved_result(self, package: FrozenPublicationPackage) -> DeliveryState | None:
        row = self._package_row(package)
        self._validate_package(package, row)
        state = str(row['status'])
        receipt = self._receipt(row)
        if state == 'published':
            if receipt is None:
                raise DeliveryBlocked('published_package_missing_receipt')
            self._validate_receipt(package, row, receipt)
            return DeliveryState(package.package_id, 'published', int(receipt['message_id']))
        if receipt is not None:
            self._validate_receipt(package, row, receipt)
            if state != 'publishing':
                raise DeliveryBlocked('durable_receipt_unexpected_package_state')
            return self.commit_saved_result(package)
        if state == 'publishing':
            raise DeliveryBlocked('delivery_unknown_requires_reconcile')
        if state == 'frozen':
            return None
        raise DeliveryBlocked(f'package_not_recoverable:{state}')

    def commit_sent(self, package: FrozenPublicationPackage, *, message_id: int) -> DeliveryState:
        self.save_sent_result(package, message_id=message_id)
        return self.commit_saved_result(package, message_id=message_id)

    def reconcile_unknown(self, package: FrozenPublicationPackage, *, channel_id: str, message_id: int, observed_content_hash: str) -> DeliveryState:
        if str(channel_id) != package.target_channel_id:
            raise DeliveryBlocked('reconcile_channel_mismatch')
        if str(observed_content_hash) != package.content_hash:
            raise DeliveryBlocked('reconcile_content_hash_mismatch')
        self.save_sent_result(package, message_id=int(message_id))
        return self.commit_saved_result(package, message_id=int(message_id))


__all__ = ['DeliveryBlocked','DeliveryCoordinator','DeliveryState']

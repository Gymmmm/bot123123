from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from .channel_editor import ChannelEditor, ChannelEditPlan
from .package import FrozenPublicationPackage


class SyncBlocked(RuntimeError):
    pass


@dataclass(frozen=True)
class SyncLease:
    owner: str
    key: str
    expected_content_hash: str = ''


class SyncService:
    """DB-backed optimistic ownership for exact channel-post sync."""

    def __init__(self, editor: ChannelEditor, conn: sqlite3.Connection) -> None:
        self.editor = editor
        self.conn = conn
        self.conn.row_factory = sqlite3.Row

    def acquire(self, *, key: str, owner: str, expected_content_hash: str = '') -> SyncLease:
        key = str(key)
        owner = str(owner)
        if not key or not owner:
            raise SyncBlocked('sync_lease_identity_required')
        cur = self.conn.execute(
            '''INSERT INTO v3_sync_leases(lease_key,owner,expected_content_hash)
               VALUES (?,?,?) ON CONFLICT(lease_key) DO NOTHING''',
            (key, owner, str(expected_content_hash or '')),
        )
        self.conn.commit()
        if cur.rowcount != 1:
            row = self.conn.execute(
                'SELECT owner FROM v3_sync_leases WHERE lease_key=?', (key,)
            ).fetchone()
            if row is None or str(row['owner']) != owner:
                raise SyncBlocked('concurrent_worker_single_owner')
        return SyncLease(owner=owner, key=key, expected_content_hash=str(expected_content_hash or ''))

    def _assert_owned(self, lease: SyncLease) -> None:
        row = self.conn.execute(
            'SELECT owner,expected_content_hash FROM v3_sync_leases WHERE lease_key=?',
            (lease.key,),
        ).fetchone()
        if row is None or str(row['owner']) != lease.owner:
            raise SyncBlocked('sync_lease_not_owned')
        if str(row['expected_content_hash'] or '') != lease.expected_content_hash:
            raise SyncBlocked('sync_lease_hash_changed')

    def release(self, lease: SyncLease) -> None:
        self.conn.execute(
            'DELETE FROM v3_sync_leases WHERE lease_key=? AND owner=?',
            (lease.key, lease.owner),
        )
        self.conn.commit()

    def plan_sync(
        self, *, lease: SyncLease, package: FrozenPublicationPackage,
        channel_id: str, message_id: int, expected_previous_hash: str,
    ) -> ChannelEditPlan:
        self._assert_owned(lease)
        row = self.conn.execute(
            '''SELECT channel_id,message_id,listing_id,offer_id,content_hash
               FROM v3_channel_posts WHERE channel_id=? AND message_id=?''',
            (str(channel_id), int(message_id)),
        ).fetchone()
        if row is None:
            raise SyncBlocked('channel_post_mapping_missing')
        mapping = dict(row)
        if lease.expected_content_hash and str(row['content_hash']) != lease.expected_content_hash:
            raise SyncBlocked('sync_lease_previous_hash_conflict')
        return self.editor.plan_edit(
            package=package,
            mapping=mapping,
            channel_id=str(channel_id),
            message_id=int(message_id),
            expected_previous_hash=str(expected_previous_hash),
        )

    def execute_sync(self, *, lease: SyncLease, plan: ChannelEditPlan, package: FrozenPublicationPackage):
        self._assert_owned(lease)
        row = self.conn.execute(
            '''SELECT listing_id,offer_id,content_hash FROM v3_channel_posts
               WHERE channel_id=? AND message_id=?''',
            (plan.channel_id, int(plan.message_id)),
        ).fetchone()
        if row is None:
            raise SyncBlocked('channel_post_mapping_missing')
        if int(row['listing_id']) != plan.listing_id or int(row['offer_id']) != plan.offer_id:
            raise SyncBlocked('sync_mapping_changed')
        if str(row['content_hash']) != plan.previous_content_hash:
            raise SyncBlocked('sync_previous_hash_changed')
        result = self.editor.execute(plan, package)
        if plan.action == 'edit' and not (plan.dry_run or self.editor.dry_run):
            cur = self.conn.execute(
                '''UPDATE v3_channel_posts
                   SET content_hash=?,current_package_id=(SELECT id FROM v3_publication_packages WHERE package_id=?),
                       last_synced_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
                   WHERE channel_id=? AND message_id=? AND listing_id=? AND offer_id=? AND content_hash=?''',
                (
                    package.content_hash, package.package_id, plan.channel_id, int(plan.message_id),
                    plan.listing_id, plan.offer_id, plan.previous_content_hash,
                ),
            )
            self.conn.commit()
            if cur.rowcount != 1:
                raise SyncBlocked('sync_commit_optimistic_conflict')
        return result


__all__ = ['SyncBlocked','SyncLease','SyncService']

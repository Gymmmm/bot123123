from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .channel_editor import ChannelEditor, ChannelEditPlan
from .package import FrozenPublicationPackage


class SyncBlocked(RuntimeError):
    pass


@dataclass(frozen=True)
class SyncLease:
    owner: str
    key: str


class SyncService:
    """In-process optimistic ownership used to make one worker the sole planner.

    Persistent mapping/hash validation is still performed by ChannelEditor. The
    lease prevents two concurrent workers in the same service instance from
    planning the same mapping simultaneously.
    """
    def __init__(self, editor: ChannelEditor) -> None:
        self.editor = editor
        self._owners: dict[str, str] = {}

    def acquire(self, *, key: str, owner: str) -> SyncLease:
        current = self._owners.get(str(key))
        if current and current != str(owner):
            raise SyncBlocked('concurrent_worker_single_owner')
        self._owners[str(key)] = str(owner)
        return SyncLease(owner=str(owner), key=str(key))

    def release(self, lease: SyncLease) -> None:
        if self._owners.get(lease.key) == lease.owner:
            self._owners.pop(lease.key, None)

    def plan_sync(
        self, *, lease: SyncLease, package: FrozenPublicationPackage,
        mapping: dict[str, Any], channel_id: str, message_id: int,
        expected_previous_hash: str,
    ) -> ChannelEditPlan:
        if self._owners.get(lease.key) != lease.owner:
            raise SyncBlocked('sync_lease_not_owned')
        return self.editor.plan_edit(
            package=package, mapping=mapping, channel_id=channel_id,
            message_id=message_id, expected_previous_hash=expected_previous_hash,
        )


__all__ = ['SyncBlocked','SyncLease','SyncService']

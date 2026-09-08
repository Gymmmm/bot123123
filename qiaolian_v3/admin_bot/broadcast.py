from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any


class BroadcastBlocked(RuntimeError):
    pass


@dataclass(frozen=True)
class BroadcastPreview:
    text: str
    audience: str
    preview_hash: str


class BroadcastService:
    def __init__(self, *, gateway: Any, dry_run: bool = True) -> None:
        self.gateway = gateway
        self.dry_run = bool(dry_run)

    def preview(self, *, text: str, audience: str = 'all_users') -> BroadcastPreview:
        normalized = str(text).strip()
        if not normalized:
            raise BroadcastBlocked('empty_broadcast')
        digest = hashlib.sha256(f'{audience}\n{normalized}'.encode('utf-8')).hexdigest()
        return BroadcastPreview(text=normalized, audience=str(audience), preview_hash=digest)

    def send(self, preview: BroadcastPreview, *, confirm: bool = False) -> dict[str, object]:
        if not confirm:
            raise BroadcastBlocked('explicit_confirm_required')
        if self.dry_run:
            return {'action': 'broadcast', 'dry_run': True, 'writes': 0, 'preview_hash': preview.preview_hash}
        result = self.gateway.broadcast(text=preview.text, audience=preview.audience)
        return {'action': 'broadcast', 'dry_run': False, 'writes': 1, 'result': result}


__all__ = ['BroadcastBlocked', 'BroadcastPreview', 'BroadcastService']

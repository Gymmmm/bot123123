from __future__ import annotations

from typing import Any


class DiscussionPublisher:
    def __init__(self, *, gateway: Any, enabled: bool = False) -> None:
        self.gateway = gateway
        self.enabled = bool(enabled)

    def publish(self, **payload) -> dict[str, Any]:
        if not self.enabled:
            return {'status': 'disabled', 'writes': 0}
        result = self.gateway.send(**payload)
        return {'status': 'sent', 'result': result}


__all__ = ['DiscussionPublisher']

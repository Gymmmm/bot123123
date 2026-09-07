from __future__ import annotations


class FakeTelegramGateway:
    """In-memory Phase 0 test double. It performs no network I/O."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def send(self, **payload):
        self.calls.append(("send", dict(payload)))
        return {"ok": True, "message_id": len(self.calls)}

    def edit(self, **payload):
        self.calls.append(("edit", dict(payload)))
        return {"ok": True}

    @property
    def write_count(self) -> int:
        return len(self.calls)

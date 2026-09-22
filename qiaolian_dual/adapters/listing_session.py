"""Adapter 2: search-result cursor over QL ids. Not inventory IO."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ListingSessionAdapter:
    public_ids: list[str] = field(default_factory=list)
    index: int = 0

    def set_results(self, public_ids: list[str], index: int = 0) -> None:
        self.public_ids = [str(item).strip() for item in public_ids if str(item).strip()]
        if not self.public_ids:
            self.index = 0
            return
        self.index = max(0, min(int(index), len(self.public_ids) - 1))

    def current(self) -> str | None:
        if not self.public_ids:
            return None
        return self.public_ids[self.index]

    def drop_unavailable(self, unavailable: set[str]) -> str | None:
        blocked = {str(item).strip() for item in unavailable}
        self.public_ids = [item for item in self.public_ids if item not in blocked]
        if not self.public_ids:
            self.index = 0
            return None
        if self.index >= len(self.public_ids):
            self.index = len(self.public_ids) - 1
        return self.current()

    def prev(self) -> str | None:
        if not self.public_ids:
            return None
        self.index = (self.index - 1) % len(self.public_ids)
        return self.current()

    def next(self) -> str | None:
        if not self.public_ids:
            return None
        self.index = (self.index + 1) % len(self.public_ids)
        return self.current()

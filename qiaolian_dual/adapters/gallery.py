"""Adapter 3: frozen gallery cursor + Telegram media/text boundary."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class GalleryAdapter:
    items: tuple[str, ...] = ()
    index: int = 0
    showing_media: bool = False

    def load(self, gallery: tuple[str, ...] | list[str], *, cover_first: bool = True) -> None:
        raw = tuple(str(item).strip() for item in gallery if str(item).strip())
        self.items = raw
        self.index = 0 if raw else 0
        self.showing_media = False
        if cover_first and self.items:
            self.index = 0

    def current(self) -> str | None:
        if not self.items:
            return None
        return self.items[self.index]

    def prev(self) -> str | None:
        if not self.items:
            return None
        self.index = (self.index - 1) % len(self.items)
        self.showing_media = True
        return self.current()

    def next(self) -> str | None:
        if not self.items:
            return None
        self.index = (self.index + 1) % len(self.items)
        self.showing_media = True
        return self.current()

    def back_to_text_panel(self) -> None:
        self.showing_media = False

    def should_edit_media(self) -> bool:
        return bool(self.showing_media and self.current())

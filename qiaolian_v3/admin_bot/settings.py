from __future__ import annotations


class AdminSettingsService:
    def __init__(self, settings: dict[str, object] | None = None) -> None:
        self._settings = dict(settings or {})

    def get_all(self) -> dict[str, object]:
        return dict(self._settings)

    def update(self, **values: object) -> dict[str, object]:
        self._settings.update(values)
        return self.get_all()


__all__ = ['AdminSettingsService']

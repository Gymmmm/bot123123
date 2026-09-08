from __future__ import annotations


class CollectorManagementService:
    def __init__(self, source_repository) -> None:
        self.source_repository = source_repository

    def describe(self) -> dict[str, object]:
        return {'mode': 'configuration_only', 'telegram_writes': 0}


__all__ = ['CollectorManagementService']

"""V3 ingest boundary."""

from .source_service import IngestDisposition, IngestResult, SourceIngestService
from .telegram_collector import TelegramCollector

__all__ = [
    "IngestDisposition",
    "IngestResult",
    "SourceIngestService",
    "TelegramCollector",
]

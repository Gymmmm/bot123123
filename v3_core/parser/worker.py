"""Independent pending-source worker for V3 canonical/inventory processing."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import sqlite3

from v3_core.ingest.source_repository import SourceRepository
from v3_core.inventory.identity import IdentityService
from v3_core.inventory.service import InventoryMaterializationService
from v3_core.parser.service import CanonicalParseService
from v3_core.storage.inventory_repository import InventoryRepository


@dataclass(frozen=True)
class WorkerItemResult:
    source_post_id: int
    status: str
    listing_id: str = ""
    error: str = ""


class CanonicalWorker:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        self.sources = SourceRepository(self.db_path)
        self.inventory = InventoryRepository(self.db_path)
        self.identities = IdentityService(self.db_path)
        self.parser = CanonicalParseService(self.sources, self.inventory)
        self.materializer = InventoryMaterializationService(self.inventory)

    def pending_ids(self, *, limit: int = 100) -> list[int]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """SELECT id FROM source_posts
                   WHERE parse_status='pending'
                   ORDER BY id ASC LIMIT ?""",
                (max(1, int(limit)),),
            ).fetchall()
        return [int(row[0]) for row in rows]

    def process_one(self, source_post_id: int) -> WorkerItemResult:
        try:
            parsed = self.parser.parse_source_post(int(source_post_id))
            facts = self.inventory.canonical_facts(parsed.canonical_record_id)
            identity = self.identities.allocate(
                canonical_record_id=parsed.canonical_record_id,
                facts=facts,
            )
            materialized = self.materializer.materialize(
                canonical_record_id=parsed.canonical_record_id,
                listing_id=identity.listing_id,
                public_listing_id=identity.public_listing_id,
                create_review=True,
            )
            return WorkerItemResult(
                source_post_id=int(source_post_id),
                status="materialized",
                listing_id=materialized.listing_id,
            )
        except Exception as exc:
            return WorkerItemResult(
                source_post_id=int(source_post_id),
                status="failed",
                error=f"{type(exc).__name__}:{exc}"[:1000],
            )

    def run_once(self, *, limit: int = 100) -> dict[str, int]:
        stats: Counter[str] = Counter()
        ids = self.pending_ids(limit=limit)
        for source_post_id in ids:
            result = self.process_one(source_post_id)
            stats[result.status] += 1
        stats["selected"] = len(ids)
        return dict(stats)


__all__ = ["CanonicalWorker", "WorkerItemResult"]

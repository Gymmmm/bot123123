"""Independent pending-source worker for V3 canonical/inventory processing."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import sqlite3

from v3_core.pipeline import V3CorePipeline


@dataclass(frozen=True)
class WorkerItemResult:
    source_post_id: int
    status: str
    listing_id: str = ""
    error: str = ""


class CanonicalWorker:
    def __init__(self, pipeline: V3CorePipeline):
        self.pipeline = pipeline

    def pending_ids(self, *, limit: int = 100) -> list[int]:
        with sqlite3.connect(self.pipeline.db_path) as conn:
            rows = conn.execute(
                """SELECT id FROM source_posts
                   WHERE parse_status='pending'
                   ORDER BY id ASC LIMIT ?""",
                (max(1, int(limit)),),
            ).fetchall()
        return [int(row[0]) for row in rows]

    def process_one(self, source_post_id: int) -> WorkerItemResult:
        try:
            _parsed, materialized = self.pipeline.parse_and_materialize(
                source_post_id=int(source_post_id),
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

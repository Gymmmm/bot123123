#!/usr/bin/env python3
"""Pure, non-persistent review queue presentation helpers."""
from __future__ import annotations

import re
import sqlite3
from typing import Any


def display_title(raw_title: Any, *, max_length: int = 80) -> str:
    """Deduplicate only the review-screen projection; never mutate drafts.title."""
    text = str(raw_title or "").strip()
    if not text:
        return "（无标题，待事实重算）"
    parts = [part.strip() for part in re.split(r"[｜|]", text) if part.strip()]
    seen: set[str] = set()
    unique: list[str] = []
    for part in parts:
        if part in seen:
            continue
        seen.add(part)
        unique.append(part)
    result = "｜".join(unique) or "（无标题，待事实重算）"
    return result[:max_length]


def pending_overview(
    conn: sqlite3.Connection,
    *,
    preview_min_score: float,
    preview_limit: int = 6,
    queue_limit: int = 12,
) -> dict[str, Any]:
    """Return the real count plus bounded rows for the admin review screen."""
    conn.row_factory = sqlite3.Row
    total_row = conn.execute(
        "SELECT COUNT(*) AS total FROM drafts WHERE review_status='pending'"
    ).fetchone()
    total = int(total_row["total"] or 0)
    # Return complete draft rows.  The review UI renders the canonical cover,
    # caption and callback keyboard from this projection; a reduced SELECT made
    # the first visible card crash on row['id'] and would omit further facts.
    rows = conn.execute(
        """SELECT d.*,
                  (SELECT COUNT(*) FROM media_assets m
                   WHERE m.owner_type='source_post'
                     AND CAST(m.owner_ref_id AS TEXT)=CAST(d.source_post_id AS TEXT)
                     AND COALESCE(m.status,'active')='active') AS source_media_count,
                  (SELECT p.status FROM publication_packages p
                   WHERE p.draft_id=d.draft_id
                     AND p.status IN ('approved','published')
                   ORDER BY p.id DESC LIMIT 1) AS frozen_status
           FROM drafts d
           WHERE d.review_status='pending'
           ORDER BY COALESCE(d.queue_score,0) DESC,d.id DESC
           LIMIT ?""",
        (int(queue_limit),),
    ).fetchall()
    preview = [
        row
        for row in rows
        if row["cover_asset_id"] not in (None, "")
        and float(row["queue_score"] or 0) >= float(preview_min_score)
        and int(row["source_media_count"] or 0) >= 2
        and str(row["frozen_status"] or "").lower() not in {"approved", "published"}
    ][: int(preview_limit)]
    return {
        "pending_total": total,
        "queue_rows": [dict(row) for row in rows],
        "preview_rows": [dict(row) for row in preview],
        "display_titles": {
            str(row["draft_id"]): display_title(row["title"]) for row in rows
        },
        "legacy_rows": [
            str(row["draft_id"])
            for row in rows
            if "canonical_facts.v1" not in str(row["normalized_data"] or "")
        ],
    }


__all__ = ["display_title", "pending_overview"]

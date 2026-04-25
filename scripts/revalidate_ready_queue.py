#!/usr/bin/env python3
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

DB_PATH = os.getenv("DB_PATH", str(ROOT / "data/qiaolian_dual_bot.db"))


def _merge_note(old_note: str, extra: str) -> str:
    old = str(old_note or "").strip()
    if extra in old:
        return old[:500]
    return f"{old} | {extra}".strip(" |")[:500]


def main() -> int:
    import sys

    sys.path.insert(0, str(ROOT))
    from meihua_publisher import evaluate_publish_gate

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT * FROM drafts
        WHERE review_status='ready'
        ORDER BY COALESCE(queue_score, 0) DESC, id ASC
        """
    ).fetchall()

    total = len(rows)
    blocked = 0
    kept = 0

    for row in rows:
        draft = dict(row)
        cover_path = ""
        cover_asset_id = row["cover_asset_id"]
        if cover_asset_id not in (None, "", 0, "0"):
            c = conn.execute(
                "SELECT local_path FROM media_assets WHERE id=? LIMIT 1",
                (cover_asset_id,),
            ).fetchone()
            if c:
                cover_path = str(c[0] or "")

        gate = evaluate_publish_gate(draft, cover_path, DB_PATH)
        if gate.get("is_publishable", True):
            kept += 1
            continue

        reasons = ",".join(gate.get("reasons") or [])
        note = _merge_note(str(row["review_note"] or ""), f"queue_revalidated_blocked:{reasons}")
        conn.execute(
            """
            UPDATE drafts
            SET review_status='pending',
                review_note=?,
                updated_at=CURRENT_TIMESTAMP
            WHERE draft_id=?
            """,
            (note, row["draft_id"]),
        )
        blocked += 1

    conn.commit()
    conn.close()

    print(f"DB={DB_PATH}")
    print(f"ready_total={total}")
    print(f"ready_kept={kept}")
    print(f"ready_blocked_to_pending={blocked}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

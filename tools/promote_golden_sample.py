#!/usr/bin/env python3
"""将已核验的 SKYTREE 真实房源收敛为 L1047 黄金样板。"""
from __future__ import annotations

import json
import sqlite3
import sys


DRAFT_ID = "DRF_c1d2bd9d-1d0b-4464-950b-0474617012be"


def main(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT normalized_data FROM drafts WHERE draft_id=?", (DRAFT_ID,)
    ).fetchone()
    if not row:
        raise SystemExit("黄金样板原始 draft 不存在")
    try:
        normalized = json.loads(row["normalized_data"] or "{}")
    except json.JSONDecodeError:
        normalized = {}
    normalized.update(
        {
            "project": "SKYTREE天空树",
            "area": "金边",
            "layout": "1房1厅",
            "price": 480,
            "size": "84",
            "floor": "14楼",
            "deposit": "押一付一",
            "verification_status": "verified",
            "highlights": ["高层采光", "家具齐全"],
        }
    )
    conn.execute(
        """UPDATE drafts SET
               listing_id=?, title=?, project=?, community=?, area=?, layout=?,
               size=?, floor=?, deposit=?, highlights=?, normalized_data=?,
               queue_score=95, approved_at=CURRENT_TIMESTAMP,
               review_note=COALESCE(review_note,'') || ?, updated_at=CURRENT_TIMESTAMP
           WHERE draft_id=?""",
        (
            "l_1047",
            "SKYTREE天空树｜1房1厅",
            "SKYTREE天空树",
            "SKYTREE天空树",
            "金边",
            "1房1厅",
            "84",
            "14楼",
            "押一付一",
            json.dumps(["高层采光", "家具齐全"], ensure_ascii=False),
            json.dumps(normalized, ensure_ascii=False),
            " | GOLDEN_SAMPLE:L1047;人工核验真实字段与10张实拍",
            DRAFT_ID,
        ),
    )
    conn.commit()
    result = conn.execute(
        """SELECT draft_id, listing_id, project, price, layout, size, floor,
                  deposit, review_status FROM drafts WHERE draft_id=?""",
        (DRAFT_ID,),
    ).fetchone()
    print(dict(result))
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1] if len(sys.argv) > 1 else "data/qiaolian_dual_bot.db"))

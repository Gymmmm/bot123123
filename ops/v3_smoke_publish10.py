from __future__ import annotations

import os
import sqlite3

from meihua_publisher import MeihuaPublisher

DB_PATH = os.environ["DB_PATH"]


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT draft_id FROM drafts ORDER BY id DESC LIMIT 10"
    ).fetchall()
    print("DRAFT_IDS", rows)
    if len(rows) < 10:
        raise SystemExit(f"only_{len(rows)}_drafts")
    draft_ids = [row[0] for row in rows]
    conn.executemany(
        "UPDATE drafts SET review_status='ready', updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
        [(draft_id,) for draft_id in draft_ids],
    )
    conn.commit()
    conn.close()

    publisher = MeihuaPublisher(DB_PATH)
    ok = 0
    for index, draft_id in enumerate(reversed(draft_ids), 1):
        result = bool(publisher.publish_draft(draft_id))
        print("PUBLISH_RESULT", index, draft_id, result)
        ok += int(result)
    print("PUBLISH_OK_COUNT", ok)
    if ok != 10:
        raise SystemExit(f"published_{ok}_of_10")


if __name__ == "__main__":
    main()

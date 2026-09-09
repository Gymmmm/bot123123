from __future__ import annotations

import os
import sqlite3
import traceback

from publication_package import build_package

DB_PATH = os.environ["DB_PATH"]
conn = sqlite3.connect(DB_PATH)
rows = conn.execute(
    "SELECT d.draft_id, d.source_post_id, s.parse_status, d.review_note "
    "FROM drafts d JOIN source_posts s ON s.id=d.source_post_id "
    "WHERE s.parse_status='parsed' ORDER BY d.id DESC LIMIT 12"
).fetchall()
conn.close()

for draft_id, source_post_id, parse_status, review_note in rows:
    print("PROBE", draft_id, source_post_id, parse_status, review_note)
    try:
        result = build_package(DB_PATH, draft_id)
        print("PACKAGE_OK", draft_id, result.get("package_id"))
        break
    except Exception as exc:
        print("PACKAGE_FAIL", draft_id, type(exc).__name__, repr(exc))
        traceback.print_exc()

#!/usr/bin/env python3
"""
Post-migration checks for the listings table (SQLite).
Usage:
  DB_PATH=/path/to/db.db python3 scripts/validate_listings_after_migration.py
"""
from __future__ import annotations

import os
import sqlite3
import sys


def main() -> int:
    db_path = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db")
    fail = False

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()

        ico = cur.execute("PRAGMA integrity_check").fetchone()
        ival = (ico[0] if ico else "").lower() if ico else ""
        if ival == "ok":
            print("[OK] integrity_check = ok")
        else:
            print(f"[FAIL] integrity_check = {ico!r}")
            fail = True

        cols = {r[1] for r in cur.execute("PRAGMA table_info(listings)").fetchall()}
        has_type = "type" in cols
        if not has_type:
            print("[FAIL] listings.type column missing")
            n_empty = -1
            fail = True
        else:
            n_empty = cur.execute(
                """
                SELECT COUNT(*)
                FROM listings
                WHERE "type" IS NULL OR TRIM(COALESCE("type", '')) = ''
                """
            ).fetchone()[0]
            if n_empty == 0:
                print(f"[OK] empty type rows = {n_empty}")
            else:
                print(f"[FAIL] empty type rows = {n_empty}")
                fail = True

        has_pt = "property_type" in cols
        if has_pt:
            n_mis = cur.execute(
                """
                SELECT COUNT(*)
                FROM listings
                WHERE IFNULL("property_type", '') != IFNULL("type", '')
                """
            ).fetchone()[0]
        else:
            n_mis = 0
            print("[—] property_type column missing — skip mismatch (treated 0)")

        if n_mis == 0:
            print(f"[OK] mismatched (property_type vs type) rows = {n_mis}")
        else:
            print(f"[WARN] mismatched (property_type vs type) rows = {n_mis}")
            fail = True

        n_total = cur.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        print(f"[OK] total listings count = {n_total}")
    except sqlite3.Error as e:
        print(f"[FAIL] database error: {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()

    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Migrate discussion_map.json and discussion_bridge.json into SQLite table discussion_map.

Table:
  discussion_map (
    channel_post_id INTEGER PRIMARY KEY,
    discussion_msg_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )

  - discussion_map.json: { "<channel_post_id>": <discussion_msg_id>, ... }
  - discussion_bridge.json: { "discuss_mgid": { ...: { "channel_post_id": N, "t": ... } } }
    New rows from bridge get discussion_msg_id = NULL if not already present (INSERT OR IGNORE).
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

DDL = """
CREATE TABLE IF NOT EXISTS discussion_map (
    channel_post_id INTEGER PRIMARY KEY,
    discussion_msg_id INTEGER,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""


def _load_json(path: Path) -> dict | list | None:
    if not path.is_file():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def main() -> int:
    base = Path(__file__).resolve().parents[1]
    db_path = Path(os.environ.get("DB_PATH", str(base / "data" / "qiaolian_dual_bot.db"))).resolve()
    map_path = Path(
        os.environ.get("DISCUSSION_MAP_FILE", str(base / "data" / "discussion_map.json"))
    ).resolve()
    bridge_path = Path(
        os.environ.get("DISCUSSION_BRIDGE_FILE", str(base / "data" / "discussion_bridge.json"))
    ).resolve()

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.executescript(DDL)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_discussion_map_msg ON discussion_map (discussion_msg_id);"
        )

        n_inserted = 0
        n_bridge_only = 0

        m = _load_json(map_path)
        if isinstance(m, dict):
            for k, v in m.items():
                try:
                    post_id = int(k)
                except (TypeError, ValueError):
                    continue
                try:
                    msg_id = int(v) if v is not None else None
                except (TypeError, ValueError):
                    continue
                cur.execute(
                    """
                    INSERT OR REPLACE INTO discussion_map (channel_post_id, discussion_msg_id)
                    VALUES (?, ?)
                    """,
                    (post_id, msg_id),
                )
                n_inserted += 1
        else:
            print("warning: discussion_map.json missing or not an object; skipping", file=sys.stderr)

        b = _load_json(bridge_path)
        if isinstance(b, dict):
            mg = b.get("discuss_mgid")
            if isinstance(mg, dict):
                for _mgid, slot in mg.items():
                    if not isinstance(slot, dict):
                        continue
                    cp = slot.get("channel_post_id")
                    if cp is None:
                        continue
                    try:
                        post_id = int(cp)
                    except (TypeError, ValueError):
                        continue
                    cur.execute(
                        """
                        INSERT OR IGNORE INTO discussion_map (channel_post_id, discussion_msg_id)
                        VALUES (?, NULL)
                        """,
                        (post_id,),
                    )
                    if cur.rowcount:
                        n_bridge_only += 1
        else:
            print("warning: discussion_bridge.json missing or not an object; skipping", file=sys.stderr)

        conn.commit()
        print(f"Migrated discussion_map: {n_inserted} row(s) from JSON (insert/replace).")
        print(f"From discussion_bridge (new channel_post_id only): {n_bridge_only} row(s).")
        print(f"Database: {db_path}")
    except OSError as e:
        print(f"error: {e}", file=sys.stderr)
        conn.rollback()
        return 1
    except sqlite3.Error as e:
        print(f"sqlite error: {e}", file=sys.stderr)
        conn.rollback()
        return 1
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

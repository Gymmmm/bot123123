#!/usr/bin/env python3
"""
偶发用：把当前所有 review_status=ready 的草稿按顺序发到频道（不走槽位等待）。

常态请仍交给 autopilot_publish_bot 的定时 tick；本脚本仅用于：
  - 想立刻看空队列效果
  - 槽位之间临时清空 ready

用法：
  cd qiaolian_dual_bots_local && python3 scripts/publish_ready_batch.py
  python3 scripts/publish_ready_batch.py --dry-run
  python3 scripts/publish_ready_batch.py --delay 3.0
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env", override=True)
sys.path.insert(0, str(ROOT))

DB_PATH = os.getenv("DB_PATH", str(ROOT / "data/qiaolian_dual_bot.db"))


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="只打印 draft_id，不发频道")
    p.add_argument("--delay", type=float, default=2.5, help="每条之间间隔秒数，防 Telegram 限流")
    args = p.parse_args()

    import sqlite3
    from meihua_publisher import MeihuaPublisher

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT draft_id FROM drafts WHERE review_status='ready' ORDER BY queue_score DESC, id ASC"
    ).fetchall()
    conn.close()

    if not rows:
        print("ready 队列为空，无需发布。")
        return 0

    print(f"共 {len(rows)} 条 ready。")
    if args.dry_run:
        for (did,) in rows:
            print(" ", did)
        return 0

    pub = MeihuaPublisher(DB_PATH)
    ok_n = 0
    for i, (did,) in enumerate(rows):
        ok = pub.publish_draft(did)
        print(f"[{i + 1}/{len(rows)}] {did} -> {'OK' if ok else 'FAIL'}")
        if ok:
            ok_n += 1
        if i < len(rows) - 1 and args.delay > 0:
            time.sleep(args.delay)
    print(f"完成：成功 {ok_n}/{len(rows)}")
    return 0 if ok_n == len(rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())

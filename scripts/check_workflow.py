#!/usr/bin/env python3
"""
流程健康检查（只读库，不发频道、不改状态）。

用于确认「采集 -> 解析 -> 封面 -> 预览/入队 -> 定时发」各环节是否在积货，
而不是靠偶尔手发一条就算过关。

用法：
  cd /opt/qiaolian_dual_bots && .venv/bin/python check_workflow.py
  cd /opt/qiaolian_dual_bots && .venv/bin/python scripts/check_workflow.py
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env", override=True)
DB_PATH = Path(os.getenv("DB_PATH", str(ROOT / "data/qiaolian_dual_bot.db"))).resolve()


def main() -> int:
    if not DB_PATH.is_file():
        print(f"缺少数据库：{DB_PATH}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print(f"DB: {DB_PATH}\n")

    sp = conn.execute(
        "SELECT parse_status, COUNT(*) AS n FROM source_posts GROUP BY parse_status ORDER BY parse_status"
    ).fetchall()
    print("【采集 / 解析入口】source_posts")
    for r in sp:
        print(f"  {r['parse_status']}: {r['n']}")
    if not sp:
        print("  (无数据)")

    dr = conn.execute(
        "SELECT review_status, COUNT(*) AS n FROM drafts GROUP BY review_status ORDER BY review_status"
    ).fetchall()
    print("\n【稿件状态】drafts")
    for r in dr:
        print(f"  {r['review_status']}: {r['n']}")
    if not dr:
        print("  (无数据)")

    pend_no_cover = conn.execute(
        """SELECT COUNT(*) FROM drafts
           WHERE review_status='pending'
             AND (cover_asset_id IS NULL OR cover_asset_id='')"""
    ).fetchone()[0]
    pend_cover = conn.execute(
        """SELECT COUNT(*) FROM drafts
           WHERE review_status='pending'
             AND cover_asset_id IS NOT NULL AND cover_asset_id != ''"""
    ).fetchone()[0]
    print("\n【预览池细分】pending")
    print(f"  待生成封面: {pend_no_cover}")
    print(f"  已有封面（可 /pending 预览）: {pend_cover}")

    ready_n = conn.execute(
        "SELECT COUNT(*) FROM drafts WHERE review_status='ready'"
    ).fetchone()[0]
    print(f"\n【定时队列】ready: {ready_n}（由 autopilot 槽位 dequeue）")

    recent = conn.execute(
        """SELECT draft_id, review_status, updated_at FROM drafts
           ORDER BY id DESC LIMIT 8"""
    ).fetchall()
    print("\n【最近 drafts】")
    for r in recent:
        print(f"  {r['draft_id']} | {r['review_status']} | {r['updated_at']}")

    conn.close()

    print(
        "\n说明：标准流程是环节递进；"
        "AUTO_APPROVE=false 时须管理员在 Bot 里把「有封面 pending」点进 ready；"
        "AUTO_APPROVE=true 为无人值守模式（跳过人工预览环节）。"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

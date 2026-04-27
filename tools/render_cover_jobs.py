#!/usr/bin/env python3
"""
render_cover_jobs.py — Excel 封面渲染工具（Excel 第一发布链路 B 步）

职责：
  1. 扫描 excel_listing_rows（ingestion_status='imported'）中还没有对应
     cover_render_jobs 的记录，为其自动创建 pending 任务（补录历史数据用）。
  2. 处理所有 render_status='pending' 的 cover_render_jobs：
     - 从 excel_listing_rows 读取房源信息和封面底图路径
     - 调用 cover_generator.generate_house_cover() 生成封面图
     - 将结果写回 cover_render_jobs.render_status / output_path / error_message

用法：
  python3 tools/render_cover_jobs.py
  python3 tools/render_cover_jobs.py --db data/qiaolian_dual_bot.db
  python3 tools/render_cover_jobs.py --out-dir media/renders/cover_jobs --limit 20
  python3 tools/render_cover_jobs.py --enqueue-only   # 只补录任务，不渲染
  python3 tools/render_cover_jobs.py --render-only    # 只渲染已有 pending 任务
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import uuid
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

import cover_generator as cg

DEFAULT_DB = ROOT / "data" / "qiaolian_dual_bot.db"
DEFAULT_OUT_DIR = ROOT / "media" / "renders" / "cover_jobs"


# ── 数据库帮助 ────────────────────────────────────────────

def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone()
    return bool(row)


def _ensure_tables(conn: sqlite3.Connection) -> None:
    missing = [
        t for t in ("excel_listing_rows", "cover_render_jobs")
        if not _table_exists(conn, t)
    ]
    if missing:
        raise RuntimeError(
            f"缺少必要的数据库表: {', '.join(missing)}。"
            "请先运行 scripts/migrate_excel_pipeline_v1.py 执行迁移。"
        )


# ── 第一步：为未入队的 excel_listing_rows 创建任务 ─────────

def enqueue_missing_jobs(conn: sqlite3.Connection) -> int:
    """
    为 excel_listing_rows（ingestion_status='imported'）中
    还没有 cover_render_jobs 的行创建 pending 任务。
    返回新创建的任务数量。
    """
    rows = conn.execute(
        """
        SELECT r.row_id, r.desired_cover_w, r.desired_cover_h, r.desired_cover_kind
        FROM excel_listing_rows r
        WHERE r.ingestion_status = 'imported'
          AND NOT EXISTS (
              SELECT 1 FROM cover_render_jobs j WHERE j.row_id = r.row_id
          )
        ORDER BY r.id
        """
    ).fetchall()

    created = 0
    for row in rows:
        job_id = f"JOB_{uuid.uuid4()}"
        conn.execute(
            """
            INSERT INTO cover_render_jobs (
                job_id, row_id, desired_w, desired_h, desired_kind,
                render_status, output_path, error_message,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'pending', '', '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (
                job_id,
                row["row_id"],
                row["desired_cover_w"],
                row["desired_cover_h"],
                row["desired_cover_kind"],
            ),
        )
        created += 1

    if created:
        conn.commit()
    return created


# ── 第二步：渲染 pending 的任务 ───────────────────────────

def _format_price(monthly_rent) -> str:
    """将月租数字格式化为封面所需的价格字符串。"""
    if not monthly_rent:
        return ""
    try:
        v = int(monthly_rent)
        return f"${v}/月" if v > 0 else ""
    except (ValueError, TypeError):
        return str(monthly_rent).strip()


def render_pending_jobs(
    conn: sqlite3.Connection,
    out_dir: Path,
    limit: int | None = None,
) -> tuple[int, int]:
    """
    处理所有 render_status='pending' 的 cover_render_jobs。
    返回 (成功数, 失败数)。
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    query = """
        SELECT j.job_id, j.row_id, j.desired_w, j.desired_h, j.desired_kind,
               r.title, r.area, r.property_type, r.layout,
               r.monthly_rent, r.image_cover
        FROM cover_render_jobs j
        JOIN excel_listing_rows r ON r.row_id = j.row_id
        WHERE j.render_status = 'pending'
        ORDER BY j.id
        LIMIT ?
    """
    params: tuple = (limit if limit is not None and limit > 0 else -1,)
    jobs = conn.execute(query, params).fetchall()
    if not jobs:
        return 0, 0

    success = 0
    failed = 0

    for job in jobs:
        job_id = job["job_id"]
        output_path = str(out_dir / f"{job_id}.jpg")

        # 解析底图路径
        image_cover = str(job["image_cover"] or "").strip()
        if image_cover and not os.path.isabs(image_cover):
            image_cover = str(ROOT / image_cover)
        if image_cover and not os.path.isfile(image_cover):
            image_cover = ""

        # 拼装封面参数
        title = str(job["title"] or "").strip()
        area = str(job["area"] or "").strip()
        layout = str(job["layout"] or "").strip()
        property_type = str(job["property_type"] or "").strip()
        price = _format_price(job["monthly_rent"])

        try:
            cg.generate_house_cover(
                output_path=output_path,
                project=title,
                property_type=layout or property_type,
                area=area,
                price=price,
                base_image_path=image_cover or None,
            )
            conn.execute(
                """
                UPDATE cover_render_jobs
                SET render_status='done', output_path=?, error_message='',
                    updated_at=CURRENT_TIMESTAMP
                WHERE job_id=?
                """,
                (output_path, job_id),
            )
            conn.commit()
            success += 1
            print(f"✓ {job_id}  →  {output_path}")
        except (OSError, ValueError, RuntimeError, AttributeError, TypeError) as exc:
            err = str(exc)[:400]
            conn.execute(
                """
                UPDATE cover_render_jobs
                SET render_status='failed', error_message=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE job_id=?
                """,
                (err, job_id),
            )
            conn.commit()
            failed += 1
            print(f"✗ {job_id}  失败: {err}")

    return success, failed


# ── 主入口 ────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="处理 cover_render_jobs，为 excel_listing_rows 生成封面图"
    )
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite 数据库路径")
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help="封面输出目录（默认 media/renders/cover_jobs）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="单次最多渲染任务数（默认不限制）",
    )
    parser.add_argument(
        "--enqueue-only",
        action="store_true",
        help="只为未入队的 excel_listing_rows 创建任务，不渲染",
    )
    parser.add_argument(
        "--render-only",
        action="store_true",
        help="只渲染已有的 pending 任务，不补录新任务",
    )
    args = parser.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    if not db_path.exists():
        print(f"❌ 数据库不存在: {db_path}")
        return 2

    out_dir = Path(args.out_dir).expanduser().resolve()

    with _connect(db_path) as conn:
        try:
            _ensure_tables(conn)
        except RuntimeError as exc:
            print(f"❌ {exc}")
            return 2

        # 步骤 1：补录未入队的行
        if not args.render_only:
            enqueued = enqueue_missing_jobs(conn)
            if enqueued:
                print(f"[入队] 新建 cover_render_jobs 任务: {enqueued} 条")
            else:
                print("[入队] 无新任务需要入队")

        if args.enqueue_only:
            return 0

        # 步骤 2：渲染 pending 任务
        print(f"[渲染] 开始处理 pending 任务，输出目录: {out_dir}")
        ok, err = render_pending_jobs(conn, out_dir, limit=args.limit)
        print(f"\n[渲染结果] 成功: {ok}  失败: {err}  总计: {ok + err}")

    return 0 if err == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

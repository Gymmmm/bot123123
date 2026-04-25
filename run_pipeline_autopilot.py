#!/usr/bin/env python3
"""
run_pipeline_autopilot.py — 与 run_pipeline 同源思路，但「不在流水线里直接刷屏发帖」。

标准流程中的一段（B 段）：只负责「解析 + 封面」，不负责发帖终验。
  source_posts (pending) → ai_parser → drafts (pending)
  → cover_generator（对 pending 且缺封面）
  → 可选 AUTO_APPROVE：pending+有封面 → ready（跳过人工预览）
  → 【本脚本不 publish】发帖由 autopilot_publish_bot 按槽位从 ready dequeue，
     或管理员 /send / 立即发布（例外路径）。

用法（cron 与原 pipeline 相同，只换脚本名）：
  cd /opt/qiaolian_dual_bots && \
  export $(grep -v '^#' .env | xargs) && \
  python3 /opt/qiaolian_dual_bots/run_pipeline_autopilot.py >> logs/pipeline_autopilot.log 2>&1

部署：可将本文件复制为 /opt/qiaolian_dual_bots/run_pipeline_autopilot.py
"""

import os
import sys
import sqlite3
import logging
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pipeline_autopilot")

DB_PATH = os.getenv("DB_PATH", str(BASE_DIR / "data/qiaolian_dual_bot.db"))
AUTO_APPROVE = os.getenv("AUTO_APPROVE", "false").lower() == "true"
MAX_COVERS_PER_RUN = int(os.getenv("MAX_COVERS_PER_RUN", "30"))
AUTO_READY_MIN_SCORE = int(os.getenv("AUTO_READY_MIN_SCORE", "75"))
REPARSE_LOW_QUALITY_DRAFTS = os.getenv("REPARSE_LOW_QUALITY_DRAFTS", "true").lower() == "true"
REPARSE_DRAFT_LIMIT = int(os.getenv("REPARSE_DRAFT_LIMIT", "40"))
AUTO_REPAIR_PENDING_DRAFTS = os.getenv("AUTO_REPAIR_PENDING_DRAFTS", "true").lower() == "true"
AUTO_REPAIR_PENDING_LIMIT = int(os.getenv("AUTO_REPAIR_PENDING_LIMIT", "120"))
# true = 无 OPENROUTER_API_KEY 则直接退出；false = 使用本地规则解析，后续再叠加模型抽取
PIPELINE_REQUIRE_OPENROUTER = (
    os.getenv("PIPELINE_REQUIRE_OPENROUTER", "false").lower() == "true"
)


def _db():
    return sqlite3.connect(DB_PATH)


def _source_status_counts() -> dict[str, int]:
    conn = _db()
    rows = conn.execute(
        "SELECT parse_status, COUNT(*) FROM source_posts GROUP BY parse_status"
    ).fetchall()
    conn.close()
    out: dict[str, int] = {}
    for key, value in rows:
        out[str(key or "null")] = int(value or 0)
    return out


def _draft_status_counts() -> dict[str, int]:
    conn = _db()
    rows = conn.execute(
        "SELECT review_status, COUNT(*) FROM drafts GROUP BY review_status"
    ).fetchall()
    conn.close()
    out: dict[str, int] = {}
    for key, value in rows:
        out[str(key or "null")] = int(value or 0)
    return out


def _pending_cover_metrics() -> dict[str, int]:
    conn = _db()
    pending_total = conn.execute(
        "SELECT COUNT(*) FROM drafts WHERE review_status='pending'"
    ).fetchone()[0]
    pending_no_cover = conn.execute(
        "SELECT COUNT(*) FROM drafts WHERE review_status='pending' AND (cover_asset_id IS NULL OR cover_asset_id='')"
    ).fetchone()[0]
    pending_has_cover = max(0, int(pending_total or 0) - int(pending_no_cover or 0))
    conn.close()
    return {
        "pending_total": int(pending_total or 0),
        "pending_no_cover": int(pending_no_cover or 0),
        "pending_has_cover": int(pending_has_cover),
    }


def _diff_counts(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    keys = set(before.keys()) | set(after.keys())
    delta: dict[str, int] = {}
    for key in sorted(keys):
        change = int(after.get(key, 0)) - int(before.get(key, 0))
        if change:
            delta[key] = change
    return delta


def _log_checkpoint(stage: str, snapshot: dict[str, Any]) -> None:
    logger.info("  [%s] source_posts=%s", stage, snapshot.get("source_posts"))
    logger.info("  [%s] drafts=%s", stage, snapshot.get("drafts"))
    logger.info("  [%s] cover=%s", stage, snapshot.get("cover"))


def _snapshot_pipeline_state() -> dict[str, Any]:
    return {
        "source_posts": _source_status_counts(),
        "drafts": _draft_status_counts(),
        "cover": _pending_cover_metrics(),
    }


def step_ai_parse() -> dict[str, Any]:
    logger.info("── Step 1: AI 解析 ──────────────────────────────")
    before = _source_status_counts()
    pending = int(before.get("pending", 0))
    if pending == 0:
        logger.info("没有 pending 的 source_posts，跳过")
        return {"pending_before": 0, "results": {"total_pending": 0}, "delta": {}}
    sys.path.insert(0, str(BASE_DIR))
    from ai_parser import AIParserModule

    parser = AIParserModule(DB_PATH)
    results = parser.process_pending_source_posts()
    after = _source_status_counts()
    delta = _diff_counts(before, after)
    logger.info("解析统计: %s", results)
    logger.info("source_posts parse_status 变化: %s", delta or {"no_change": 0})
    return {"pending_before": pending, "results": results, "delta": delta}


def step_refresh_low_quality_drafts() -> int:
    if not REPARSE_LOW_QUALITY_DRAFTS:
        return 0
    logger.info("── Step 1b: 修复低质量 drafts ─────────────────────")
    sys.path.insert(0, str(BASE_DIR))
    from ai_parser import AIParserModule

    parser = AIParserModule(DB_PATH)
    refreshed = parser.refresh_low_quality_drafts(limit=REPARSE_DRAFT_LIMIT)
    logger.info("已刷新低质量 drafts：%s 条", refreshed)
    return int(refreshed)


def step_auto_repair_pending_drafts() -> int:
    if not AUTO_REPAIR_PENDING_DRAFTS:
        return 0
    logger.info("── Step 1c: 自动修复 pending 解析 ─────────────────")
    sys.path.insert(0, str(BASE_DIR))
    from ai_parser import AIParserModule

    parser = AIParserModule(DB_PATH)
    reparsed = parser.refresh_pending_drafts(limit=AUTO_REPAIR_PENDING_LIMIT)
    area_fixed = parser.normalize_pending_area_labels(limit=AUTO_REPAIR_PENDING_LIMIT)
    logger.info("pending 重解析：%s 条；area 纠正：%s 条", reparsed, area_fixed)
    return int(reparsed or 0) + int(area_fixed or 0)


def step_cover_generate() -> int:
    """为 pending 且无封面的 drafts 生成封面（不再依赖 approved）。"""
    logger.info("── Step 2: 封面图生成 ─────────────────────────")
    before_cover = _pending_cover_metrics()
    logger.info("  生成前 cover 状态: %s", before_cover)
    rows = _db().execute(
        """SELECT draft_id FROM drafts
           WHERE review_status='pending'
             AND (cover_asset_id IS NULL OR cover_asset_id='')
           ORDER BY id
           LIMIT ?""",
        (MAX_COVERS_PER_RUN,),
    ).fetchall()
    if not rows:
        logger.info("没有需要生成封面图的 pending drafts，跳过")
        return 0
    sys.path.insert(0, str(BASE_DIR))
    from cover_generator import CoverGenerator

    gen = CoverGenerator(DB_PATH)
    count = 0
    for (did,) in rows:
        asset_id, path = gen.generate_for_draft(did)
        if asset_id:
            logger.info(f"  封面成功：{did}")
            count += 1
        else:
            logger.warning(f"  封面失败：{did}")
    after_cover = _pending_cover_metrics()
    logger.info("  生成后 cover 状态: %s", after_cover)
    logger.info("  cover 状态变化: %s", _diff_counts(before_cover, after_cover) or {"no_change": 0})
    return count


def step_auto_ready() -> int:
    """
    可选：AUTO_APPROVE=true 时，将「已有封面」的 pending 直接送进 ready 队列，
    完全无人值守定时发。若你希望必须人工点「入队」，保持 AUTO_APPROVE=false。
    """
    if not AUTO_APPROVE:
        return 0
    logger.info("── Step 2b: AUTO_APPROVE → pending+封面 转 ready ──")
    sys.path.insert(0, str(BASE_DIR))
    from media_consistency import assess_draft_media, mark_draft_media_broken, media_blocks_ready, media_issue_summary
    from meihua_publisher import evaluate_publish_gate

    conn = _db()
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT draft_id, cover_asset_id FROM drafts
           WHERE review_status='pending'
             AND cover_asset_id IS NOT NULL AND cover_asset_id != ''
             AND COALESCE(queue_score, 0) >= ?
           ORDER BY queue_score DESC, id ASC""",
        (AUTO_READY_MIN_SCORE,),
    ).fetchall()
    conn.close()

    ready_count = 0
    broken_count = 0
    gate_blocked_count = 0
    for row in rows:
        draft_id = str(row["draft_id"])
        status = assess_draft_media(draft_id, DB_PATH)
        if media_blocks_ready(status):
            mark_draft_media_broken(draft_id, status, DB_PATH)
            broken_count += 1
            logger.warning("  跳过入队：%s media=%s", draft_id, media_issue_summary(status))
            continue
        conn = _db()
        conn.row_factory = sqlite3.Row
        d = conn.execute(
            "SELECT * FROM drafts WHERE draft_id=? LIMIT 1",
            (draft_id,),
        ).fetchone()
        cover_path_row = conn.execute(
            "SELECT local_path FROM media_assets WHERE id=? LIMIT 1",
            (row["cover_asset_id"],),
        ).fetchone()
        conn.close()
        if not d:
            logger.warning("  跳过入队：%s draft_missing", draft_id)
            continue
        cover_path = str((cover_path_row or [None])[0] or "")
        gate = evaluate_publish_gate(dict(d), cover_path, DB_PATH)
        if not gate.get("is_publishable", True):
            gate_blocked_count += 1
            reasons = ",".join(gate.get("reasons") or [])
            conn = _db()
            old_note_row = conn.execute(
                "SELECT review_note FROM drafts WHERE draft_id=? LIMIT 1",
                (draft_id,),
            ).fetchone()
            old_note = str((old_note_row or [None])[0] or "").strip()
            block_note = f"auto_ready_blocked:{reasons}" if reasons else "auto_ready_blocked"
            if block_note not in old_note:
                merged = f"{old_note} | {block_note}".strip(" |")[:500]
                conn.execute(
                    "UPDATE drafts SET review_note=?, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
                    (merged, draft_id),
                )
                conn.commit()
            conn.close()
            logger.warning("  跳过入队：%s gate=%s", draft_id, reasons or "blocked")
            continue
        conn = _db()
        conn.execute(
            "UPDATE drafts SET review_status='ready', updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
            (draft_id,),
        )
        conn.commit()
        conn.close()
        ready_count += 1
    logger.info(
        "  已入队 ready：%s 条；媒体损坏跳过：%s 条；网关拦截：%s 条",
        ready_count,
        broken_count,
        gate_blocked_count,
    )
    return ready_count


def print_summary():
    sp = _source_status_counts()
    dr = _draft_status_counts()
    cover = _pending_cover_metrics()
    logger.info("── 摘要 ──────────────────────────────────────")
    logger.info(f"  source_posts: {sp}")
    logger.info(f"  drafts:       {dr}")
    logger.info(f"  cover:        {cover}")


def main():
    logger.info("=" * 60)
    logger.info(f"Autopilot pipeline 启动 DB_PATH={DB_PATH}")
    logger.info(
        "AUTO_APPROVE=%s（true=有封面自动进 ready） MAX_COVERS_PER_RUN=%s AUTO_READY_MIN_SCORE=%s",
        AUTO_APPROVE,
        MAX_COVERS_PER_RUN,
        AUTO_READY_MIN_SCORE,
    )
    logger.info(
        "AUTO_REPAIR_PENDING_DRAFTS=%s AUTO_REPAIR_PENDING_LIMIT=%s",
        AUTO_REPAIR_PENDING_DRAFTS,
        AUTO_REPAIR_PENDING_LIMIT,
    )
    logger.info("=" * 60)
    if PIPELINE_REQUIRE_OPENROUTER and not os.getenv("OPENROUTER_API_KEY", "").strip():
        logger.error("PIPELINE_REQUIRE_OPENROUTER=true 但未设置 OPENROUTER_API_KEY，中止")
        sys.exit(1)
    if not os.getenv("OPENROUTER_API_KEY", "").strip():
        logger.warning(
            "未设置 OPENROUTER_API_KEY：当前使用 ai_parser 规则解析。"
            "如需更强抽取效果，再接入模型并设 PIPELINE_REQUIRE_OPENROUTER=true。"
        )
    try:
        start_state = _snapshot_pipeline_state()
        _log_checkpoint("start", start_state)
        parse_result = step_ai_parse()
        logger.info("Step 1 输出: %s", parse_result)
        after_parse = _snapshot_pipeline_state()
        logger.info(
            "Step 1 节点变化 source_posts=%s drafts=%s",
            _diff_counts(start_state["source_posts"], after_parse["source_posts"]) or {"no_change": 0},
            _diff_counts(start_state["drafts"], after_parse["drafts"]) or {"no_change": 0},
        )
        step_refresh_low_quality_drafts()
        step_auto_repair_pending_drafts()
        after_refresh = _snapshot_pipeline_state()
        logger.info(
            "Step 1b/1c 节点变化 drafts=%s",
            _diff_counts(after_parse["drafts"], after_refresh["drafts"]) or {"no_change": 0},
        )
        step_cover_generate()
        after_cover = _snapshot_pipeline_state()
        logger.info(
            "Step 2 节点变化 drafts=%s cover=%s",
            _diff_counts(after_refresh["drafts"], after_cover["drafts"]) or {"no_change": 0},
            _diff_counts(after_refresh["cover"], after_cover["cover"]) or {"no_change": 0},
        )
        step_auto_ready()
        end_state = _snapshot_pipeline_state()
        logger.info(
            "Step 2b 节点变化 drafts=%s",
            _diff_counts(after_cover["drafts"], end_state["drafts"]) or {"no_change": 0},
        )
        _log_checkpoint("end", end_state)
    except Exception:
        logger.exception("pipeline 异常")
        sys.exit(1)
    print_summary()
    logger.info("本脚本不执行频道发帖；发帖由 autopilot_publish_bot 负责。")


if __name__ == "__main__":
    main()

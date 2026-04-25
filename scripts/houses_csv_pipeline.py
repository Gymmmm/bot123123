#!/usr/bin/env python3
"""
houses.csv 发布流水线：
1) prepare: 预生成封面与待发布素材（不发频道）
2) send-next: 按状态文件发送下一条（适合定时任务）

目标：将 houses.csv 作为唯一输入源，发布过程可持续、可追踪、可恢复。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from telegram import Bot
from telegram.error import RetryAfter, TelegramError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 复用现有 CSV 发布能力，避免维护两套逻辑。
from tools.publish_houses_csv import (  # type: ignore
    DEFAULT_COVER_H,
    DEFAULT_COVER_W,
    DEFAULT_CSV,
    DEFAULT_PREPARED,
    DEFAULT_RENDERS,
    _build_payload,
    _cover_kind_from_row,
    _prepare_payload_assets,
    _read_rows,
    _resolve_text_style,
    _send_payload,
)

load_dotenv(ROOT / ".env", override=True)


DEFAULT_STATE = ROOT / "data" / "houses_publish_state.json"
DEFAULT_LOG = ROOT / "logs" / "houses_csv_pipeline.log"


@dataclass
class PublishResult:
    status: str  # sent | skipped | failed | none
    title: str
    index: int
    detail: str = ""


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _load_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"next_index": 0, "history": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"next_index": 0, "history": []}
        data.setdefault("next_index", 0)
        data.setdefault("history", [])
        return data
    except Exception:
        return {"next_index": 0, "history": []}


def _save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    history = state.get("history") or []
    if len(history) > 500:
        state["history"] = history[-500:]
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _append_log(path: Path, msg: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = f"[{_now()}] {msg}\n"
    with path.open("a", encoding="utf-8") as f:
        f.write(line)


def _rows(csv_path: Path) -> list[dict[str, str]]:
    if not csv_path.is_file():
        raise FileNotFoundError(f"csv not found: {csv_path}")
    return _read_rows(csv_path)


async def _prepare_all(args: argparse.Namespace) -> int:
    rows = _rows(args.csv)
    csv_dir = args.csv.parent
    prepared_dir = args.prepared_dir
    auto_cover_dir = args.auto_cover_dir
    prepared = 0
    skipped = 0

    for i, row in enumerate(rows, start=1):
        row_kind = _cover_kind_from_row(row, args.kind)
        try:
            payload = _build_payload(
                row,
                csv_dir,
                kind=row_kind,
                auto_cover_dir=auto_cover_dir,
                text_style=args.text_style,
                render_template=args.render_template,
                cover_w=args.cover_w,
                cover_h=args.cover_h,
                force_render_cover=True,
                check_files=True,
            )
            _prepare_payload_assets(
                payload,
                row,
                out_root=prepared_dir,
                detail_brand_mark=args.detail_brand_mark == "on",
                detail_brand_style=args.detail_brand_style,
            )
            prepared += 1
            print(f"[prepared {i}] {payload.title}")
        except Exception as e:
            skipped += 1
            print(f"[skip {i}] {row.get('title', '(untitled)')}: {e}")

    msg = f"prepare done prepared={prepared} skipped={skipped} total={len(rows)}"
    print(msg)
    _append_log(args.log_path, msg)
    return 0 if skipped == 0 else 2


async def _send_one(args: argparse.Namespace) -> int:
    rows = _rows(args.csv)
    state = _load_state(args.state)
    idx = int(state.get("next_index") or 0)
    if idx >= len(rows):
        msg = f"send-next none: next_index={idx}, total={len(rows)}"
        print(msg)
        _append_log(args.log_path, msg)
        return 0

    row = rows[idx]
    title = row.get("title", "") or "(untitled)"
    row_kind = _cover_kind_from_row(row, args.kind)
    csv_dir = args.csv.parent
    bot = Bot(token=args.bot_token)

    result = PublishResult(status="failed", title=title, index=idx)
    try:
        payload = _build_payload(
            row,
            csv_dir,
            kind=row_kind,
            auto_cover_dir=args.auto_cover_dir,
            text_style=args.text_style,
            render_template=args.render_template,
            cover_w=args.cover_w,
            cover_h=args.cover_h,
            force_render_cover=True,
            check_files=True,
        )
        if len(payload.images) < max(1, int(args.min_images)):
            result = PublishResult(
                status="skipped",
                title=payload.title,
                index=idx,
                detail=f"images={len(payload.images)} < min_images={args.min_images}",
            )
        else:
            await _send_payload(
                bot,
                args.channel_id,
                payload,
                detail_brand_mark=args.detail_brand_mark == "on",
                detail_brand_style=args.detail_brand_style,
            )
            result = PublishResult(status="sent", title=payload.title, index=idx)
    except RetryAfter as e:
        wait_s = int(getattr(e, "retry_after", 5) or 5)
        result = PublishResult(status="failed", title=title, index=idx, detail=f"retry_after={wait_s}")
    except TelegramError as e:
        result = PublishResult(status="failed", title=title, index=idx, detail=f"telegram={e}")
    except Exception as e:
        result = PublishResult(status="failed", title=title, index=idx, detail=f"error={e}")

    # 避免死卡：无论 sent/skipped/failed 都前进到下一条（失败由 history 追溯补发）。
    state["next_index"] = idx + 1
    history = state.get("history") or []
    history.append(
        {
            "time": _now(),
            "status": result.status,
            "index": result.index,
            "title": result.title,
            "detail": result.detail,
        }
    )
    state["history"] = history
    _save_state(args.state, state)

    msg = (
        f"send-next {result.status}: index={result.index} "
        f"title={result.title} detail={result.detail} next_index={state['next_index']}"
    ).strip()
    print(msg)
    _append_log(args.log_path, msg)
    return 0 if result.status == "sent" else 3


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="houses.csv scheduled pipeline")
    ap.add_argument("--mode", choices=("prepare", "send-next"), default="send-next")
    ap.add_argument("--csv", default=str(DEFAULT_CSV))
    ap.add_argument("--state", default=str(DEFAULT_STATE))
    ap.add_argument("--log-path", default=str(DEFAULT_LOG))
    ap.add_argument("--bot-token", default=os.getenv("PUBLISHER_BOT_TOKEN", os.getenv("BOT_TOKEN", "")))
    ap.add_argument("--channel-id", default=os.getenv("CHANNEL_ID", ""))
    ap.add_argument("--kind", default="right_price_fixed")
    ap.add_argument("--text-style", default="qc")
    ap.add_argument("--auto-cover-dir", default=str(DEFAULT_RENDERS))
    ap.add_argument("--prepared-dir", default=str(DEFAULT_PREPARED))
    ap.add_argument("--render-template", default="")
    ap.add_argument("--cover-w", type=int, default=DEFAULT_COVER_W)
    ap.add_argument("--cover-h", type=int, default=DEFAULT_COVER_H)
    ap.add_argument("--detail-brand-mark", choices=("on", "off"), default="on")
    ap.add_argument("--detail-brand-style", choices=("auto", "v1", "v2", "v3"), default="auto")
    ap.add_argument("--min-images", type=int, default=1)
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    args.csv = Path(args.csv).expanduser().resolve()
    args.state = Path(args.state).expanduser().resolve()
    args.log_path = Path(args.log_path).expanduser().resolve()
    args.auto_cover_dir = Path(args.auto_cover_dir).expanduser().resolve()
    args.prepared_dir = Path(args.prepared_dir).expanduser().resolve()

    if args.mode == "send-next":
        if not args.bot_token:
            print("missing bot token")
            return 1
        if not args.channel_id:
            print("missing channel id")
            return 1
        return asyncio.run(_send_one(args))
    return asyncio.run(_prepare_all(args))


if __name__ == "__main__":
    raise SystemExit(main())

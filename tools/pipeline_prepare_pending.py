#!/usr/bin/env python3
"""
一键执行房源流水线：

1) 可选：从 DB 自动采集/归类生成 houses.csv
2) 统一渲染封面 + 详情图品牌叠加
3) 二选一：
   - 默认：输出到 pending_upload（等待人工发）
   - --auto-publish：直接自动发布到频道

典型用法：
  # A. 全自动待发（DB -> houses.csv -> pending_upload）
  python3 tools/pipeline_prepare_pending.py

  # B. 全自动直发（DB -> houses.csv -> 自动发布）
  python3 tools/pipeline_prepare_pending.py --auto-publish

  # C. 你手里已有 Excel 导出的 houses.csv
  python3 tools/pipeline_prepare_pending.py --source csv --input-csv /path/to/houses.csv
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import warnings
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COLLECT_SCRIPT = ROOT / "tools" / "collect_houses_csv.py"
PUBLISH_SCRIPT = ROOT / "tools" / "publish_houses_csv.py"
DEFAULT_DB = ROOT / "data" / "qiaolian_dual_bot.db"
DEFAULT_HOUSES_CSV = ROOT / "data" / "houses.csv"
DEFAULT_PREPARED_DIR = ROOT / "media" / "renders" / "pending_upload"


def _run(cmd: list[str]) -> None:
    print("$ " + " ".join(cmd))
    subprocess.run(cmd, check=True)


def _build_collect_cmd(args: argparse.Namespace, out_csv: Path) -> list[str]:
    cmd = [
        sys.executable,
        str(COLLECT_SCRIPT),
        "--db",
        str(Path(args.db).expanduser().resolve()),
        "--out",
        str(out_csv),
        "--statuses",
        str(args.statuses),
        "--limit",
        str(int(args.limit)),
        "--contact",
        str(args.contact),
        "--brand",
        str(args.brand),
    ]
    if args.allow_no_images:
        cmd.append("--allow-no-images")
    if args.allow_no_price:
        cmd.append("--allow-no-price")
    if args.no_dedupe_source_post:
        cmd.append("--no-dedupe-source-post")
    if args.no_dedupe_fingerprint:
        cmd.append("--no-dedupe-fingerprint")
    return cmd


def _build_publish_cmd(args: argparse.Namespace, csv_path: Path) -> list[str]:
    cmd = [
        sys.executable,
        str(PUBLISH_SCRIPT),
        "--csv",
        str(csv_path),
        "--kind",
        str(args.kind),
        "--text-style",
        str(args.text_style),
        "--detail-brand-mark",
        str(args.detail_brand_mark),
        "--detail-brand-style",
        str(args.detail_brand_style),
        "--prepared-dir",
        str(Path(args.prepared_dir).expanduser().resolve()),
        "--sleep",
        str(float(args.sleep)),
    ]
    if int(args.prepare_limit) > 0:
        cmd.extend(["--limit", str(int(args.prepare_limit))])
    min_images = int(args.min_images or 0)
    if min_images <= 0 and args.auto_publish:
        min_images = 4
    if min_images > 0:
        cmd.extend(["--min-images", str(min_images)])
    if args.force_render_cover:
        cmd.append("--force-render-cover")
    if args.render_template:
        cmd.extend(["--render-template", str(args.render_template)])
    if str(args.bot_token or "").strip():
        cmd.extend(["--bot-token", str(args.bot_token).strip()])
    if str(args.channel_id or "").strip():
        cmd.extend(["--channel-id", str(args.channel_id).strip()])
    if args.dry_run:
        cmd.append("--dry-run")
    elif not args.auto_publish:
        cmd.append("--prepare-only")
    return cmd


def main() -> int:
    ap = argparse.ArgumentParser(description="DB/CSV -> 待发素材或自动发布 一键脚本")
    ap.add_argument("--source", choices=("db", "csv"), default="db", help="数据来源：db 自动采集，或直接用 csv")
    ap.add_argument("--input-csv", default="", help="source=csv 时必填：输入 houses.csv 路径")
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--houses-csv", default=str(DEFAULT_HOUSES_CSV), help="中间 houses.csv 输出/读取路径")

    ap.add_argument("--statuses", default="ready,published,pending")
    ap.add_argument("--limit", type=int, default=80, help="source=db 时采集条数上限")
    ap.add_argument("--allow-no-images", action="store_true")
    ap.add_argument("--allow-no-price", action="store_true")
    ap.add_argument("--no-dedupe-source-post", action="store_true")
    ap.add_argument("--no-dedupe-fingerprint", action="store_true")
    ap.add_argument("--contact", default="@pengqingw")
    ap.add_argument("--brand", default="侨联地产")

    ap.add_argument("--kind", default="right_price_fixed")
    ap.add_argument("--text-style", default="qc")
    ap.add_argument("--force-render-cover", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--render-template", default="")
    ap.add_argument("--detail-brand-mark", choices=("on", "off"), default="on")
    ap.add_argument("--detail-brand-style", choices=("auto", "v1", "v2", "v3"), default="auto")
    ap.add_argument("--prepared-dir", default=str(DEFAULT_PREPARED_DIR))
    ap.add_argument("--clean-pending", action="store_true", help="生成前先清空 prepared-dir")
    ap.add_argument("--auto-publish", action="store_true", help="自动发布到频道（不走待上传目录）")
    ap.add_argument("--bot-token", default="", help="可选：覆盖环境变量里的 bot token")
    ap.add_argument("--channel-id", default="", help="可选：覆盖环境变量里的频道 chat_id")
    ap.add_argument(
        "--min-images",
        type=int,
        default=0,
        help="最少图片数。0=自动：auto-publish 时默认 4，其他模式默认 1。",
    )
    ap.add_argument("--prepare-limit", type=int, default=0, help="渲染准备条数上限，0 表示全部")
    ap.add_argument("--sleep", type=float, default=0.1)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if args.auto_publish:
        warnings.warn(
            "--auto-publish is deprecated and will be removed. Use the admin web UI instead.",
            DeprecationWarning,
            stacklevel=2,
        )

    houses_csv = Path(args.houses_csv).expanduser().resolve()
    houses_csv.parent.mkdir(parents=True, exist_ok=True)

    if args.source == "db":
        _run(_build_collect_cmd(args, houses_csv))
    else:
        input_csv = Path(str(args.input_csv or "")).expanduser().resolve()
        if not input_csv.is_file():
            raise FileNotFoundError("source=csv 时，--input-csv 必须是可用文件")
        if input_csv != houses_csv:
            shutil.copy2(str(input_csv), str(houses_csv))
        print(f"using csv: {houses_csv}")

    prepared_dir = Path(args.prepared_dir).expanduser().resolve()
    if (not args.auto_publish) and args.clean_pending and prepared_dir.exists():
        shutil.rmtree(prepared_dir)
    if not args.auto_publish:
        prepared_dir.mkdir(parents=True, exist_ok=True)

    _run(_build_publish_cmd(args, houses_csv))
    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

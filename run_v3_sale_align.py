#!/usr/bin/env python3
"""Daily sale inventory scan over the shared V3 database.

Default mode scans at most once per Asia/Phnom_Penh day. --loop keeps that
cadence without becoming a second collector. Rent publication flags are not
reset. This entrypoint is not a production systemd service yet.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import time
from typing import Callable

from dotenv import load_dotenv

from v3_core.sale.align import SaleInventoryAligner


def run_once(aligner: SaleInventoryAligner, *, force: bool) -> dict:
    return aligner.align_if_due(force=force)


def run_daily_loop(
    aligner: SaleInventoryAligner,
    *,
    interval_seconds: float,
    force_first: bool = False,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> None:
    first = True
    interval = max(60.0, float(interval_seconds))
    while True:
        stats = run_once(aligner, force=force_first and first)
        first = False
        print(json.dumps(stats, ensure_ascii=False, sort_keys=True), flush=True)
        sleep_fn(interval)


def main() -> None:
    root = Path(__file__).resolve().parent
    load_dotenv(root / ".env")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db",
        default=os.getenv("V3_DB_PATH")
        or os.getenv("DB_PATH")
        or os.getenv("SQLITE_PATH")
        or str(root / "data" / "qiaolian_dual_bot.db"),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="scan even if today's daily pass already ran",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="keep checking on an interval; still only writes once per local day",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.getenv("V3_SALE_ALIGN_INTERVAL_SECONDS", "86400")),
        help="seconds between daily checks in --loop mode, default 86400",
    )
    args = parser.parse_args()
    aligner = SaleInventoryAligner(Path(args.db).expanduser().resolve())
    if args.loop:
        try:
            run_daily_loop(
                aligner,
                interval_seconds=max(60.0, float(args.interval)),
                force_first=bool(args.force),
            )
        except KeyboardInterrupt:
            return
        return
    print(json.dumps(run_once(aligner, force=bool(args.force)), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()

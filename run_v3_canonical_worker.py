#!/usr/bin/env python3
"""Process pending V3 source evidence into canonical inventory."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import time
from typing import Callable

from dotenv import load_dotenv
from v3_core.parser.worker import CanonicalWorker


def run_worker_loop(
    worker: CanonicalWorker,
    *,
    limit: int,
    interval_seconds: float,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> None:
    """Continuously process pending source rows until interrupted.

    ``CanonicalWorker.run_once`` remains the single business operation. This
    wrapper only provides the production daemon loop required by systemd.
    """
    clean_limit = max(1, int(limit))
    interval = max(0.25, float(interval_seconds))
    while True:
        stats = worker.run_once(limit=clean_limit)
        print(json.dumps(stats, ensure_ascii=False, sort_keys=True), flush=True)
        sleep_fn(interval)


def main() -> None:
    root = Path(__file__).resolve().parent
    load_dotenv(root / ".env")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db",
        default=os.getenv("DB_PATH", str(root / "data" / "qiaolian_dual_bot.db")),
    )
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument(
        "--loop",
        action="store_true",
        help="run continuously for production service use",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.getenv("V3_CANONICAL_WORKER_INTERVAL_SECONDS", "5")),
        help="seconds between worker cycles in --loop mode",
    )
    args = parser.parse_args()

    worker = CanonicalWorker(str(Path(args.db).expanduser().resolve()))
    if args.loop:
        try:
            run_worker_loop(
                worker,
                limit=max(1, int(args.limit)),
                interval_seconds=max(0.25, float(args.interval)),
            )
        except KeyboardInterrupt:
            return
        return

    stats = worker.run_once(limit=max(1, int(args.limit)))
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()

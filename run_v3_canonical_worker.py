#!/usr/bin/env python3
"""Process pending V3 source evidence into canonical inventory once."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from v3_core.parser.worker import CanonicalWorker


def main() -> None:
    root = Path(__file__).resolve().parent
    load_dotenv(root / ".env")
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=os.getenv("DB_PATH", str(root / "data" / "qiaolian_dual_bot.db")))
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()

    stats = CanonicalWorker(str(Path(args.db).expanduser().resolve())).run_once(
        limit=max(1, int(args.limit)),
    )
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()

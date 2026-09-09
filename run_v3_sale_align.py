#!/usr/bin/env python3
"""Optional one-shot alignment of collected V3 facts onto sale offers.

This does not start a service and does not change rent publication flags.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv

from v3_core.sale.align import SaleInventoryAligner


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
    args = parser.parse_args()
    stats = SaleInventoryAligner(Path(args.db).expanduser().resolve()).align()
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()

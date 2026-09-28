#!/usr/bin/env python3
"""Print a read-only JSON health report for the future sale inventory."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv

from v3_core.sale.audit import SaleInventoryAudit


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
    parser.add_argument("--sample-limit", type=int, default=20)
    args = parser.parse_args()
    report = SaleInventoryAudit(Path(args.db)).report(sample_limit=max(1, args.sample_limit))
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()

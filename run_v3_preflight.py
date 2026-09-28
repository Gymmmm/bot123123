#!/usr/bin/env python3
"""Read-only V3 readiness check; storage creation requires --initialize."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

from dotenv import load_dotenv

from v3_core.readiness import COMPONENTS, run_preflight
from v3_core.storage.bootstrap import initialize_v3_storage


def _resolve_db(root: Path, raw: str | None) -> Path:
    value = raw or os.getenv("DB_PATH") or os.getenv("SQLITE_PATH") or "data/qiaolian_dual_bot.db"
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def main() -> int:
    root = Path(__file__).resolve().parent
    load_dotenv(root / ".env")

    parser = argparse.ArgumentParser(
        description="Check V3 readiness without starting Telegram services.",
    )
    parser.add_argument(
        "--component",
        choices=sorted(COMPONENTS),
        default="all",
        help="limit checks to one V3 component",
    )
    parser.add_argument("--db", default=None, help="override DB_PATH/SQLITE_PATH")
    parser.add_argument(
        "--initialize",
        action="store_true",
        help="explicitly create additive V3 tables before checking",
    )
    args = parser.parse_args()

    db_path = _resolve_db(root, args.db)
    if args.initialize:
        initialize_v3_storage(db_path)

    report = run_preflight(
        repo_root=root,
        db_path=db_path,
        component=args.component,
    )
    payload = report.as_dict()
    payload["initialized"] = bool(args.initialize)
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report.ok else 2


if __name__ == "__main__":
    sys.exit(main())

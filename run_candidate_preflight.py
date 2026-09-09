#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from candidate_runtime.bootstrap import initialize_runtime_storage, missing_runtime_tables
from candidate_runtime.runtime_env import configure_candidate_environment


def main() -> int:
    parser = argparse.ArgumentParser(description="Check or initialize isolated candidate runtime storage.")
    parser.add_argument("--initialize", action="store_true", help="create missing drafts/listings/posts pipeline tables")
    args = parser.parse_args()

    paths = configure_candidate_environment()
    before = sorted(missing_runtime_tables(paths.db_path))
    if args.initialize:
        initialize_runtime_storage(paths.db_path)
    after = sorted(missing_runtime_tables(paths.db_path))
    payload = {
        "runtime_root": str(paths.runtime_root),
        "db_path": str(paths.db_path),
        "initialized": bool(args.initialize),
        "missing_before": before,
        "missing_after": after,
        "ok": not after,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not after else 2


if __name__ == "__main__":
    raise SystemExit(main())

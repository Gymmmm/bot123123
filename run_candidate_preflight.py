#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from candidate_runtime.bootstrap import database_integrity, initialize_runtime_storage, missing_runtime_tables
from candidate_runtime.runtime_env import configure_candidate_environment
from v3_core.readiness import run_preflight


def main() -> int:
    parser = argparse.ArgumentParser(description="Preflight isolated V3 candidate runtime.")
    parser.add_argument("--component", default="all", choices=("all", "collector", "worker", "publisher", "user"))
    parser.add_argument("--initialize", action="store_true", help="explicitly initialize additive V3 schema")
    args = parser.parse_args()

    paths = configure_candidate_environment()
    backup = None
    if args.initialize:
        initialize_runtime_storage(paths.db_path, backup_dir=paths.backup_dir)

    missing = sorted(missing_runtime_tables(paths.db_path))
    integrity_ok, integrity = database_integrity(paths.db_path)
    report = run_preflight(repo_root=paths.repo_root, db_path=paths.db_path, component=args.component)
    payload = report.as_dict()
    payload.update({
        "runtime_root": str(paths.runtime_root),
        "db_path": str(paths.db_path),
        "missing_runtime_tables": missing,
        "sqlite_integrity": integrity,
        "sqlite_integrity_ok": integrity_ok,
        "initialized": bool(args.initialize),
        "backup": str(backup) if backup else "",
    })
    payload["ok"] = bool(report.ok and integrity_ok and not missing)
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

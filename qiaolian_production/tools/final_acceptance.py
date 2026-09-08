#!/usr/bin/env python3
"""Final structural acceptance for the extracted production runtime.

This check is intentionally conservative. It verifies that the extracted tree has
only production runtime/assets plus explicit audit tooling, that all three entry
points exist, and that already-audited slices remain fully reduced.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"

REQUIRED = {
    ROOT / "run_collector.py",
    ROOT / "run_publisher.py",
    ROOT / "run_user.py",
    ROOT / "runtime_env.py",
    ROOT / "requirements.txt",
    ROOT / "collector" / "collector_bot.py",
    ROOT / "collector" / "publication_package.py",
    ROOT / "publisher" / "qiaolian_publisher_v2" / "bot.py",
    ROOT / "user" / "qiaolian_dual" / "user_bot.py",
}

FORBIDDEN_FILE_MARKERS = (
    ".swp",
    ".bak",
    ".backup",
    ".before_",
    "test_",
    "_test.py",
    "demo",
    "sample",
)

AUDITS = (
    ("audit_publisher_modules.py", "PUBLISHER_DEAD_MODULES=0"),
    ("audit_publisher_db.py", "PUBLISHER_DB_DEAD_METHODS=0"),
    ("audit_user_modules.py", "USER_DEAD_MODULES=0"),
    ("audit_user_db_methods.py", "USER_DB_DEAD_METHODS=0"),
)


def run_audit(script: str, expected: str) -> None:
    proc = subprocess.run(
        [sys.executable, str(TOOLS / script)],
        cwd=ROOT.parent,
        text=True,
        capture_output=True,
    )
    output = (proc.stdout or "") + (proc.stderr or "")
    print(output, end="")
    if proc.returncode != 0:
        raise SystemExit(f"FINAL_ACCEPTANCE_AUDIT_FAILED={script}:{proc.returncode}")
    if expected not in output:
        raise SystemExit(f"FINAL_ACCEPTANCE_EXPECTATION_FAILED={script}:{expected}")


def main() -> int:
    missing = sorted(str(path.relative_to(ROOT)) for path in REQUIRED if not path.is_file())
    if missing:
        raise SystemExit("FINAL_ACCEPTANCE_MISSING=" + ",".join(missing))

    suspicious: list[str] = []
    for path in ROOT.rglob("*"):
        if not path.is_file() or TOOLS in path.parents:
            continue
        rel = path.relative_to(ROOT).as_posix()
        lowered = path.name.lower()
        if any(marker in lowered for marker in FORBIDDEN_FILE_MARKERS):
            suspicious.append(rel)
    if suspicious:
        raise SystemExit("FINAL_ACCEPTANCE_SUSPICIOUS_FILES=" + ",".join(sorted(suspicious)))

    for script, expected in AUDITS:
        run_audit(script, expected)

    py_files = [p for p in ROOT.rglob("*.py") if TOOLS not in p.parents]
    runtime_bytes = sum(p.stat().st_size for p in ROOT.rglob("*") if p.is_file() and TOOLS not in p.parents)
    print(f"FINAL_ACCEPTANCE_RUNTIME_PY_FILES={len(py_files)}")
    print(f"FINAL_ACCEPTANCE_RUNTIME_BYTES={runtime_bytes}")
    print("FINAL_ACCEPTANCE_STATUS=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

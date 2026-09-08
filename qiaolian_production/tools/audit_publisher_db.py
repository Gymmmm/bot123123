from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "publisher" / "qiaolian_publisher_v2" / "db.py"

source = TARGET.read_text(encoding="utf-8")
tree = ast.parse(source)

methods: list[str] = []
for node in tree.body:
    if isinstance(node, ast.ClassDef) and node.name == "Database":
        methods = [item.name for item in node.body if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))]
        break

scan_parts: list[str] = []
for path in ROOT.rglob("*.py"):
    if path == TARGET or "tools" in path.parts:
        continue
    try:
        scan_parts.append(path.read_text(encoding="utf-8"))
    except UnicodeDecodeError:
        continue
scan = "\n".join(scan_parts)

live: list[str] = []
dead: list[str] = []
for name in methods:
    if name == "__init__":
        live.append(name)
        continue
    patterns = [
        rf"\.{re.escape(name)}\s*\(",
        rf"\bDatabase\.{re.escape(name)}\b",
        rf"\bgetattr\s*\([^\n,]+,\s*['\"]{re.escape(name)}['\"]",
    ]
    if any(re.search(pattern, scan) for pattern in patterns):
        live.append(name)
    else:
        dead.append(name)

print(f"PUBLISHER_DB_METHODS={len(methods)}")
print(f"PUBLISHER_DB_LIVE_METHODS={len(live)}")
print(f"PUBLISHER_DB_DEAD_METHODS={len(dead)}")
print("PUBLISHER_DB_LIVE_NAMES=" + ",".join(sorted(live)))
print("PUBLISHER_DB_DEAD_NAMES=" + ",".join(sorted(dead)))

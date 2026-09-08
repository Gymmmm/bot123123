from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PKG = ROOT / "publisher" / "qiaolian_publisher_v2"
TARGET = PKG / "messages.py"


def defined_constants() -> set[str]:
    tree = ast.parse(TARGET.read_text(encoding="utf-8"))
    out: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name) and target.id.isupper():
                    out.add(target.id)
    return out


def referenced_constants() -> set[str]:
    used: set[str] = set()
    for path in PKG.glob("*.py"):
        if path == TARGET:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        aliases: set[str] = set()
        direct: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module in {"messages", ".messages"}:
                for alias in node.names:
                    direct.add(alias.name)
            elif isinstance(node, ast.ImportFrom) and node.module is None and node.level == 1:
                for alias in node.names:
                    if alias.name == "messages":
                        aliases.add(alias.asname or alias.name)
            elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                if node.value.id in aliases or node.value.id == "messages":
                    used.add(node.attr)
        used |= direct
    return used


def main() -> None:
    defs = defined_constants()
    refs = referenced_constants()
    live = sorted(defs & refs)
    dead = sorted(defs - refs)
    print(f"MESSAGE_CONSTANTS={len(defs)}")
    print(f"MESSAGE_LIVE_CONSTANTS={len(live)}")
    print(f"MESSAGE_DEAD_CONSTANTS={len(dead)}")
    print("MESSAGE_LIVE_NAMES=" + ",".join(live))
    print("MESSAGE_DEAD_NAMES=" + ",".join(dead))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Report top-level meihua_publisher symbols reachable from extracted production.

This is audit-only. It discovers direct imports and module-attribute references
from the extracted collector/publisher/user runtime, then follows top-level name
references inside meihua_publisher.py. No source is modified.
"""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "shared" / "meihua_publisher.py"


def _top_level_names(node: ast.AST) -> set[str]:
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return {node.name}
    if isinstance(node, (ast.Assign, ast.AnnAssign)):
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        out: set[str] = set()
        for target in targets:
            for child in ast.walk(target):
                if isinstance(child, ast.Name):
                    out.add(child.id)
        return out
    if isinstance(node, (ast.Import, ast.ImportFrom)):
        return {alias.asname or alias.name.split(".")[0] for alias in node.names}
    return set()


def _loaded_names(node: ast.AST) -> set[str]:
    return {
        child.id
        for child in ast.walk(node)
        if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Load)
    }


def _is_main_guard(node: ast.AST) -> bool:
    if not isinstance(node, ast.If):
        return False
    test = node.test
    return (
        isinstance(test, ast.Compare)
        and isinstance(test.left, ast.Name)
        and test.left.id == "__name__"
        and len(test.ops) == 1
        and isinstance(test.ops[0], ast.Eq)
        and len(test.comparators) == 1
        and isinstance(test.comparators[0], ast.Constant)
        and test.comparators[0].value == "__main__"
    )


def _external_roots() -> set[str]:
    roots: set[str] = set()
    for path in ROOT.rglob("*.py"):
        if path == TARGET or path.parent == ROOT / "tools":
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError, UnicodeDecodeError):
            continue

        aliases: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "meihua_publisher":
                        aliases.add(alias.asname or "meihua_publisher")
            elif isinstance(node, ast.ImportFrom) and node.module == "meihua_publisher":
                for alias in node.names:
                    if alias.name == "*":
                        raise RuntimeError(f"unsupported star import from meihua_publisher: {path}")
                    roots.add(alias.name)

        if aliases:
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Attribute)
                    and isinstance(node.value, ast.Name)
                    and node.value.id in aliases
                ):
                    roots.add(node.attr)
    return roots


def main() -> int:
    source = TARGET.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(TARGET))
    symbol_node: dict[str, ast.AST] = {}
    removable_defs: set[str] = set()
    baseline_refs: set[str] = set()

    for node in tree.body:
        names = _top_level_names(node)
        for name in names:
            symbol_node.setdefault(name, node)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            removable_defs.update(names)
        elif not _is_main_guard(node):
            baseline_refs.update(_loaded_names(node))

    external = _external_roots()
    missing = sorted(name for name in external if name not in symbol_node)
    roots = external | baseline_refs

    live: set[str] = set()
    queue = [name for name in roots if name in symbol_node]
    while queue:
        name = queue.pop()
        if name in live:
            continue
        live.add(name)
        for ref in _loaded_names(symbol_node[name]):
            if ref in symbol_node and ref not in live:
                queue.append(ref)

    dead = removable_defs - live
    removed_lines: set[int] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.name in dead:
            decorators = getattr(node, "decorator_list", None) or []
            start = min([node.lineno, *[int(item.lineno) for item in decorators]])
            removed_lines.update(range(start, (node.end_lineno or node.lineno) + 1))
        elif _is_main_guard(node):
            removed_lines.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))

    before = len(source.splitlines())
    after = before - len(removed_lines)
    print("MEIHUA_EXTERNAL_ROOTS=" + ",".join(sorted(external)))
    print(f"MEIHUA_EXTERNAL_ROOT_COUNT={len(external)}")
    print(f"MEIHUA_LIVE_TOP_LEVEL_SYMBOLS={len(live)}")
    print(f"MEIHUA_DEAD_TOP_LEVEL_DEFS={len(dead)}")
    print(f"MEIHUA_LINES_BEFORE={before}")
    print(f"MEIHUA_LINES_AFTER_ESTIMATE={after}")
    print("MEIHUA_DEAD_DEFS=" + ",".join(sorted(dead)))
    if missing:
        print("MEIHUA_MISSING_EXTERNAL_ROOTS=" + ",".join(missing))
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

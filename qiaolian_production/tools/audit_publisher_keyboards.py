#!/usr/bin/env python3
"""Audit and optionally remove unreachable keyboard builder functions."""
from __future__ import annotations

import argparse
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "publisher" / "qiaolian_publisher_v2" / "keyboards.py"


def _defs(tree: ast.Module) -> dict[str, ast.AST]:
    return {
        n.name: n for n in tree.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _loaded_names(node: ast.AST) -> set[str]:
    return {
        n.id for n in ast.walk(node)
        if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)
    }


def _external_roots() -> set[str]:
    roots: set[str] = set()
    for path in ROOT.rglob("*.py"):
        if path == TARGET or path.parent == ROOT / "tools":
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except Exception:
            continue
        aliases: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                is_target = module == "qiaolian_publisher_v2.keyboards"
                if path.parent.name == "qiaolian_publisher_v2" and node.level == 1 and module == "keyboards":
                    is_target = True
                if is_target:
                    roots.update(alias.name for alias in node.names if alias.name != "*")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "qiaolian_publisher_v2.keyboards":
                        aliases.add(alias.asname or "keyboards")
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id in aliases:
                roots.add(node.attr)
    return roots


def _write(source: str, tree: ast.Module, dead: set[str]) -> str:
    lines = source.splitlines(keepends=True)
    remove: set[int] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in dead:
            decorators = getattr(node, "decorator_list", []) or []
            start = min([node.lineno, *[d.lineno for d in decorators]])
            remove.update(range(start, int(node.end_lineno or node.lineno) + 1))
    return "".join("\n" if i in remove else line for i, line in enumerate(lines, start=1))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    source = TARGET.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(TARGET))
    defs = _defs(tree)
    roots = _external_roots()
    live: set[str] = set()
    queue = [r for r in roots if r in defs]
    while queue:
        name = queue.pop()
        if name in live:
            continue
        live.add(name)
        queue.extend(ref for ref in _loaded_names(defs[name]) if ref in defs and ref not in live)
    dead = set(defs) - live
    removed = sum(
        int(node.end_lineno or node.lineno) - node.lineno + 1
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in dead
    )
    print("KEYBOARD_EXTERNAL_ROOTS=" + ",".join(sorted(roots)))
    print(f"KEYBOARD_FUNCTIONS={len(defs)}")
    print(f"KEYBOARD_LIVE_FUNCTIONS={len(live)}")
    print(f"KEYBOARD_DEAD_FUNCTIONS={len(dead)}")
    print("KEYBOARD_DEAD_NAMES=" + ",".join(sorted(dead)))
    print(f"KEYBOARD_LINES_BEFORE={len(source.splitlines())}")
    print(f"KEYBOARD_LINES_AFTER_ESTIMATE={len(source.splitlines()) - removed}")
    if args.write and dead:
        TARGET.write_text(_write(source, tree, dead), encoding="utf-8")
        print("KEYBOARD_SLICE_WRITTEN=1")
    elif args.write:
        print("KEYBOARD_SLICE_WRITTEN=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

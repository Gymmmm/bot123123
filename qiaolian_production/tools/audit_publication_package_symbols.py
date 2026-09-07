#!/usr/bin/env python3
"""Audit/slice publication_package symbols reachable from extracted production.

Only direct imports and top-level module aliases are treated as external roots.
This avoids false positives when a local variable later reuses the same name as
an imported module alias. Dynamic reflection is rejected before any write.
"""
from __future__ import annotations

import argparse
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "collector" / "publication_package.py"
MODULE = "publication_package"


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


def _constant_string(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _external_roots() -> tuple[set[str], list[str]]:
    roots: set[str] = set()
    unsafe: list[str] = []
    for path in ROOT.rglob("*.py"):
        if path == TARGET or path.parent == ROOT / "tools":
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError, UnicodeDecodeError):
            continue

        # Module aliases are intentionally restricted to top-level imports. A
        # function-local name can legally shadow such an alias and must not be
        # interpreted as publication_package.<symbol> elsewhere in the file.
        aliases: set[str] = set()
        for node in tree.body:
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == MODULE:
                        aliases.add(alias.asname or MODULE)

        # Direct imports are unambiguous regardless of where they occur.
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == MODULE:
                for alias in node.names:
                    if alias.name == "*":
                        unsafe.append(f"{path.relative_to(ROOT)}:{node.lineno}:star-import")
                    else:
                        roots.add(alias.name)

        if not aliases:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id in aliases:
                roots.add(node.attr)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {"getattr", "setattr"}:
                if not node.args or not isinstance(node.args[0], ast.Name) or node.args[0].id not in aliases:
                    continue
                name = _constant_string(node.args[1] if len(node.args) > 1 else None)
                if name:
                    roots.add(name)
                else:
                    unsafe.append(f"{path.relative_to(ROOT)}:{node.lineno}:{node.func.id}-dynamic")
            if isinstance(node, ast.Attribute) and node.attr == "__dict__" and isinstance(node.value, ast.Name) and node.value.id in aliases:
                unsafe.append(f"{path.relative_to(ROOT)}:{node.lineno}:module-__dict__")
    return roots, unsafe


def _target_dynamic_reflection(tree: ast.Module) -> list[str]:
    unsafe: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {"eval", "exec", "globals", "locals"}:
            unsafe.append(f"target:{node.lineno}:{node.func.id}")
        if isinstance(node, ast.Attribute) and node.attr == "__dict__":
            unsafe.append(f"target:{node.lineno}:__dict__")
    return sorted(set(unsafe))


def _node_start_line(node: ast.AST) -> int:
    start = int(getattr(node, "lineno", 1))
    decorators = getattr(node, "decorator_list", None) or []
    if decorators:
        start = min(start, *(int(getattr(item, "lineno", start)) for item in decorators))
    return start


def analyze() -> tuple[set[str], set[str], list[str], list[str], ast.Module]:
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

    external, unsafe_external = _external_roots()
    unsafe = sorted(set([*unsafe_external, *_target_dynamic_reflection(tree)]))
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
    return live, dead, missing, unsafe, tree


def _removed_lines(tree: ast.Module, dead: set[str]) -> set[int]:
    removed: set[int] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.name in dead:
            removed.update(range(_node_start_line(node), (node.end_lineno or node.lineno) + 1))
        elif _is_main_guard(node):
            removed.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))
    return removed


def write_slice(dead: set[str], tree: ast.Module) -> None:
    lines = TARGET.read_text(encoding="utf-8").splitlines(keepends=True)
    removed = _removed_lines(tree, dead)
    text = "".join(line for no, line in enumerate(lines, start=1) if no not in removed)
    TARGET.write_text(text.rstrip() + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="remove unreachable top-level defs")
    args = parser.parse_args()

    live, dead, missing, unsafe, tree = analyze()
    source = TARGET.read_text(encoding="utf-8")
    before = len(source.splitlines())
    after = before - len(_removed_lines(tree, dead))
    external, _ = _external_roots()

    print("PACKAGE_EXTERNAL_ROOTS=" + ",".join(sorted(external)))
    print(f"PACKAGE_EXTERNAL_ROOT_COUNT={len(external)}")
    print(f"PACKAGE_LIVE_TOP_LEVEL_SYMBOLS={len(live)}")
    print(f"PACKAGE_DEAD_TOP_LEVEL_DEFS={len(dead)}")
    print(f"PACKAGE_LINES_BEFORE={before}")
    print(f"PACKAGE_LINES_AFTER_ESTIMATE={after}")
    print("PACKAGE_DEAD_DEFS=" + ",".join(sorted(dead)))
    if missing:
        print("PACKAGE_MISSING_EXTERNAL_ROOTS=" + ",".join(missing))
    if unsafe:
        print("PACKAGE_UNSAFE_DYNAMIC_REFS=" + ",".join(unsafe))
    if missing or unsafe:
        return 2
    if args.write:
        write_slice(dead, tree)
        print("PACKAGE_SLICE_WRITTEN=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

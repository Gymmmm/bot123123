#!/usr/bin/env python3
"""Audit/slice top-level meihua_publisher symbols reachable from production.

The tool discovers direct imports, module-attribute access and constant-name
getattr/setattr calls from the extracted collector/publisher/user runtime, then
follows top-level name references inside meihua_publisher.py. Dynamic reflection
that cannot be resolved statically is treated as unsafe and blocks slicing.
"""
from __future__ import annotations

import argparse
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


def _node_start_line(node: ast.AST) -> int:
    start = int(getattr(node, "lineno", 1))
    decorators = getattr(node, "decorator_list", None) or []
    if decorators:
        start = min(start, *(int(getattr(item, "lineno", start)) for item in decorators))
    return start


def _constant_string(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _module_aliases(tree: ast.AST) -> set[str]:
    aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "meihua_publisher":
                    aliases.add(alias.asname or "meihua_publisher")
    return aliases


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

        aliases = _module_aliases(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "meihua_publisher":
                for alias in node.names:
                    if alias.name == "*":
                        unsafe.append(f"{path.relative_to(ROOT)}:{node.lineno}:star-import")
                    else:
                        roots.add(alias.name)

            if aliases and isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id in aliases:
                roots.add(node.attr)

            if aliases and isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {"getattr", "setattr"}:
                if not node.args or not isinstance(node.args[0], ast.Name) or node.args[0].id not in aliases:
                    continue
                name = _constant_string(node.args[1] if len(node.args) > 1 else None)
                if name:
                    roots.add(name)
                else:
                    unsafe.append(f"{path.relative_to(ROOT)}:{node.lineno}:{node.func.id}-dynamic")

            if aliases and isinstance(node, ast.Attribute) and node.attr == "__dict__" and isinstance(node.value, ast.Name) and node.value.id in aliases:
                unsafe.append(f"{path.relative_to(ROOT)}:{node.lineno}:module-__dict__")

    return roots, unsafe


def _target_dynamic_reflection(tree: ast.Module) -> list[str]:
    """Reject unresolved module-level reflection that could hide symbol edges."""
    unsafe: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in {"eval", "exec"}:
                unsafe.append(f"target:{node.lineno}:{node.func.id}")
            elif node.func.id in {"globals", "locals"}:
                parent_hint = "reflection"
                unsafe.append(f"target:{node.lineno}:{node.func.id}-{parent_hint}")
        if isinstance(node, ast.Attribute) and node.attr == "__dict__":
            # Ordinary object __dict__ access could also be dynamic; none exists
            # in the current production module, so fail closed if one appears.
            unsafe.append(f"target:{node.lineno}:__dict__")
    return sorted(set(unsafe))


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
    unsafe = [*unsafe_external, *_target_dynamic_reflection(tree)]
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
    return live, dead, missing, sorted(set(unsafe)), tree


def _removed_lines(tree: ast.Module, dead: set[str]) -> set[int]:
    removed: set[int] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.name in dead:
            removed.update(range(_node_start_line(node), (node.end_lineno or node.lineno) + 1))
        elif _is_main_guard(node):
            removed.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))
    return removed


def write_slice(tree: ast.Module, dead: set[str]) -> None:
    lines = TARGET.read_text(encoding="utf-8").splitlines(keepends=True)
    removed = _removed_lines(tree, dead)
    sliced = "".join(line for no, line in enumerate(lines, start=1) if no not in removed)
    TARGET.write_text(sliced.rstrip() + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="remove validated unreachable top-level definitions")
    args = parser.parse_args()

    source = TARGET.read_text(encoding="utf-8")
    external, _ = _external_roots()
    live, dead, missing, unsafe, tree = analyze()
    removed = _removed_lines(tree, dead)
    before = len(source.splitlines())
    after = before - len(removed)

    print("MEIHUA_EXTERNAL_ROOTS=" + ",".join(sorted(external)))
    print(f"MEIHUA_EXTERNAL_ROOT_COUNT={len(external)}")
    print(f"MEIHUA_LIVE_TOP_LEVEL_SYMBOLS={len(live)}")
    print(f"MEIHUA_DEAD_TOP_LEVEL_DEFS={len(dead)}")
    print(f"MEIHUA_LINES_BEFORE={before}")
    print(f"MEIHUA_LINES_AFTER_ESTIMATE={after}")
    print("MEIHUA_DEAD_DEFS=" + ",".join(sorted(dead)))
    if missing:
        print("MEIHUA_MISSING_EXTERNAL_ROOTS=" + ",".join(missing))
    if unsafe:
        print("MEIHUA_UNSAFE_DYNAMIC_REFS=" + ",".join(unsafe))
    if missing or unsafe:
        return 2
    if args.write:
        write_slice(tree, dead)
        print("MEIHUA_SLICE_WRITTEN=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

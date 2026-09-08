#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "shared" / "db.py"
CLASS_NAME = "Database"


def class_node(tree: ast.Module) -> ast.ClassDef:
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == CLASS_NAME:
            return node
    raise SystemExit(f"{CLASS_NAME} not found")


def method_map(cls: ast.ClassDef) -> dict[str, ast.AST]:
    return {
        node.name: node
        for node in cls.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def constant_string(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def external_roots(methods: set[str]) -> tuple[set[str], list[str]]:
    roots: set[str] = {"__init__"}
    unsafe: list[str] = []
    for path in ROOT.rglob("*.py"):
        if path == TARGET or "tools" in path.parts:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError, UnicodeDecodeError):
            continue
        rel = path.relative_to(ROOT)
        for node in ast.walk(tree):
            # Conservative: any explicit .method access in extracted runtime
            # keeps the same-named shared Database method alive.
            if isinstance(node, ast.Attribute) and node.attr in methods:
                roots.add(node.attr)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {"getattr", "setattr", "hasattr"}:
                if len(node.args) < 2:
                    continue
                name = constant_string(node.args[1])
                if name is not None:
                    if name in methods:
                        roots.add(name)
                else:
                    obj = node.args[0]
                    if isinstance(obj, ast.Name) and obj.id in {"db", CLASS_NAME}:
                        unsafe.append(f"{rel}:{node.lineno}:{node.func.id}-dynamic-{obj.id}")
            if isinstance(node, ast.Attribute) and node.attr == "__dict__":
                if isinstance(node.value, ast.Name) and node.value.id in {"db", CLASS_NAME}:
                    unsafe.append(f"{rel}:{node.lineno}:{node.value.id}.__dict__")
    return roots, unsafe


def internal_refs(node: ast.AST, methods: set[str]) -> tuple[set[str], list[str]]:
    refs: set[str] = set()
    unsafe: list[str] = []
    for child in ast.walk(node):
        if isinstance(child, ast.Attribute) and isinstance(child.value, ast.Name) and child.value.id in {"self", "cls"}:
            if child.attr in methods:
                refs.add(child.attr)
            if child.attr == "__dict__":
                unsafe.append(f"{getattr(node, 'name', '?')}:{child.lineno}:{child.value.id}.__dict__")
        if isinstance(child, ast.Call) and isinstance(child.func, ast.Name) and child.func.id in {"getattr", "setattr", "hasattr"}:
            if len(child.args) < 2 or not isinstance(child.args[0], ast.Name) or child.args[0].id not in {"self", "cls"}:
                continue
            name = constant_string(child.args[1])
            if name is not None:
                if name in methods:
                    refs.add(name)
            else:
                unsafe.append(f"{getattr(node, 'name', '?')}:{child.lineno}:{child.func.id}-dynamic-{child.args[0].id}")
    return refs, unsafe


def write_without_methods(source: str, cls: ast.ClassDef, dead: set[str]) -> str:
    lines = source.splitlines(keepends=True)
    remove: set[int] = set()
    for node in cls.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or node.name not in dead:
            continue
        decorators = getattr(node, "decorator_list", None) or []
        start = min([node.lineno, *[int(d.lineno) for d in decorators]])
        end = int(node.end_lineno or node.lineno)
        remove.update(range(start, end + 1))
    return "".join("\n" if i in remove else line for i, line in enumerate(lines, start=1))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    source = TARGET.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(TARGET))
    cls = class_node(tree)
    nodes = method_map(cls)
    methods = set(nodes)
    roots, unsafe = external_roots(methods)

    live: set[str] = set()
    queue = [name for name in roots if name in nodes]
    while queue:
        name = queue.pop()
        if name in live:
            continue
        live.add(name)
        refs, method_unsafe = internal_refs(nodes[name], methods)
        unsafe.extend(method_unsafe)
        queue.extend(ref for ref in refs if ref not in live)

    dead = methods - live
    removed_lines = 0
    for node in cls.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in dead:
            decorators = getattr(node, "decorator_list", None) or []
            start = min([node.lineno, *[int(d.lineno) for d in decorators]])
            removed_lines += int(node.end_lineno or node.lineno) - start + 1

    before = len(source.splitlines())
    print("SHARED_DB_ROOTS=" + ",".join(sorted(roots)))
    print(f"SHARED_DB_METHODS={len(methods)}")
    print(f"SHARED_DB_LIVE_METHODS={len(live)}")
    print(f"SHARED_DB_DEAD_METHODS={len(dead)}")
    print("SHARED_DB_DEAD_NAMES=" + ",".join(sorted(dead)))
    print(f"SHARED_DB_LINES_BEFORE={before}")
    print(f"SHARED_DB_LINES_AFTER_ESTIMATE={before - removed_lines}")
    if unsafe:
        print("SHARED_DB_UNSAFE_DYNAMIC_REFS=" + ",".join(sorted(set(unsafe))))
        return 2

    if args.write and dead:
        TARGET.write_text(write_without_methods(source, cls, dead), encoding="utf-8")
        print("SHARED_DB_SLICE_WRITTEN=1")
    elif args.write:
        print("SHARED_DB_SLICE_WRITTEN=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

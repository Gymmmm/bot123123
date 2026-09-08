#!/usr/bin/env python3
"""Audit and optionally remove unreachable PublisherBot methods in extracted production.

Roots are the methods wired by the real module-level Telegram handlers plus any
explicit PublisherBot method references from the extracted runtime. The closure
then follows ``self.method`` / ``cls.method`` calls inside PublisherBot.

The tool refuses to slice when it sees dynamic reflection against PublisherBot
or a PublisherBot instance that cannot be resolved statically.
"""
from __future__ import annotations

import argparse
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "publisher" / "qiaolian_publisher_v2" / "bot.py"
CLASS_NAME = "PublisherBot"


def _class_node(tree: ast.Module) -> ast.ClassDef:
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == CLASS_NAME:
            return node
    raise RuntimeError(f"{CLASS_NAME} not found in {TARGET}")


def _method_map(cls: ast.ClassDef) -> dict[str, ast.AST]:
    return {
        node.name: node
        for node in cls.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _const_str(node: ast.AST | None) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _publisher_instance_names(tree: ast.Module) -> set[str]:
    out: set[str] = set()
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for child in ast.walk(node):
            if not isinstance(child, (ast.Assign, ast.AnnAssign)):
                continue
            value = child.value
            if not (
                isinstance(value, ast.Call)
                and isinstance(value.func, ast.Name)
                and value.func.id == CLASS_NAME
            ):
                continue
            targets = child.targets if isinstance(child, ast.Assign) else [child.target]
            for target in targets:
                if isinstance(target, ast.Name):
                    out.add(target.id)
    return out


def _roots_from_target_outside_class(tree: ast.Module, cls: ast.ClassDef, methods: set[str]) -> tuple[set[str], list[str]]:
    roots: set[str] = {"__init__"}
    unsafe: list[str] = []
    instance_names = _publisher_instance_names(tree)

    for node in tree.body:
        if node is cls:
            continue
        for child in ast.walk(node):
            if isinstance(child, ast.Attribute):
                if isinstance(child.value, ast.Name) and child.value.id in instance_names and child.attr in methods:
                    roots.add(child.attr)
                if isinstance(child.value, ast.Name) and child.value.id == CLASS_NAME and child.attr in methods:
                    roots.add(child.attr)
            if isinstance(child, ast.Call) and isinstance(child.func, ast.Name) and child.func.id in {"getattr", "setattr", "hasattr"}:
                if len(child.args) < 2 or not isinstance(child.args[0], ast.Name):
                    continue
                obj = child.args[0].id
                if obj != CLASS_NAME and obj not in instance_names:
                    continue
                name = _const_str(child.args[1])
                if name:
                    if name in methods:
                        roots.add(name)
                else:
                    unsafe.append(f"target:{child.lineno}:{child.func.id}-dynamic-{obj}")
            if isinstance(child, ast.Attribute) and child.attr == "__dict__" and isinstance(child.value, ast.Name):
                if child.value.id == CLASS_NAME or child.value.id in instance_names:
                    unsafe.append(f"target:{child.lineno}:{child.value.id}.__dict__")
    return roots, unsafe


def _roots_from_other_files(methods: set[str]) -> tuple[set[str], list[str]]:
    roots: set[str] = set()
    unsafe: list[str] = []
    for path in ROOT.rglob("*.py"):
        if path == TARGET or path.parent == ROOT / "tools":
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError, UnicodeDecodeError):
            continue
        rel = path.relative_to(ROOT)
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == CLASS_NAME:
                if node.attr in methods:
                    roots.add(node.attr)
                if node.attr == "__dict__":
                    unsafe.append(f"{rel}:{node.lineno}:{CLASS_NAME}.__dict__")
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {"getattr", "setattr", "hasattr"}:
                if len(node.args) < 2 or not isinstance(node.args[0], ast.Name) or node.args[0].id != CLASS_NAME:
                    continue
                name = _const_str(node.args[1])
                if name:
                    if name in methods:
                        roots.add(name)
                else:
                    unsafe.append(f"{rel}:{node.lineno}:{node.func.id}-dynamic-{CLASS_NAME}")
    return roots, unsafe


def _method_refs(node: ast.AST, methods: set[str]) -> tuple[set[str], list[str]]:
    refs: set[str] = set()
    unsafe: list[str] = []
    arg_names = {arg.arg for arg in getattr(node, "args", ast.arguments([], [], None, [], [], None, [])).args}
    receiver_names = {name for name in ("self", "cls") if name in arg_names}
    for child in ast.walk(node):
        if isinstance(child, ast.Attribute) and isinstance(child.value, ast.Name) and child.value.id in receiver_names:
            if child.attr in methods:
                refs.add(child.attr)
            if child.attr == "__dict__":
                unsafe.append(f"method:{getattr(node, 'name', '?')}:{child.lineno}:{child.value.id}.__dict__")
        if isinstance(child, ast.Call) and isinstance(child.func, ast.Name) and child.func.id in {"getattr", "setattr", "hasattr"}:
            if len(child.args) < 2 or not isinstance(child.args[0], ast.Name) or child.args[0].id not in receiver_names:
                continue
            name = _const_str(child.args[1])
            if name:
                if name in methods:
                    refs.add(name)
            else:
                unsafe.append(
                    f"method:{getattr(node, 'name', '?')}:{child.lineno}:{child.func.id}-dynamic-{child.args[0].id}"
                )
    return refs, unsafe


def _write_without_methods(source: str, cls: ast.ClassDef, dead: set[str]) -> str:
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
    cls = _class_node(tree)
    method_nodes = _method_map(cls)
    methods = set(method_nodes)

    roots_a, unsafe_a = _roots_from_target_outside_class(tree, cls, methods)
    roots_b, unsafe_b = _roots_from_other_files(methods)
    roots = roots_a | roots_b
    unsafe = [*unsafe_a, *unsafe_b]

    live: set[str] = set()
    queue = [name for name in roots if name in method_nodes]
    while queue:
        name = queue.pop()
        if name in live:
            continue
        live.add(name)
        refs, method_unsafe = _method_refs(method_nodes[name], methods)
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
    print("PUBLISHER_BOT_ROOTS=" + ",".join(sorted(roots)))
    print(f"PUBLISHER_BOT_METHOD_COUNT={len(methods)}")
    print(f"PUBLISHER_BOT_LIVE_METHODS={len(live)}")
    print(f"PUBLISHER_BOT_DEAD_METHODS={len(dead)}")
    print(f"PUBLISHER_BOT_LINES_BEFORE={before}")
    print(f"PUBLISHER_BOT_LINES_AFTER_ESTIMATE={before - removed_lines}")
    print("PUBLISHER_BOT_DEAD_METHOD_NAMES=" + ",".join(sorted(dead)))
    if unsafe:
        print("PUBLISHER_BOT_UNSAFE_DYNAMIC_REFS=" + ",".join(sorted(set(unsafe))))
        return 2

    if args.write and dead:
        TARGET.write_text(_write_without_methods(source, cls, dead), encoding="utf-8")
        print("PUBLISHER_BOT_SLICE_WRITTEN=1")
    elif args.write:
        print("PUBLISHER_BOT_SLICE_WRITTEN=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

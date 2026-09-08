#!/usr/bin/env python3
"""Report unreachable modules inside the extracted qiaolian_publisher_v2 package."""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PKG = ROOT / "publisher" / "qiaolian_publisher_v2"
PACKAGE = "qiaolian_publisher_v2"
ROOT_MODULES = {
    "__init__",
    "bot",
    "cover_picker_patch",
    "daily_broadcast_patch",
    "media_selection_patch",
    "review_queue_patch",
    "release_contract_patch",
}


def _module_name(path: Path) -> str:
    rel = path.relative_to(PKG).with_suffix("")
    return ".".join(rel.parts)


def _module_files() -> dict[str, Path]:
    return {_module_name(path): path for path in PKG.rglob("*.py")}


def _resolve_relative(current: str, node: ast.ImportFrom) -> str | None:
    if node.level <= 0:
        return None
    current_parts = current.split(".")
    base = current_parts[:-1]
    up = max(0, node.level - 1)
    if up:
        base = base[:-up] if up <= len(base) else []
    if node.module:
        base.extend(node.module.split("."))
    return ".".join(base)


def _deps(name: str, path: Path, modules: set[str]) -> tuple[set[str], list[str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    deps: set[str] = set()
    unsafe: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level > 0:
                base = _resolve_relative(name, node)
                if base in modules:
                    deps.add(base)
                for alias in node.names:
                    child = f"{base}.{alias.name}" if base else alias.name
                    if child in modules:
                        deps.add(child)
            elif node.module == PACKAGE:
                for alias in node.names:
                    if alias.name in modules:
                        deps.add(alias.name)
            elif node.module and node.module.startswith(PACKAGE + "."):
                child = node.module[len(PACKAGE) + 1 :]
                if child in modules:
                    deps.add(child)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith(PACKAGE + "."):
                    child = alias.name[len(PACKAGE) + 1 :]
                    if child in modules:
                        deps.add(child)
        elif isinstance(node, ast.Call):
            fn = node.func
            is_dynamic_import = (
                isinstance(fn, ast.Name) and fn.id == "__import__"
            ) or (
                isinstance(fn, ast.Attribute)
                and isinstance(fn.value, ast.Name)
                and fn.value.id == "importlib"
                and fn.attr == "import_module"
            )
            if not is_dynamic_import:
                continue
            arg = node.args[0] if node.args else None
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                raw = arg.value
                if raw.startswith(PACKAGE + "."):
                    child = raw[len(PACKAGE) + 1 :]
                    if child in modules:
                        deps.add(child)
            else:
                unsafe.append(f"{path.relative_to(ROOT)}:{node.lineno}:dynamic-import")
    return deps, unsafe


def main() -> int:
    files = _module_files()
    modules = set(files)
    missing_roots = sorted(ROOT_MODULES - modules)
    if missing_roots:
        print("PUBLISHER_MODULE_MISSING_ROOTS=" + ",".join(missing_roots))
        return 2

    graph: dict[str, set[str]] = {}
    unsafe: list[str] = []
    for name, path in files.items():
        deps, local_unsafe = _deps(name, path, modules)
        graph[name] = deps
        unsafe.extend(local_unsafe)

    live: set[str] = set()
    queue = list(ROOT_MODULES)
    while queue:
        name = queue.pop()
        if name in live:
            continue
        live.add(name)
        queue.extend(dep for dep in graph.get(name, set()) if dep not in live)

    dead = modules - live
    print(f"PUBLISHER_MODULE_COUNT={len(modules)}")
    print(f"PUBLISHER_LIVE_MODULES={len(live)}")
    print(f"PUBLISHER_DEAD_MODULES={len(dead)}")
    print("PUBLISHER_DEAD_MODULE_NAMES=" + ",".join(sorted(dead)))
    if unsafe:
        print("PUBLISHER_MODULE_UNSAFE_DYNAMIC_IMPORTS=" + ",".join(sorted(set(unsafe))))
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

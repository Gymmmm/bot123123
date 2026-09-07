#!/usr/bin/env python3
"""Audit/slice the extracted autopilot helper to symbols reachable in production.

This operates only on qiaolian_production/shared/autopilot_publish_bot.py.
Production roots are discovered from the active Publisher Bot and its runtime
patch modules. The production call to register_autopilot_features always uses
simple_mode=True, so dependency scanning of that function intentionally follows
only the simple-mode branch. The historical standalone ``main`` entrypoint is
not a production service entrypoint and is excluded together with its guard.
"""
from __future__ import annotations

import argparse
import ast
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "shared" / "autopilot_publish_bot.py"
HOST_SOURCES = [
    ROOT / "publisher" / "qiaolian_publisher_v2" / "bot.py",
    ROOT / "publisher" / "qiaolian_publisher_v2" / "cover_picker_patch.py",
    ROOT / "publisher" / "qiaolian_publisher_v2" / "daily_broadcast_patch.py",
    ROOT / "publisher" / "qiaolian_publisher_v2" / "media_selection_patch.py",
    ROOT / "publisher" / "qiaolian_publisher_v2" / "review_queue_patch.py",
    ROOT / "publisher" / "qiaolian_publisher_v2" / "release_contract_patch.py",
]

# Symbols registered only by the live simple-mode branch. Host-source discovery
# adds every ap.<symbol> / direct import used by the active Publisher and patches.
SIMPLE_MODE_ROOTS = {
    "register_autopilot_features",
    "clear_autopilot_input_state",
    "cmd_pending",
    "cmd_send",
    "cmd_status",
    "cmd_logs",
    "cmd_sources",
    "cmd_source_add",
    "cmd_source_on",
    "cmd_source_off",
    "cmd_daily",
    "cmd_daily_on",
    "cmd_daily_off",
    "cmd_daily_time",
    "cmd_daily_text",
    "cmd_intake",
    "cmd_intake_done",
    "on_preview_callback",
    "on_daily_callback",
    "on_photo_private",
    "on_text_private",
    "notify_new_reviewable_drafts",
    "tick_daily_broadcast",
}


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


def _simple_register_refs(node: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    """Scan only the production simple_mode path of register_autopilot_features."""
    refs: set[str] = set()
    for stmt in node.body:
        if isinstance(stmt, ast.If) and isinstance(stmt.test, ast.Name) and stmt.test.id == "simple_mode":
            for child in stmt.body:
                refs.update(_loaded_names(child))
            break
        refs.update(_loaded_names(stmt))
    return refs


def _host_roots() -> set[str]:
    roots: set[str] = set()
    for path in HOST_SOURCES:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "ap":
                roots.add(node.attr)
            elif isinstance(node, ast.ImportFrom) and node.module == "autopilot_publish_bot":
                roots.update(alias.name for alias in node.names)
    return roots


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


def analyze() -> tuple[set[str], set[str], set[str], ast.Module, dict[str, ast.AST]]:
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
            # Conservatively preserve top-level setup/assignments/imports and any
            # helper definitions they reference. The standalone main guard is
            # explicitly outside the active systemd Publisher entrypoint.
            baseline_refs.update(_loaded_names(node))

    roots = SIMPLE_MODE_ROOTS | _host_roots() | baseline_refs
    required = SIMPLE_MODE_ROOTS | _host_roots()
    missing = {name for name in required if name not in symbol_node}

    live: set[str] = set()
    queue = [name for name in roots if name in symbol_node]
    while queue:
        name = queue.pop()
        if name in live:
            continue
        live.add(name)
        node = symbol_node[name]
        if name == "register_autopilot_features" and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            refs = _simple_register_refs(node)
        else:
            refs = _loaded_names(node)
        for ref in refs:
            if ref in symbol_node and ref not in live:
                queue.append(ref)

    dead = removable_defs - live
    return live, dead, missing, tree, symbol_node


def _removed_lines(tree: ast.Module, dead: set[str]) -> set[int]:
    removed: set[int] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.name in dead:
            removed.update(range(_node_start_line(node), (node.end_lineno or node.lineno) + 1))
        elif _is_main_guard(node):
            removed.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))
    return removed


def _line_count_after_slice(tree: ast.Module, dead: set[str]) -> tuple[int, int]:
    source_lines = TARGET.read_text(encoding="utf-8").splitlines()
    removed = _removed_lines(tree, dead)
    return len(source_lines), len(source_lines) - len(removed)


def write_slice(dead: set[str], tree: ast.Module) -> None:
    lines = TARGET.read_text(encoding="utf-8").splitlines(keepends=True)
    removed = _removed_lines(tree, dead)
    sliced = "".join(line for no, line in enumerate(lines, start=1) if no not in removed)
    # Keep exactly one terminal newline so git diff --check does not report
    # blank lines at EOF after the historical standalone main block is removed.
    TARGET.write_text(sliced.rstrip() + "\n", encoding="utf-8")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="remove unreachable top-level defs from extracted helper")
    args = parser.parse_args(list(argv) if argv is not None else None)

    live, dead, missing, tree, _ = analyze()
    before, after = _line_count_after_slice(tree, dead)
    print(f"AUTOPILOT_RUNTIME_ROOTS={len(SIMPLE_MODE_ROOTS | _host_roots())}")
    print(f"AUTOPILOT_LIVE_TOP_LEVEL_SYMBOLS={len(live)}")
    print(f"AUTOPILOT_DEAD_TOP_LEVEL_DEFS={len(dead)}")
    print(f"AUTOPILOT_LINES_BEFORE={before}")
    print(f"AUTOPILOT_LINES_AFTER_ESTIMATE={after}")
    print("AUTOPILOT_DEAD_DEFS=" + ",".join(sorted(dead)))
    if missing:
        print("AUTOPILOT_MISSING_ROOTS=" + ",".join(sorted(missing)))
        return 2
    if args.write:
        write_slice(dead, tree)
        print("AUTOPILOT_SLICE_WRITTEN=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

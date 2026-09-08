from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "user" / "qiaolian_dual" / "listing_taxonomy.py"
PACKAGE = ROOT / "user" / "qiaolian_dual"
TARGET_MODULE = "qiaolian_dual.listing_taxonomy"


def top_level_symbols(tree: ast.Module) -> dict[str, ast.AST]:
    out: dict[str, ast.AST] = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            out[node.name] = node
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name):
                    out[target.id] = node
    return out


def referenced_names(node: ast.AST) -> set[str]:
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}


def scan_external_roots() -> tuple[set[str], list[str]]:
    roots: set[str] = set()
    unsafe: list[str] = []
    target_resolved = TARGET.resolve()
    for path in ROOT.rglob("*.py"):
        if path.resolve() == target_resolved or path.resolve() == Path(__file__).resolve():
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (SyntaxError, UnicodeDecodeError):
            continue
        aliases: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                is_target = module == TARGET_MODULE
                if not is_target and node.level and path.is_relative_to(PACKAGE):
                    # listing_taxonomy is a sibling module for qiaolian_dual files.
                    is_target = module == "listing_taxonomy"
                if is_target:
                    for item in node.names:
                        if item.name == "*":
                            unsafe.append(f"{path.relative_to(ROOT)}: star import")
                        else:
                            roots.add(item.name)
            elif isinstance(node, ast.Import):
                for item in node.names:
                    if item.name == TARGET_MODULE:
                        aliases.add(item.asname or item.name.split(".")[-1])
            elif isinstance(node, ast.Call):
                fn = node.func
                is_import_module = (
                    isinstance(fn, ast.Name) and fn.id == "import_module"
                ) or (
                    isinstance(fn, ast.Attribute)
                    and isinstance(fn.value, ast.Name)
                    and fn.value.id == "importlib"
                    and fn.attr == "import_module"
                )
                if is_import_module and node.args and isinstance(node.args[0], ast.Constant):
                    if node.args[0].value == TARGET_MODULE:
                        unsafe.append(f"{path.relative_to(ROOT)}: dynamic import_module({TARGET_MODULE!r})")
        if aliases:
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id in aliases:
                    roots.add(node.attr)
                elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {"getattr", "setattr"}:
                    if node.args and isinstance(node.args[0], ast.Name) and node.args[0].id in aliases:
                        if len(node.args) > 1 and isinstance(node.args[1], ast.Constant) and isinstance(node.args[1].value, str):
                            roots.add(node.args[1].value)
                        else:
                            unsafe.append(f"{path.relative_to(ROOT)}: dynamic {node.func.id} on listing_taxonomy")
    return roots, unsafe


source = TARGET.read_text(encoding="utf-8")
tree = ast.parse(source, filename=str(TARGET))
symbols = top_level_symbols(tree)
roots, unsafe = scan_external_roots()
missing = sorted(name for name in roots if name not in symbols)

live = set(name for name in roots if name in symbols)
stack = list(live)
while stack:
    name = stack.pop()
    node = symbols[name]
    for dep in referenced_names(node):
        if dep in symbols and dep not in live:
            live.add(dep)
            stack.append(dep)

defined_defs = {
    name for name, node in symbols.items()
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
}
dead_defs = sorted(defined_defs - live)
lines = source.splitlines()
removed = 0
for name in dead_defs:
    node = symbols[name]
    end = getattr(node, "end_lineno", node.lineno)
    removed += end - node.lineno + 1

print("TAXONOMY_EXTERNAL_ROOTS=" + ",".join(sorted(roots)))
print(f"TAXONOMY_EXTERNAL_ROOT_COUNT={len(roots)}")
print(f"TAXONOMY_TOP_LEVEL_SYMBOLS={len(symbols)}")
print(f"TAXONOMY_LIVE_TOP_LEVEL_SYMBOLS={len(live)}")
print(f"TAXONOMY_DEAD_TOP_LEVEL_DEFS={len(dead_defs)}")
print("TAXONOMY_DEAD_DEF_NAMES=" + ",".join(dead_defs))
print(f"TAXONOMY_LINES_BEFORE={len(lines)}")
print(f"TAXONOMY_LINES_AFTER_ESTIMATE={len(lines) - removed}")
if missing:
    raise SystemExit("missing listing_taxonomy roots: " + repr(missing))
if unsafe:
    raise SystemExit("unsafe listing_taxonomy references: " + repr(sorted(set(unsafe))))

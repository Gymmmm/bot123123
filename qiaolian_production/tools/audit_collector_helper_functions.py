from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COLLECTOR_DIR = ROOT / "collector"
TARGET_MODULES = {
    "ai_parser",
    "collector_db_compat",
    "source_sanitizer",
}
DYNAMIC_NAMES = {"getattr", "setattr", "hasattr", "globals", "locals", "eval", "exec"}


class ModuleInfo:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.tree = ast.parse(path.read_text(encoding="utf-8"))
        self.functions = {
            node.name: node
            for node in self.tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        self.internal_refs: dict[str, set[str]] = {name: set() for name in self.functions}
        for name, node in self.functions.items():
            for child in ast.walk(node):
                if isinstance(child, ast.Name) and child.id in self.functions:
                    self.internal_refs[name].add(child.id)

        self.module_roots: set[str] = set()
        for node in self.tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            # Class methods and module-level executable code can reference
            # top-level helpers used at runtime.
            for child in ast.walk(node):
                if isinstance(child, ast.Name) and child.id in self.functions:
                    self.module_roots.add(child.id)


infos = {
    module: ModuleInfo(COLLECTOR_DIR / f"{module}.py")
    for module in sorted(TARGET_MODULES)
    if (COLLECTOR_DIR / f"{module}.py").exists()
}
external_roots: dict[str, set[str]] = {
    module: set(info.module_roots) for module, info in infos.items()
}
dynamic_modules: set[str] = set()


def imported_target(node: ast.ImportFrom) -> str:
    raw = node.module or ""
    if raw in infos:
        return raw
    prefix = "qiaolian_production.collector."
    if raw.startswith(prefix):
        return raw[len(prefix):]
    marker = ".collector."
    if marker in raw:
        return raw.rsplit(marker, 1)[1]
    return ""


for path in ROOT.rglob("*.py"):
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError):
        continue

    aliases: dict[str, str] = {}
    imported_names: dict[str, tuple[str, str]] = {}

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                raw = alias.name
                target = ""
                if raw in infos:
                    target = raw
                elif raw.startswith("qiaolian_production.collector."):
                    target = raw[len("qiaolian_production.collector."):]
                elif ".collector." in raw:
                    target = raw.rsplit(".collector.", 1)[1]
                if target in infos:
                    aliases[alias.asname or raw.rsplit(".", 1)[-1]] = target
        elif isinstance(node, ast.ImportFrom):
            target = imported_target(node)
            if target in infos:
                for alias in node.names:
                    if alias.name == "*":
                        dynamic_modules.add(target)
                    elif alias.name in infos[target].functions:
                        imported_names[alias.asname or alias.name] = (target, alias.name)
                        external_roots[target].add(alias.name)

    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            module = aliases.get(node.value.id)
            if module and node.attr in infos[module].functions:
                external_roots[module].add(node.attr)
        elif isinstance(node, ast.Name) and node.id in imported_names:
            module, func = imported_names[node.id]
            external_roots[module].add(func)
        elif isinstance(node, ast.Call):
            fn = node.func
            if isinstance(fn, ast.Name) and fn.id in DYNAMIC_NAMES:
                dynamic_modules.update(aliases.values())
            elif isinstance(fn, ast.Name) and fn.id == "__import__":
                dynamic_modules.update(infos)
            elif isinstance(fn, ast.Attribute) and fn.attr == "import_module":
                dynamic_modules.update(infos)

for module, info in infos.items():
    roots = set(info.functions) if module in dynamic_modules else set(external_roots[module])
    live = set(roots)
    queue = list(roots)
    while queue:
        current = queue.pop()
        for dep in info.internal_refs.get(current, ()):
            if dep not in live:
                live.add(dep)
                queue.append(dep)
    dead = sorted(set(info.functions) - live)
    print(f"COLLECTOR_HELPER_MODULE={module}")
    print(f"COLLECTOR_HELPER_FUNCTIONS={len(info.functions)}")
    print(f"COLLECTOR_HELPER_ROOTS={','.join(sorted(roots))}")
    print(f"COLLECTOR_HELPER_LIVE={','.join(sorted(live))}")
    print(f"COLLECTOR_HELPER_DEAD={','.join(dead)}")
    print(f"COLLECTOR_HELPER_DYNAMIC={'1' if module in dynamic_modules else '0'}")

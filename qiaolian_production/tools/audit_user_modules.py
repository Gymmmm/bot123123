from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
USER_DIR = ROOT / "user" / "qiaolian_dual"


def module_name(path: Path) -> str:
    rel = path.relative_to(USER_DIR)
    parts = list(rel.with_suffix("").parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


FILES = {module_name(p): p for p in USER_DIR.rglob("*.py")}
FILES.pop("", None)
MODULES = set(FILES)


def resolve_relative(current: str, level: int, module: str | None) -> str:
    pkg = current.split(".")[:-1]
    if level:
        pkg = pkg[: max(0, len(pkg) - level + 1)]
    if module:
        pkg += module.split(".")
    return ".".join(pkg)


def refs_from_tree(tree: ast.AST, current: str) -> set[str]:
    refs: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("qiaolian_dual."):
                    cand = alias.name[len("qiaolian_dual."):]
                    if cand in MODULES:
                        refs.add(cand)
        elif isinstance(node, ast.ImportFrom):
            if node.module == "qiaolian_dual":
                for alias in node.names:
                    if alias.name in MODULES:
                        refs.add(alias.name)
            elif node.module and node.module.startswith("qiaolian_dual."):
                cand = node.module[len("qiaolian_dual."):]
                if cand in MODULES:
                    refs.add(cand)
            elif node.level:
                cand = resolve_relative(current, node.level, node.module)
                if cand in MODULES:
                    refs.add(cand)
                for alias in node.names:
                    child = f"{cand}.{alias.name}" if cand else alias.name
                    if child in MODULES:
                        refs.add(child)
        elif isinstance(node, ast.Call):
            fn = node.func
            is_import_module = (
                isinstance(fn, ast.Name) and fn.id == "import_module"
            ) or (
                isinstance(fn, ast.Attribute)
                and fn.attr == "import_module"
            )
            if is_import_module and node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                raw = node.args[0].value
                if raw.startswith("qiaolian_dual."):
                    cand = raw[len("qiaolian_dual."):]
                    if cand in MODULES:
                        refs.add(cand)
    return refs


DEPS: dict[str, set[str]] = {}
for mod, path in FILES.items():
    DEPS[mod] = refs_from_tree(ast.parse(path.read_text(encoding="utf-8")), mod)

# Runtime roots: public user-bot entrypoints plus qiaolian_dual modules referenced
# from collector/publisher/shared extraction code outside the copied user package.
roots = {m for m in ("user_bot", "app") if m in MODULES}
for path in ROOT.rglob("*.py"):
    try:
        path.relative_to(USER_DIR)
        continue
    except ValueError:
        pass
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError):
        continue
    roots |= refs_from_tree(tree, "")

live = set(roots)
queue = list(roots)
while queue:
    mod = queue.pop()
    for dep in DEPS.get(mod, ()):
        if dep not in live:
            live.add(dep)
            queue.append(dep)

dead = sorted(MODULES - live)
print("USER_MODULE_COUNT=" + str(len(MODULES)))
print("USER_ROOT_COUNT=" + str(len(roots)))
print("USER_LIVE_MODULES=" + str(len(live)))
print("USER_DEAD_MODULES=" + str(len(dead)))
print("USER_ROOT_NAMES=" + ",".join(sorted(roots)))
print("USER_DEAD_MODULE_NAMES=" + ",".join(dead))

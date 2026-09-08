from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
USER_DIR = ROOT / "user" / "qiaolian_dual"
PACKAGE = "qiaolian_dual"


def module_name(path: Path) -> str:
    rel = path.relative_to(USER_DIR).with_suffix("")
    parts = list(rel.parts)
    if parts and parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def bind_names(target: ast.AST) -> set[str]:
    if isinstance(target, ast.Name):
        return {target.id}
    if isinstance(target, (ast.Tuple, ast.List)):
        out: set[str] = set()
        for item in target.elts:
            out |= bind_names(item)
        return out
    return set()


FILES = {module_name(path): path for path in USER_DIR.rglob("*.py")}
FILES.pop("", None)
MODULES = set(FILES)
TOP_LEVEL: dict[str, set[str]] = {}
STAR_IMPORTS: dict[str, list[str]] = {}

for module, path in FILES.items():
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    stars: list[str] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                names |= bind_names(target)
        elif isinstance(node, ast.AnnAssign):
            names |= bind_names(node.target)
        elif isinstance(node, (ast.For, ast.AsyncFor)):
            names |= bind_names(node.target)
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            for item in node.items:
                if item.optional_vars is not None:
                    names |= bind_names(item.optional_vars)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.asname or alias.name.split(".", 1)[0])
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == "*":
                    stars.append(node.module or "")
                else:
                    names.add(alias.asname or alias.name)
    TOP_LEVEL[module] = names
    STAR_IMPORTS[module] = stars


def local_target(current: str, node: ast.ImportFrom) -> str | None:
    if node.level == 1:
        return node.module or ""
    raw = node.module or ""
    if raw == PACKAGE:
        return ""
    if raw.startswith(PACKAGE + "."):
        return raw[len(PACKAGE) + 1 :]
    return None


errors: list[str] = []
checked_symbols = 0
checked_modules = 0
star_import_count = 0

for current, path in sorted(FILES.items()):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                raw = alias.name
                if raw == PACKAGE:
                    continue
                if raw.startswith(PACKAGE + "."):
                    target = raw[len(PACKAGE) + 1 :]
                    checked_modules += 1
                    if target not in MODULES:
                        errors.append(f"{current}: missing local module {raw}")
        elif isinstance(node, ast.ImportFrom):
            target = local_target(current, node)
            if target is None:
                continue
            if target == "":
                for alias in node.names:
                    if alias.name == "*":
                        star_import_count += 1
                        continue
                    checked_modules += 1
                    if alias.name not in MODULES:
                        errors.append(f"{current}: missing local module {PACKAGE}.{alias.name}")
                continue
            checked_modules += 1
            if target not in MODULES:
                errors.append(f"{current}: missing local module {PACKAGE}.{target}")
                continue
            for alias in node.names:
                if alias.name == "*":
                    star_import_count += 1
                    continue
                checked_symbols += 1
                if alias.name not in TOP_LEVEL[target]:
                    errors.append(
                        f"{current}: missing local symbol {PACKAGE}.{target}.{alias.name}"
                    )

print(f"USER_IMPORT_MODULES={len(MODULES)}")
print(f"USER_IMPORT_MODULE_CHECKS={checked_modules}")
print(f"USER_IMPORT_SYMBOL_CHECKS={checked_symbols}")
print(f"USER_IMPORT_STAR_IMPORTS={star_import_count}")
print(f"USER_IMPORT_ERRORS={len(errors)}")
for error in sorted(errors):
    print("USER_IMPORT_ERROR=" + error)
if errors:
    raise SystemExit(1)
print("USER_IMPORT_STATUS=PASS")

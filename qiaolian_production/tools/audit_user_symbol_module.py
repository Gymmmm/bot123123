from __future__ import annotations

import argparse
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "user" / "qiaolian_dual"


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


def top_level_import_bindings(tree: ast.Module) -> set[str]:
    """Names intentionally re-exported by normal top-level imports."""
    out: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            for item in node.names:
                if item.name != "*":
                    out.add(item.asname or item.name)
        elif isinstance(node, ast.Import):
            for item in node.names:
                out.add(item.asname or item.name.split(".")[0])
    return out


def referenced_names(node: ast.AST) -> set[str]:
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}


def scan_roots(target: Path, short_module: str) -> tuple[set[str], list[str]]:
    roots: set[str] = set()
    unsafe: list[str] = []
    full_module = f"qiaolian_dual.{short_module}"
    target_resolved = target.resolve()
    self_resolved = Path(__file__).resolve()

    for path in ROOT.rglob("*.py"):
        if path.resolve() in {target_resolved, self_resolved}:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (SyntaxError, UnicodeDecodeError):
            continue
        aliases: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                is_target = module == full_module
                if not is_target and node.level and path.is_relative_to(PACKAGE):
                    is_target = module == short_module
                if is_target:
                    for item in node.names:
                        if item.name == "*":
                            unsafe.append(f"{path.relative_to(ROOT)}: star import")
                        else:
                            roots.add(item.name)
            elif isinstance(node, ast.Import):
                for item in node.names:
                    if item.name == full_module:
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
                    raw = node.args[0].value
                    if raw == full_module or raw == f".{short_module}":
                        unsafe.append(f"{path.relative_to(ROOT)}: dynamic import_module({raw!r})")
        if aliases:
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id in aliases:
                    roots.add(node.attr)
                elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {"getattr", "setattr"}:
                    if node.args and isinstance(node.args[0], ast.Name) and node.args[0].id in aliases:
                        if len(node.args) > 1 and isinstance(node.args[1], ast.Constant) and isinstance(node.args[1].value, str):
                            roots.add(node.args[1].value)
                        else:
                            unsafe.append(f"{path.relative_to(ROOT)}: dynamic {node.func.id} on {short_module}")
    return roots, unsafe


def write_slice(target: Path, source: str, tree: ast.Module, symbols: dict[str, ast.AST], dead: list[str]) -> None:
    if not dead:
        return
    lines = source.splitlines(keepends=True)
    remove_lines: set[int] = set()
    for name in dead:
        node = symbols[name]
        for lineno in range(node.lineno, getattr(node, "end_lineno", node.lineno) + 1):
            remove_lines.add(lineno)
    output = [line for lineno, line in enumerate(lines, start=1) if lineno not in remove_lines]
    text = "".join(output)

    # Keep an explicit __all__ export list consistent with the sliced module.
    sliced_tree = ast.parse(text, filename=str(target))
    text_lines = text.splitlines(keepends=True)
    for node in sliced_tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets):
            continue
        if not isinstance(node.value, (ast.List, ast.Tuple)):
            raise SystemExit("unsafe dynamic __all__ while writing slice")
        values: list[str] = []
        for elt in node.value.elts:
            if not isinstance(elt, ast.Constant) or not isinstance(elt.value, str):
                raise SystemExit("unsafe non-string __all__ while writing slice")
            if elt.value not in dead:
                values.append(elt.value)
        replacement = "__all__ = " + repr(values) + "\n"
        start = node.lineno - 1
        end = getattr(node, "end_lineno", node.lineno)
        text_lines[start:end] = [replacement]
        text = "".join(text_lines)
        break

    target.write_text(text.rstrip() + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("module", help="qiaolian_dual module path without .py, e.g. canonical_facts")
    parser.add_argument("--write", action="store_true", help="remove audited dead top-level defs from the target module")
    args = parser.parse_args()
    short_module = args.module.replace("/", ".").removesuffix(".py")
    target = PACKAGE.joinpath(*short_module.split(".")).with_suffix(".py")
    if not target.exists():
        raise SystemExit(f"module file not found: {target}")

    source = target.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(target))
    symbols = top_level_symbols(tree)
    imported = top_level_import_bindings(tree)
    roots, unsafe = scan_roots(target, short_module)
    missing = sorted(name for name in roots if name not in symbols and name not in imported)

    live = {name for name in roots if name in symbols}
    stack = list(live)
    while stack:
        name = stack.pop()
        for dep in referenced_names(symbols[name]):
            if dep in symbols and dep not in live:
                live.add(dep)
                stack.append(dep)

    defs = {name for name, node in symbols.items() if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
    dead = sorted(defs - live)
    lines = source.splitlines()
    removable = sum(
        getattr(symbols[name], "end_lineno", symbols[name].lineno) - symbols[name].lineno + 1
        for name in dead
    )
    prefix = short_module.upper().replace(".", "_")
    print(f"{prefix}_EXTERNAL_ROOTS=" + ",".join(sorted(roots)))
    print(f"{prefix}_EXTERNAL_ROOT_COUNT={len(roots)}")
    print(f"{prefix}_REEXPORTED_IMPORTS=" + ",".join(sorted(roots & imported)))
    print(f"{prefix}_TOP_LEVEL_SYMBOLS={len(symbols)}")
    print(f"{prefix}_LIVE_TOP_LEVEL_SYMBOLS={len(live)}")
    print(f"{prefix}_DEAD_TOP_LEVEL_DEFS={len(dead)}")
    print(f"{prefix}_DEAD_DEF_NAMES=" + ",".join(dead))
    print(f"{prefix}_LINES_BEFORE={len(lines)}")
    print(f"{prefix}_LINES_AFTER_ESTIMATE={len(lines) - removable}")
    if missing:
        raise SystemExit(f"missing {short_module} roots: {missing!r}")
    if unsafe:
        raise SystemExit(f"unsafe {short_module} references: {sorted(set(unsafe))!r}")
    if args.write:
        write_slice(target, source, tree, symbols, dead)
        print(f"{prefix}_SLICE_WRITTEN={1 if dead else 0}")


if __name__ == "__main__":
    main()

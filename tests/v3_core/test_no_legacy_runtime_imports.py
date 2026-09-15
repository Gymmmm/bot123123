"""Keep the V3 runtime from silently reconnecting to legacy production code."""
from __future__ import annotations

import ast
from pathlib import Path


FORBIDDEN_ROOTS = {
    "qiaolian_dual",
    "v2",
    "collector_db_compat",
    "ai_parser",
    "autopilot_publish_bot",
    "meihua_publisher",
    "publication_package",
    "publication_delivery",
    "cover_generator",
    "discussion_map_store",
    "parser_v2_safe",
    "photo_ranker",
    "photo_formatter_v1_1",
    "media_pipeline_v1_1",
    "media_consistency",
    "cover_picker_patch",
    "daily_broadcast_patch",
    "media_selection_patch",
    "review_queue_patch",
    "release_contract_patch",
}

RETIRED_V3_MODULES = {
    "canonical_fact_projection",
    "canonical_listing_materializer",
    "channel_links",
    "channel_post",
    "media_consistency",
    "media_pipeline_v1_1",
    "photo_formatter_v1_1",
}


def _forbidden(module: str) -> bool:
    root = str(module or "").split(".", 1)[0]
    return root in FORBIDDEN_ROOTS


def _retired_relative(module: str | None) -> bool:
    leaf = str(module or "").rsplit(".", 1)[-1]
    return leaf in RETIRED_V3_MODULES


def test_v3_python_modules_do_not_import_legacy_runtime_namespaces():
    failures: list[str] = []
    for path in sorted(Path("v3_core").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if _forbidden(alias.name):
                        failures.append(f"{path}:{node.lineno}: import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                if node.level == 0 and node.module and _forbidden(node.module):
                    failures.append(
                        f"{path}:{node.lineno}: from {node.module} import ..."
                    )
                elif node.level > 0 and _retired_relative(node.module):
                    failures.append(
                        f"{path}:{node.lineno}: retired relative import {node.module}"
                    )

    assert not failures, "legacy runtime imports found:\n" + "\n".join(failures)

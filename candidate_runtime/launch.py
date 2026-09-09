from __future__ import annotations

import importlib
import os
from typing import Callable

from .bootstrap import initialize_runtime_storage, missing_runtime_tables
from .runtime_env import RuntimePaths, configure_candidate_environment


def prepare_runtime(repo_root: str | os.PathLike[str] | None = None) -> RuntimePaths:
    paths = configure_candidate_environment(repo_root)
    missing = missing_runtime_tables(paths.db_path)
    auto_bootstrap = str(os.getenv("CANDIDATE_AUTO_BOOTSTRAP", "0")).strip().lower() in {"1", "true", "yes", "on"}
    if missing and auto_bootstrap:
        initialize_runtime_storage(paths.db_path, backup_dir=paths.backup_dir)
        missing = missing_runtime_tables(paths.db_path)
    if missing:
        raise RuntimeError(
            "candidate V3 database is not initialized; run run_candidate_preflight.py --initialize; missing: "
            + ", ".join(sorted(missing))
        )
    return paths


def _runner(module_name: str, *names: str) -> Callable[[], None]:
    module = importlib.import_module(module_name)
    for name in names:
        value = getattr(module, name, None)
        if callable(value):
            return value
    raise RuntimeError(f"no runnable entrypoint in {module_name}: {', '.join(names)}")


def component_runner(component: str) -> Callable[[], None]:
    normalized = str(component or "").strip().lower()
    if normalized == "collector":
        return _runner("run_v3_collector", "main")
    if normalized == "publisher":
        return _runner("run_v3_publisher_bot", "run", "main")
    if normalized == "user":
        return _runner("run_v3_user_bot", "run_v3_user_bot", "main")
    if normalized in {"worker", "canonical-worker", "canonical_worker"}:
        return _runner("run_v3_canonical_worker", "main", "run")
    raise ValueError(f"unknown candidate component: {component}")


def run_component(component: str, repo_root: str | os.PathLike[str] | None = None) -> None:
    prepare_runtime(repo_root)
    component_runner(component)()


__all__ = ["component_runner", "prepare_runtime", "run_component"]

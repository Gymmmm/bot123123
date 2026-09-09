from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import Callable

from .bootstrap import initialize_runtime_storage, missing_runtime_tables
from .runtime_env import RuntimePaths, configure_candidate_environment


def prepare_runtime(repo_root: str | os.PathLike[str] | None = None) -> RuntimePaths:
    paths = configure_candidate_environment(repo_root)
    auto_bootstrap = str(os.getenv("CANDIDATE_AUTO_BOOTSTRAP", "0")).strip().lower() in {
        "1", "true", "yes", "on"
    }
    missing = missing_runtime_tables(paths.db_path)
    if missing and auto_bootstrap:
        initialize_runtime_storage(paths.db_path)
        missing = missing_runtime_tables(paths.db_path)
    if missing:
        raise RuntimeError(
            "candidate database is not initialized; missing tables: " + ", ".join(sorted(missing))
        )
    return paths


def _collector() -> Callable[[], None]:
    module = importlib.import_module("collector_bot")
    return module.main


def _publisher() -> Callable[[], None]:
    module = importlib.import_module("v2.run_publisher_bot_v2")
    acquire = getattr(module, "_acquire_single_instance_lock")
    main = getattr(module, "main")

    def run() -> None:
        acquire()
        main()

    return run


def _user() -> Callable[[], None]:
    guard = importlib.import_module("qiaolian_dual.runtime_guard")
    module = importlib.import_module("qiaolian_dual.user_bot")

    def run() -> None:
        handle = guard.acquire_user_bot_polling_lock()
        try:
            module.main()
        finally:
            guard.release_user_bot_polling_lock(handle)

    return run


def component_runner(component: str) -> Callable[[], None]:
    normalized = str(component or "").strip().lower()
    if normalized == "collector":
        return _collector()
    if normalized == "publisher":
        return _publisher()
    if normalized == "user":
        return _user()
    raise ValueError(f"unknown candidate component: {component}")


def run_component(component: str, repo_root: str | os.PathLike[str] | None = None) -> None:
    prepare_runtime(repo_root)
    component_runner(component)()


__all__ = ["component_runner", "prepare_runtime", "run_component"]

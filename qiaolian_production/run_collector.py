from __future__ import annotations

import os
import sys
from pathlib import Path

try:
    from .runtime_env import configure_environment, patch_legacy_path_globals
    from .bootstrap_schema import ensure_runtime_schema
except ImportError:  # direct script execution
    from runtime_env import configure_environment, patch_legacy_path_globals
    from bootstrap_schema import ensure_runtime_schema


def _root() -> Path:
    return Path(__file__).resolve().parent


def _activate_paths() -> None:
    root = _root()
    for path in (root / "shared", root / "user", root / "collector"):
        value = str(path)
        if value not in sys.path:
            sys.path.insert(0, value)


def run() -> None:
    _activate_paths()
    app_root = configure_environment()
    ensure_runtime_schema(os.environ["DB_PATH"])
    patch_legacy_path_globals(app_root)
    from collector_bot import main

    main()


if __name__ == "__main__":
    run()

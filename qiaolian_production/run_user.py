from __future__ import annotations

import sys
from pathlib import Path


def _root() -> Path:
    return Path(__file__).resolve().parent


def _activate_paths() -> None:
    root = _root()
    for path in (root / "shared", root / "user"):
        value = str(path)
        if value not in sys.path:
            sys.path.insert(0, value)


def _load_runtime():
    _activate_paths()
    from qiaolian_dual.runtime_guard import (
        acquire_user_bot_polling_lock,
        release_user_bot_polling_lock,
    )
    from qiaolian_dual.user_bot import main
    return main, acquire_user_bot_polling_lock, release_user_bot_polling_lock


def run() -> None:
    main, acquire_lock, release_lock = _load_runtime()
    lock_handle = acquire_lock()
    try:
        main()
    finally:
        release_lock(lock_handle)


if __name__ == "__main__":
    run()

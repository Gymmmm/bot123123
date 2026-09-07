from __future__ import annotations

import sys
from pathlib import Path


def _user_runtime_dir() -> Path:
    return Path(__file__).resolve().parent / "user"


def _load_runtime():
    runtime_dir = str(_user_runtime_dir())
    if runtime_dir not in sys.path:
        sys.path.insert(0, runtime_dir)

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

from __future__ import annotations

import atexit
import fcntl
import os
import sys
from pathlib import Path

try:
    from .runtime_env import configure_environment, patch_legacy_path_globals
except ImportError:  # direct script execution
    from runtime_env import configure_environment, patch_legacy_path_globals


def _root() -> Path:
    return Path(__file__).resolve().parent


def _activate_paths() -> None:
    root = _root()
    for path in (root / "shared", root / "user", root / "collector", root / "publisher"):
        value = str(path)
        if value not in sys.path:
            sys.path.insert(0, value)


def _load_runtime():
    _activate_paths()
    app_root = configure_environment()
    patch_legacy_path_globals(app_root, publisher_runtime=True)

    from qiaolian_publisher_v2.cover_picker_patch import install_cover_picker
    from qiaolian_publisher_v2.daily_broadcast_patch import install_daily_broadcast_patch
    from qiaolian_publisher_v2.media_selection_patch import install_media_selection_patch
    from qiaolian_publisher_v2.review_queue_patch import install_review_queue_patch
    from qiaolian_publisher_v2.release_contract_patch import install_release_contract_patch
    from qiaolian_publisher_v2.bot import main

    return (
        main,
        install_cover_picker,
        install_daily_broadcast_patch,
        install_media_selection_patch,
        install_review_queue_patch,
        install_release_contract_patch,
    )


_LOCK_FH = None


def _acquire_single_instance_lock() -> None:
    global _LOCK_FH
    lock_path = Path("/tmp/qiaolian_publisher_bot_v2.lock")
    fh = lock_path.open("w")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("Publisher Bot 已在运行，拒绝重复启动。")
    fh.write(str(os.getpid()))
    fh.flush()
    _LOCK_FH = fh

    def _release() -> None:
        try:
            if _LOCK_FH:
                fcntl.flock(_LOCK_FH.fileno(), fcntl.LOCK_UN)
                _LOCK_FH.close()
        except Exception:
            pass

    atexit.register(_release)


def run() -> None:
    (
        main,
        install_cover_picker,
        install_daily_broadcast_patch,
        install_media_selection_patch,
        install_review_queue_patch,
        install_release_contract_patch,
    ) = _load_runtime()
    install_cover_picker()
    install_media_selection_patch()
    install_review_queue_patch()
    install_release_contract_patch()
    install_daily_broadcast_patch()
    _acquire_single_instance_lock()
    main()


if __name__ == "__main__":
    run()

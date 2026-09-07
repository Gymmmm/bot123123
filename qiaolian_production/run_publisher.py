from __future__ import annotations

import atexit
import fcntl
import os
from pathlib import Path

from v2.qiaolian_publisher_v2.cover_picker_patch import install_cover_picker
from v2.qiaolian_publisher_v2.daily_broadcast_patch import install_daily_broadcast_patch
from v2.qiaolian_publisher_v2.media_selection_patch import install_media_selection_patch
from v2.qiaolian_publisher_v2.review_queue_patch import install_review_queue_patch
from v2.qiaolian_publisher_v2.release_contract_patch import install_release_contract_patch
from v2.qiaolian_publisher_v2.bot import main


_LOCK_FH = None


def _install_runtime_patches() -> None:
    install_cover_picker()
    install_media_selection_patch()
    install_review_queue_patch()
    install_release_contract_patch()
    install_daily_broadcast_patch()


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
    _install_runtime_patches()
    _acquire_single_instance_lock()
    main()


if __name__ == "__main__":
    run()

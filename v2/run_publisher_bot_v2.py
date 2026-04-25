from __future__ import annotations

import atexit
import fcntl
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from qiaolian_publisher_v2.bot import main

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

if __name__ == "__main__":
    _acquire_single_instance_lock()
    main()

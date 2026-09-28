"""Runtime guards for long-polling bot processes."""
from __future__ import annotations

import os
from pathlib import Path
from typing import IO


DEFAULT_USER_BOT_LOCK = "/tmp/qiaolian-user-bot.lock"


def acquire_user_bot_polling_lock(lock_path: str | None = None) -> IO[str]:
    """Acquire a process-wide advisory lock for the User Bot poller.

    The lock is kernel-managed (flock), so a crashed/stopped process releases it
    automatically. The lock file itself may remain on disk without blocking a
    later healthy start.
    """
    try:
        import fcntl
    except ImportError as exc:  # pragma: no cover - production is Linux
        raise RuntimeError("当前系统不支持 User Bot 单实例锁（缺少 fcntl）") from exc

    path = Path(lock_path or os.getenv("QIAOLIAN_USER_BOT_LOCK_FILE", DEFAULT_USER_BOT_LOCK))
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise RuntimeError(
            f"用户 Bot 已有 polling 实例在运行，拒绝启动第二个实例：{path}"
        ) from exc

    handle.seek(0)
    handle.truncate()
    handle.write(str(os.getpid()))
    handle.flush()
    return handle


def release_user_bot_polling_lock(handle: IO[str]) -> None:
    """Release an acquired polling lock."""
    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()

#!/usr/bin/env python3
"""
一键拉起侨联常驻进程（子进程），Ctrl+C 会尽量一并结束子进程。

默认：只启动「用户 Bot」（承接频道深链、留资、预约等）。
需要 v2 发布 Bot 时显式加 --with-publisher（你当前场景可忽略）。

可选 --with-collector：同时起 Telethon 采集（勿与现网双开）。
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
_children: list[tuple[str, subprocess.Popen]] = []


def _terminate_all() -> None:
    for _name, p in _children:
        if p.poll() is None:
            try:
                p.terminate()
            except ProcessLookupError:
                pass
    deadline = time.time() + 8.0
    for _name, p in _children:
        while p.poll() is None and time.time() < deadline:
            time.sleep(0.1)
    for _name, p in _children:
        if p.poll() is None:
            try:
                p.kill()
            except ProcessLookupError:
                pass


def _on_signal(signum: int, frame) -> None:  # noqa: ARG001
    _terminate_all()
    sys.exit(128 + signum if signum > 0 else 0)


def _spawn(name: str, rel_path: str) -> subprocess.Popen:
    script = BASE_DIR / rel_path
    if not script.is_file():
        raise FileNotFoundError(f"缺少脚本: {script}")
    p = subprocess.Popen(
        [sys.executable, str(script)],
        cwd=str(BASE_DIR),
        env=os.environ.copy(),
    )
    _children.append((name, p))
    print(f"[stack] 已启动 {name} pid={p.pid} → {rel_path}", flush=True)
    return p


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="侨联一键启动：默认仅用户 Bot；可选带发布 Bot / 采集器"
    )
    parser.add_argument(
        "--with-publisher",
        action="store_true",
        help="同时启动 v2 频道发布 Bot（v2/run_publisher_bot_v2.py）",
    )
    parser.add_argument(
        "--with-collector",
        action="store_true",
        help="同时启动 Telethon collector_bot.py（需已配置 sources.json 与会话）",
    )
    parser.add_argument(
        "--publisher-only",
        action="store_true",
        help="仅启动 v2 发布 Bot（不设默认；与「只要用户 Bot」场景分离）",
    )
    args = parser.parse_args()

    if args.publisher_only and args.with_publisher:
        parser.error("--publisher-only 与 --with-publisher 不要同时使用")

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    if args.publisher_only:
        _spawn("发布 Bot v2", "v2/run_publisher_bot_v2.py")
    else:
        _spawn("用户 Bot", "run_user_bot.py")
        if args.with_publisher:
            _spawn("发布 Bot v2", "v2/run_publisher_bot_v2.py")

    if args.with_collector:
        _spawn("采集器", "collector_bot.py")

    try:
        while True:
            time.sleep(0.5)
            for name, p in _children:
                code = p.poll()
                if code is not None:
                    print(
                        f"[stack] {name} 已退出 code={code}，正在结束其余进程…",
                        flush=True,
                    )
                    _terminate_all()
                    sys.exit(code if code is not None else 1)
    except KeyboardInterrupt:
        _terminate_all()


if __name__ == "__main__":
    main()

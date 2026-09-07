from __future__ import annotations

import sys
from pathlib import Path


def _collector_dir() -> Path:
    return Path(__file__).resolve().parent / "collector"


def run() -> None:
    collector_dir = str(_collector_dir())
    if collector_dir not in sys.path:
        sys.path.insert(0, collector_dir)
    from collector_bot import main

    main()


if __name__ == "__main__":
    run()

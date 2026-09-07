from __future__ import annotations

import sys
from pathlib import Path


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
    from collector_bot import main
    main()


if __name__ == "__main__":
    run()

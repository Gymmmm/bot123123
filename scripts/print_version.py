#!/usr/bin/env python3
"""Print project VERSION file (single line). Used by CI / systemd Environment=."""
from __future__ import annotations

from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    p = root / "VERSION"
    print(p.read_text(encoding="utf-8").strip() if p.is_file() else "0.0.0")


if __name__ == "__main__":
    main()

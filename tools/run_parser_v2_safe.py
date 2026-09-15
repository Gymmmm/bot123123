#!/usr/bin/env python3
"""Offline batch runner for V1.1 -> V2 SAFE enrichment.

Reads existing V1.1 JSON outputs and RAW text only. It never calls Telegram,
never downloads media, never writes production DB, and never publishes.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from parser_v2_safe import enrich_v2


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def first_existing(folder: Path, names: tuple[str, ...]) -> Path | None:
    for name in names:
        candidate = folder / name
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def extract_text(raw: Any) -> str:
    if not isinstance(raw, dict):
        return ""
    return str(
        raw.get("original_text")
        or raw.get("text_raw")
        or raw.get("text")
        or raw.get("raw_text")
        or ""
    )


def run(messages: Path, output: Path) -> dict[str, int]:
    stats = {
        "folders_seen": 0,
        "enriched": 0,
        "missing_v1_1": 0,
        "missing_raw": 0,
        "failed": 0,
    }
    if not messages.exists():
        raise FileNotFoundError(f"messages_dir_not_found:{messages}")

    for folder in sorted(messages.iterdir()):
        if not folder.is_dir():
            continue
        stats["folders_seen"] += 1

        v1_file = first_existing(folder, ("parsed_v1_1.json", "parsed_v1_1"))
        raw_file = first_existing(folder, ("raw.json", "data.json"))
        if not v1_file:
            stats["missing_v1_1"] += 1
            continue
        if not raw_file:
            stats["missing_raw"] += 1
            continue

        try:
            v1 = read_json(v1_file)
            raw = read_json(raw_file)
            result = enrich_v2(extract_text(raw), v1)
            write_json(output / folder.name / "parsed_v2_safe.json", result)
            stats["enriched"] += 1
        except Exception:
            stats["failed"] += 1

    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="V1.1 authoritative + V2 SAFE enrichment")
    parser.add_argument("--messages", default="messages", type=Path)
    parser.add_argument("--output", default="washed_v2_safe", type=Path)
    args = parser.parse_args()

    stats = run(args.messages, args.output)
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

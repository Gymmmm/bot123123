#!/usr/bin/env python3
"""Read-only coverage audit for project recognition and reference knowledge."""
from __future__ import annotations

import argparse
import sqlite3
from collections import Counter
from pathlib import Path

from v3_core.inventory.canonical_facts import canonicalize_source
from v3_core.inventory.project_knowledge import (
    project_reference_for_key,
    project_reference_location,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    db = Path(args.db).expanduser().resolve()
    if not db.is_file():
        raise SystemExit(f"db_not_found:{db}")
    uri = f"file:{db.as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        conn.row_factory = sqlite3.Row
        sql = "SELECT id, raw_text FROM source_posts WHERE trim(coalesce(raw_text,''))<>'' ORDER BY id"
        if args.limit > 0:
            sql += f" LIMIT {int(args.limit)}"
        rows = conn.execute(sql).fetchall()

    usage: Counter[str] = Counter()
    recognized = locations = ambiguous = references = 0
    missing_location: list[tuple[int, str, str]] = []
    missing_reference: list[tuple[int, str]] = []

    for row in rows:
        facts = canonicalize_source(row["raw_text"])
        key = str(facts.get("project_key") or "").strip()
        if key:
            recognized += 1
            usage[key] += 1
            if project_reference_for_key(key):
                references += 1
            else:
                missing_reference.append((int(row["id"]), key))
        if facts.get("public_location_display"):
            locations += 1
        else:
            missing_location.append((int(row["id"]), key, " ".join(str(row["raw_text"]).split())[:120]))
        flags = set(facts.get("candidate_flags") or [])
        ambiguous += bool(flags & {"ambiguous_area", "ambiguous_market_location", "ambiguous_project"})

    unique = list(usage)
    unique_with_reference = sum(bool(project_reference_for_key(key)) for key in unique)
    unique_with_location_reference = sum(bool(project_reference_location(key)) for key in unique)

    print(
        "SUMMARY",
        f"posts={len(rows)}",
        f"project_recognized={recognized}",
        f"project_reference={references}",
        f"public_location={locations}",
        f"missing_location={len(rows)-locations}",
        f"ambiguous={ambiguous}",
    )
    print(
        "UNIQUE_PROJECTS",
        f"recognized={len(unique)}",
        f"with_reference={unique_with_reference}",
        f"with_verified_location_reference={unique_with_location_reference}",
    )
    print("PROJECT_USAGE")
    for key, count in usage.most_common():
        reference = project_reference_for_key(key) or {}
        location = project_reference_location(key) or {}
        print(
            count,
            key,
            f"knowledge={reference.get('knowledge_key') or '-'}",
            f"location_ref={location.get('display') or '-'}",
        )
    print("MISSING_REFERENCE")
    for item in missing_reference:
        print(*item)
    print("MISSING_LOCATION")
    for item in missing_location:
        print(*item)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

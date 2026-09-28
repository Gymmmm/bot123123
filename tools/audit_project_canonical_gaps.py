#!/usr/bin/env python3
"""Audit VERIFIED project → Canonical location/type gaps.

Four buckets (single-listed):
  1. recognized project + public_location still empty
  2. recognized project + public_location still equals project name
  3. recognized project + property_type still unknown (when mode=single)
  4. mixed project that still auto-filled a property type

Usage:
  PYTHONPATH=. python tools/audit_project_canonical_gaps.py
"""
from __future__ import annotations

import json
from pathlib import Path

from v3_core.inventory.canonical_facts import canonicalize_source
from v3_core.inventory.listing_taxonomy import PROJECT_IDENTITIES


def _probe_text(project) -> str:
    alias = project.aliases[0] if project.aliases else project.display
    return f"{alias}\n2房\n租金$800/月\n"


def main() -> int:
    empty_location: list[dict] = []
    location_is_project_name: list[dict] = []
    unknown_type_when_single: list[dict] = []
    mixed_but_typed: list[dict] = []
    registry_missing_location: list[dict] = []

    for project in PROJECT_IDENTITIES:
        if project.kind != "project":
            continue
        mode = str(project.property_type_mode or ("single" if project.property_family else "")).lower()
        if not project.default_location_key or not project.default_location_display:
            registry_missing_location.append(
                {
                    "project_key": project.key,
                    "display": project.display,
                    "mode": mode or None,
                    "property_family": project.property_family,
                }
            )

        facts = canonicalize_source(_probe_text(project))
        if facts.get("project_key") != project.key:
            # Alias collision / not recognized from primary alias — skip runtime buckets.
            continue

        loc = str(facts.get("public_location_display") or "").strip()
        project_name = str(facts.get("project_name") or "").strip()
        row = {
            "project_key": project.key,
            "project_name": project_name,
            "public_location_display": loc or None,
            "publication_location_level": facts.get("publication_location_level"),
            "property_type": facts.get("property_type"),
            "property_type_status": facts.get("property_type_status"),
            "mode": mode or None,
        }
        if not loc:
            empty_location.append(row)
        elif loc == project_name:
            location_is_project_name.append(row)

        prop = str(facts.get("property_type") or "").strip()
        status = str(facts.get("property_type_status") or "").strip()
        if mode == "single" and (not prop or prop == "未知"):
            unknown_type_when_single.append(row)
        if mode == "mixed" and prop and prop != "未知" and status == "inferred":
            mixed_but_typed.append(row)

    report = {
        "registry_missing_default_location": registry_missing_location,
        "runtime_public_location_empty": empty_location,
        "runtime_public_location_equals_project_name": location_is_project_name,
        "runtime_unknown_type_when_single": unknown_type_when_single,
        "runtime_mixed_but_inferred_type": mixed_but_typed,
    }
    out = Path("tmp") / "project_canonical_gap_audit.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"wrote {out}")
    for key, rows in report.items():
        print(f"{key}: {len(rows)}")
        for row in rows[:12]:
            print(f"  - {row.get('project_key') or row.get('display')}: {row}")
        if len(rows) > 12:
            print(f"  ... +{len(rows) - 12} more")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

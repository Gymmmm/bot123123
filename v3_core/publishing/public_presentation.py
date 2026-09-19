"""Public listing presentation projection for Publisher surfaces.

Canonical facts remain authoritative. Project Registry owns identity/GEO and
Chinese Naming owns public project naming. This module only projects those
facts for public display; it never upgrades ambiguous/family aliases.
"""
from __future__ import annotations

import json
from typing import Any

from v3_core.inventory.project_registry import canonical_location_projection, project_by_key, resolve_project


def _clean(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def public_listing_presentation(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a copy with one Registry/Naming-backed public presentation.

    Direct concrete project resolution may enrich display fields. Ambiguous,
    family, deprecated and search-only aliases are preserved as non-authoritative
    input and never promoted to a concrete project.
    """
    out = dict(payload or {})
    candidates = [
        _clean(out.get("project_entity_id")),
        _clean(out.get("project")),
        _clean(out.get("project_name")),
        _clean(out.get("title")),
    ]
    resolution = None
    project = None
    for value in candidates:
        if not value:
            continue
        direct = project_by_key(value)
        if direct is not None:
            project = direct
            break
        result = resolve_project(value)
        if result.project_entity_id and not result.ambiguity:
            resolution = result
            project = project_by_key(result.project_entity_id)
            if project is not None:
                break
    if project is None:
        out["project_resolution_ambiguity"] = bool(
            any(resolve_project(v).ambiguity for v in candidates if v and not v.startswith("project:"))
        )
        return out

    location = canonical_location_projection(project)
    out["project_entity_id"] = project.entity_id
    out["preferred_project_name_cn"] = _clean(project.name)
    out["canonical_project_name_en"] = _clean(project.name_en)
    out["project"] = _clean(project.name) or _clean(project.name_en)
    out["project_name"] = out["project"]
    out["project_name_en"] = _clean(project.name_en)
    out["project_alias"] = _clean(project.name_en)
    out["canonical_geo"] = _clean(location.get("canonical_geo"))
    out["canonical_road"] = _clean(location.get("road"))
    public_location = _clean(location.get("public_location_display"))
    if public_location:
        out["public_location_display"] = public_location
        out["area"] = public_location
    out["project_resolution_ambiguity"] = False
    out["project_resolution_action"] = getattr(resolution, "resolution_action", "") if resolution else "CANONICAL_IDENTITY"
    out["project_residential_eligible"] = bool(project.residential_eligible)
    return out


def public_listing_presentation_from_draft(payload: dict[str, Any]) -> dict[str, Any]:
    """Project a draft after canonical facts have already been overlaid."""
    out = dict(payload or {})
    raw = out.get("normalized_data")
    if raw:
        try:
            facts = json.loads(raw) if isinstance(raw, str) else dict(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            facts = {}
        for key in ("project_entity_id", "project_name", "project", "public_location_display", "canonical_geo"):
            if not out.get(key) and facts.get(key):
                out[key] = facts.get(key)
    return public_listing_presentation(out)


__all__ = ["public_listing_presentation", "public_listing_presentation_from_draft"]

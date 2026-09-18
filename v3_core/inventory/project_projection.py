"""Safe canonical/public projection from Project Registry V3."""
from .project_registry import project_by_key, canonical_location_projection
def apply_project_projection(facts:dict)->dict:
    out=dict(facts or {}); p=project_by_key(out.get("project_key"))
    if not p or not p.location_authoritative:return out
    proj=canonical_location_projection(p)
    if not out.get("canonical_area_key") and proj["canonical_geo"]:
        out["canonical_area_key"]=proj["canonical_geo"]; out["canonical_area_display"]=proj["canonical_geo"]
        out["area_status"]="confirmed"; out["canonical_area_level"]="project_registry_verified"
    if proj["public_location_display"]:
        out["public_location_key"]=out.get("canonical_area_key") or p.key
        out["public_location_display"]=proj["public_location_display"]
        out["publication_location_level"]="level_2_registry_verified"
    return out

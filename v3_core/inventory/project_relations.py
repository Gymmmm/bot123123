"""Evidence-preserving Project Registry V3 location relations."""
from __future__ import annotations
from dataclasses import dataclass
from .project_registry import _rows
ALLOWED_RELATIONS={"ADJACENT","WALKABLE","NEARBY_DRIVE","MARKET_POSITIONING"}
@dataclass(frozen=True)
class ProjectRelation:
    project_entity_id:str; target_entity_id:str; target_name_cn:str; relation_type:str
    distance_m:float|None; distance_km:float|None; walk_minutes:float|None; drive_minutes:float|None
    source:str; confidence:str; notes:str
def _number(v):
    try:return float(v) if str(v).strip() else None
    except (TypeError,ValueError):return None
def _make(r):
    if r["relation_type"] not in ALLOWED_RELATIONS:raise RuntimeError("invalid relation")
    return ProjectRelation(r["project_entity_id"],r["target_entity_id"],r["target_name_cn"],r["relation_type"],
      _number(r["distance_m"]),_number(r["distance_km"]),_number(r["walk_minutes"]),_number(r["drive_minutes"]),
      r["source"],r["confidence"],r["notes"])
RELATIONS=tuple(_make(r) for r in _rows("pp_project_location_relations_v3.csv"))
if len(RELATIONS)!=39:raise RuntimeError(f"relation count drift: {len(RELATIONS)}")
def relations_for(project_entity_id:str,relation_type:str=""):
    return tuple(r for r in RELATIONS if r.project_entity_id==project_entity_id and (not relation_type or r.relation_type==relation_type))
__all__=["ALLOWED_RELATIONS","ProjectRelation","RELATIONS","relations_for"]

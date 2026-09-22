"""Project living facts with evidence scope and conflict gates."""
from __future__ import annotations
from dataclasses import dataclass
from .project_registry import _rows
SCOPES={"PROJECT_OFFICIAL","BUILDING_MANAGEMENT","COMMON_MARKET_PRACTICE","OWNER_SPECIFIC","UNKNOWN","MARKET_RANGE"}
@dataclass(frozen=True)
class LivingEvidence:
    project_entity_id:str; fact_key:str; fact_value:str; fact_unit:str; source_scope:str
    confidence:str; source_name:str; source_url:str; source_date:str; evidence_text:str; notes:str
FACT_ROWS=_rows("pp_project_living_facts_v2.csv")
EVIDENCE_ROWS=_rows("pp_project_living_evidence_v2.csv")
CONFLICT_ROWS=_rows("pp_project_living_conflicts_v2.csv")
FACTS_BY_PROJECT={r["project_entity_id"]:dict(r) for r in FACT_ROWS}
EVIDENCE=tuple(LivingEvidence(r["project_entity_id"],r["fact_key"],r["fact_value"],r["fact_unit"],
    r["source_scope"] if r["source_scope"] in SCOPES else "UNKNOWN",r["confidence"],r["source_name"],
    r["source_url"],r["source_date"],r["evidence_text"],r["notes"]) for r in EVIDENCE_ROWS)
CONFLICT_KEYS={(r["project_entity_id"],r["fact_key"]) for r in CONFLICT_ROWS}
def project_living_facts(project_entity_id:str)->dict[str,str]:
    return dict(FACTS_BY_PROJECT.get(project_entity_id,{}))
def evidence_for(project_entity_id:str,fact_key:str=""):
    return tuple(e for e in EVIDENCE if e.project_entity_id==project_entity_id and (not fact_key or e.fact_key==fact_key))
def authoritative_evidence(project_entity_id:str,fact_key:str=""):
    return tuple(e for e in evidence_for(project_entity_id,fact_key) if e.source_scope in {"PROJECT_OFFICIAL","BUILDING_MANAGEMENT"})
def has_conflict(project_entity_id:str,fact_key:str)->bool:
    return (project_entity_id,fact_key) in CONFLICT_KEYS
def safe_project_value(project_entity_id:str,fact_key:str):
    if has_conflict(project_entity_id,fact_key):return None
    ev=authoritative_evidence(project_entity_id,fact_key)
    values=tuple(dict.fromkeys(e.fact_value for e in ev if e.fact_value))
    return values[0] if len(values)==1 else None
__all__=["SCOPES","FACT_ROWS","EVIDENCE","CONFLICT_ROWS","project_living_facts","evidence_for","authoritative_evidence","has_conflict","safe_project_value"]

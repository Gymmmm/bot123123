"""Project-aware search parsing facade for Chinese Project Naming Layer V1."""
from dataclasses import dataclass
from v3_core.inventory.project_registry import resolve_project,project_search_terms,project_by_key
from v3_core.inventory.chinese_project_naming import resolve_chinese_project
@dataclass(frozen=True)
class ProjectSearchResolution:
    project_terms:tuple[str,...]=();ambiguity:bool=False;family_id:str="";candidates:tuple[str,...]=();canonical_identity:str="";project_key:str="";preferred_cn_name:str="";canonical_name_en:str="";entity_level:str="";occupancy_type:str="";canonical_geo:str="";canonical_road:str="";chinese_market_cluster:str="";public_location_display_cn:str="";matched_alias:str="";normalized_alias:str="";alias_kind:str="";resolution_action:str="";match_weight:int=0;conflict_flag:bool=False
def resolve_project_search(text,*,residential_only=True):
    r=resolve_project(text);c=resolve_chinese_project(text);terms=project_search_terms(text,residential_only=residential_only);p=project_by_key(r.project_entity_id) if r.project_entity_id else None
    return ProjectSearchResolution(terms,r.ambiguity,r.family_id,r.candidates,r.project_entity_id,r.project_key,p.name if p else c.preferred_cn_name,p.name_en if p else c.canonical_name_en,c.entity_level,p.occupancy_type if p else c.occupancy_type,p.canonical_geo_display if p else c.canonical_geo,p.primary_road if p else c.canonical_road,c.chinese_market_cluster,p.public_location_display_cn if p else c.public_location_display_cn,c.matched_alias or r.match_alias,c.normalized_alias,c.alias_kind or r.match_type,c.resolution_action or r.resolution_action,c.match_weight or r.confidence,c.conflict_flag or r.conflict_flag)

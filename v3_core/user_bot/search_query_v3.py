"""Project-aware search parsing facade."""
from dataclasses import dataclass
from v3_core.inventory.project_registry import resolve_project,project_search_terms
@dataclass(frozen=True)
class ProjectSearchResolution:
    project_terms:tuple[str,...]=(); ambiguity:bool=False; family_id:str=""; candidates:tuple[str,...]=()
def resolve_project_search(text:str)->ProjectSearchResolution:
    r=resolve_project(text)
    return ProjectSearchResolution(project_search_terms(text),r.ambiguity,r.family_id,r.candidates)

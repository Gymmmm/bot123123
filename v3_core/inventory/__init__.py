"""V3 inventory domain. Project identity loads directly from Registry V3; no import-order monkey patch."""
from .project_registry import RESEARCH_PROJECTS,FAMILIES,resolve_project,project_by_key,project_search_terms,canonical_location_projection,public_location_for_project
__all__=["RESEARCH_PROJECTS","FAMILIES","resolve_project","project_by_key","project_search_terms","canonical_location_projection","public_location_for_project"]

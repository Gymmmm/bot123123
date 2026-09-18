"""Location Registry facade. GEO/GEO_ROAD/LANDMARK remain separate from PROJECT."""
from .project_registry import canonical_location_projection, public_location_for_project
from .project_relations import RELATIONS, relations_for
__all__=["RELATIONS","relations_for","canonical_location_projection","public_location_for_project"]

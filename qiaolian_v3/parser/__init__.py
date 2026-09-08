"""V3 canonical parser extracted from locked V2.2 behavior."""

from .canonical import PARSER_REVISION, SCHEMA_VERSION, canonicalize_source
from .service import CanonicalParserService

__all__ = ["SCHEMA_VERSION", "PARSER_REVISION", "canonicalize_source", "CanonicalParserService"]

"""V3 canonical parser extracted from locked V2.2 behavior."""

from .canonical import canonicalize_source
from .service import CanonicalParserService

__all__ = ["canonicalize_source", "CanonicalParserService"]

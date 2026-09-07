"""Pure-production runtime package extracted from production SHA 8e4605cf.

This package is built incrementally on the isolated refactor branch.  The
entrypoints intentionally preserve the exact production startup behaviour while
implementation modules are moved behind the package in later commits.
"""

PRODUCTION_BASE_SHA = "8e4605cf5cc21dfec3ce30729654b09e39de9abf"

__all__ = ["PRODUCTION_BASE_SHA"]

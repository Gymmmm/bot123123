"""Public resident-service entry point for the V3 User Bot."""
from __future__ import annotations

from .service_views import ServiceView, service_home_view as _service_home_view


def service_home_view() -> ServiceView:
    return _service_home_view()


__all__ = ["service_home_view"]

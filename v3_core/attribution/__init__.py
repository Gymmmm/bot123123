"""V3 explicit attribution rules and services."""

from .classification import (
    attach_to_lead_payload,
    classify_bot_event,
    classify_public_start_arg,
    merge_touch,
)

__all__ = [
    "attach_to_lead_payload",
    "classify_bot_event",
    "classify_public_start_arg",
    "merge_touch",
]

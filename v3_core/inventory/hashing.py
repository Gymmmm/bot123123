"""Canonical fact hashing primitive."""
from __future__ import annotations

import hashlib
import json
from typing import Any


def facts_hash(facts: dict[str, Any]) -> str:
    payload = {
        key: value
        for key, value in dict(facts or {}).items()
        if key != "canonical_facts_hash"
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


__all__ = ["facts_hash"]

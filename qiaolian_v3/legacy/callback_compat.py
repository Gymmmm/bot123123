from __future__ import annotations

from dataclasses import dataclass

from .deeplink_compat import resolve_legacy_deeplink


@dataclass(frozen=True)
class LegacyCallback:
    action: str
    legacy_reference: str
    normalized_legacy_key: str


def resolve_legacy_callback(payload: str) -> LegacyCallback | None:
    raw = str(payload or '').strip()
    if not raw:
        return None
    for sep in (':', '|', '_'):
        if sep in raw:
            action, ref = raw.split(sep, 1)
            resolved = resolve_legacy_deeplink(action, ref)
            if resolved is not None:
                return LegacyCallback(
                    action=resolved.action,
                    legacy_reference=resolved.legacy_reference,
                    normalized_legacy_key=resolved.normalized_legacy_key,
                )
    return None


__all__ = ['LegacyCallback', 'resolve_legacy_callback']

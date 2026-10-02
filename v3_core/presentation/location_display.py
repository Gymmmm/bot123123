"""Pure presentation-only normalization for user-visible location labels."""
from __future__ import annotations
import re

_SPLIT_RE = re.compile(r"\s*(?:/|／|｜|\||、|;|；)\s*")

def _clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())

def _project_overlap(project: str, value: str) -> bool:
    p = re.sub(r"\s+", "", project).lower()
    v = re.sub(r"\s+", "", value).lower()
    return bool(p and v and (p in v or v in p))

def _public_form(value: str) -> str:
    text = _clean(value)
    if re.fullmatch(r"\d{2,3}米", text):
        return f"{text}大道"
    return text

def _score(value: str) -> tuple[int, int]:
    text = _clean(value)
    if re.fullmatch(r"\d{2,3}米", text):
        return (100, -len(text))
    if re.search(r"\bBKK\s*\d+\b", text, re.I):
        return (95, -len(text))
    if any(token in text for token in ("大道", "岛", "区", "城")):
        return (90, -len(text))
    if any(token in text for token in ("路", "街")):
        return (80, -len(text))
    return (60, -len(text))

def display_location(value: object, *, project: object = "") -> str:
    """Collapse stored alias text to one public label without changing facts."""
    raw = _clean(value)
    project_text = _clean(project)
    if not raw:
        return ""
    parts: list[str] = []
    for item in _SPLIT_RE.split(raw):
        item = _clean(item)
        if not item or item in parts:
            continue
        if project_text and _project_overlap(project_text, item):
            continue
        parts.append(item)
    if not parts:
        return ""
    explicit_near = next((item for item in parts if item.startswith(("近", "附近", "邻近"))), "")
    primary_candidates = [item for item in parts if item != explicit_near]
    if not primary_candidates:
        return _public_form(explicit_near)
    primary = max(primary_candidates, key=_score)
    result = _public_form(primary)
    if explicit_near and explicit_near != primary:
        result = f"{result}｜{_public_form(explicit_near)}"
    return result

def display_project_location(*, project: object = "", location: object = "") -> str:
    project_text = _clean(project)
    location_text = display_location(location, project=project_text)
    if project_text and location_text:
        return f"{project_text}｜{location_text}"
    return project_text or location_text

__all__ = ["display_location", "display_project_location"]

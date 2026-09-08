from __future__ import annotations

import html
from typing import Iterable


def e(value: object | None) -> str:
    if value is None:
        return ""
    return html.escape(str(value), quote=False)


def compact_join(items: Iterable[str], sep: str = " / ") -> str:
    return sep.join(str(item).strip() for item in items if str(item).strip())

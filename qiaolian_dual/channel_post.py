"""Final public channel listing caption contract.

The channel post is discovery-only: compact fact/status lines plus factual
hashtags. Only the title and rent are bold. Detailed costs, availability
notes that are not a concrete date, highlights and internal identifiers
belong in the detail flow, not the feed.
"""
from __future__ import annotations

import html
import re
from typing import Any, Iterable

from .channel_links import public_qc_code
from .utils_formatting import _display_floor, _display_layout

_STATUS_LABELS = {
    "active": "\U0001F7E2 \u5f53\u524d\u53ef\u9884\u7ea6",
    "reserved": "\U0001F7E1 \u5df2\u6709\u9884\u7ea6 \u00b7 \u4ecd\u53ef\u9884\u7ea6",
    "pending": "\U0001F535 \u623f\u6001\u5f85\u786e\u8ba4",
    "rented": "\U0001F534 \u5df2\u79df\u51fa",
    "inactive": "\u26ab \u5df2\u4e0b\u67b6",
    "offline": "\u26ab \u5df2\u4e0b\u67b6",
}

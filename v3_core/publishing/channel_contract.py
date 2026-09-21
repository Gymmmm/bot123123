"""Database-free public channel action contract.

The caller supplies the already-assigned public QL listing id. This keeps URL
rendering deterministic and prevents channel rendering from reaching into
legacy listing/draft tables merely to discover an identifier.
"""
from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse

from v3_core.status_labels import inventory_status_bookable
from v3_core.user_bot.telegram_navigation import advisor_handoff_url

from .public_ids import normalize_public_id

CHANNEL_ACTION_ORDER = ("details", "photos", "book", "consult")
CHANNEL_CTA_LABELS = {
    "details": "📷 房源详情",
    "photos": "📸 更多实拍",  # kept for package/compat; not shown on channel keyboard
    "book": "📅 预约看房",
    "consult": "💬 中文顾问",
    "find": "🏠 帮我找房",
    "more": "🔎 看相近房源",
    "similar": "🔎 看相近房源",
}

_ACTION_SUFFIX = {
    "detail": "details",
    "details": "details",
    "photos": "photos",
    "book": "book",
    "contact": "contact",
}
_START_PAYLOAD_RE_PREFIX = "property_"
_CHANNEL_SOURCE_CODE = "ch"

_CAPTION_PUBLIC_ID_RE = re.compile(r"(?<![A-Z0-9-])(QL-[A-Z0-9]+(?:-[A-Z0-9]+)+)(?![A-Z0-9-])")


_DEBUG_TRAILING_TEXT_RE = re.compile(
    r"(?<=[\u4e00-\u9fff。！？.!?])(?:a['’]?a|a|c)+$",
    re.IGNORECASE,
)


def sanitize_publisher_text(value: object) -> str:
    """Strip known accidental trailing debug tokens from publisher text."""
    text = str(value or "").rstrip()
    if text.lower() in {"a", "c", "a'a", "a’a"}:
        return ""
    return _DEBUG_TRAILING_TEXT_RE.sub("", text).rstrip()


def channel_caption_public_id(caption: object) -> str:
    ids = {normalize_public_id(value) for value in _CAPTION_PUBLIC_ID_RE.findall(str(caption or ""))}
    ids.discard("")
    if len(ids) != 1:
        raise ValueError("channel_caption_public_listing_id_missing_or_ambiguous")
    return next(iter(ids))


def assert_channel_identity(*, caption: object, actions: dict[str, str], public_listing_id: object) -> str:
    expected = normalize_public_id(public_listing_id)
    if not expected:
        raise ValueError("channel_public_listing_id_missing")
    caption_id = channel_caption_public_id(caption)
    if caption_id != expected:
        raise ValueError(f"channel_caption_public_listing_id_mismatch:{caption_id}:{expected}")
    verified = official_channel_action_identity(actions)
    action_ids = set()
    for action, url in verified.items():
        action_ids.add(_consult_public_id_from_url(url) if action == "consult" else _public_id_from_action_url(url, action))
    if action_ids != {expected}:
        raise ValueError(f"channel_action_public_listing_id_mismatch:{sorted(action_ids)}:{expected}")
    return expected



def channel_start_payload(
    public_listing_id: object,
    action: str,
    *,
    source_code: str = "",
) -> str:
    public_id = normalize_public_id(public_listing_id)
    if not public_id:
        raise ValueError(f"invalid_public_listing_id:{public_listing_id}")
    suffix = _ACTION_SUFFIX.get(str(action or "").strip().lower())
    if not suffix:
        raise ValueError(f"unsupported_channel_action:{action}")
    payload = f"property_{public_id}_{suffix}"
    source = str(source_code or "").strip().lower()
    if source:
        if not source.isalnum() or len(source) > 8:
            raise ValueError(f"invalid_channel_source_code:{source_code}")
        payload = f"{payload}__{source}"
    return payload


def channel_action_url(
    username: str,
    public_listing_id: object,
    action: str,
    *,
    source_code: str = "",
) -> str:
    user = str(username or "").strip().lstrip("@")
    if not user:
        return ""
    payload = channel_start_payload(
        public_listing_id,
        action,
        source_code=source_code,
    )
    return f"https://t.me/{user}?start={payload}"


def channel_actions(public_listing_id: object) -> tuple[str, str, str]:
    """Return the three User Bot deep-link actions in stable order."""
    return tuple(
        channel_start_payload(public_listing_id, action, source_code=_CHANNEL_SOURCE_CODE)
        for action in ("details", "photos", "book")
    )


def official_channel_action_urls(
    username: str,
    public_listing_id: object,
    *,
    advisor_url: str = "",
    listing_summary: object = "",
) -> dict[str, str]:
    """Build the frozen listing-action URL map or raise."""
    user = str(username or "").strip().lstrip("@")
    if not user:
        raise ValueError("channel_username_missing")
    urls = {
        action: channel_action_url(
            user,
            public_listing_id,
            action,
            source_code=_CHANNEL_SOURCE_CODE,
        )
        for action in ("details", "photos", "book")
    }
    urls["consult"] = channel_action_url(
        user,
        public_listing_id,
        "contact",
        source_code=_CHANNEL_SOURCE_CODE,
    )
    if not urls["consult"]:
        raise ValueError("advisor_url_missing")
    return official_channel_action_identity(urls)


def official_channel_cta_keys(inventory_status: object = "active") -> tuple[str, ...]:
    status = str(inventory_status or "").strip().lower()
    if inventory_status_bookable(status):
        return ("details", "book", "consult")
    if status == "pending":
        return ("more", "consult")
    # rented / offline / inactive
    return ("find", "more", "consult")


def _bot_from_details_url(details_url: str) -> str:
    details = urlparse(details_url)
    return details.path.strip("/")


def official_channel_button_spec(
    actions: dict[str, str],
    *,
    inventory_status: object = "active",
) -> tuple[tuple[tuple[str, str], ...], ...]:
    """Return keyboard rows as ``((label, url), ...)`` for publish and sync.

    Locked product matrix (2026-09-20):
    - bookable active/reserved:
        Row1: [📷 房源详情] [📅 预约看房]
        Row2: [💬 中文顾问]
    - pending:
        Row1: [🔎 看相近房源] [💬 中文顾问]
    - rented / offline / inactive:
        Row1: [🏠 帮我找房] [🔎 更多房源]
        Row2: [💬 中文顾问]
    """
    verified = official_channel_action_identity(actions)
    status = str(inventory_status or "").strip().lower()
    has_consult = "consult" in verified

    if inventory_status_bookable(status):
        rows: list[tuple[tuple[str, str], ...]] = [
            (
                (CHANNEL_CTA_LABELS["details"], verified["details"]),
                (CHANNEL_CTA_LABELS["book"], verified["book"]),
            ),
        ]
        if has_consult:
            rows.append(((CHANNEL_CTA_LABELS["consult"], verified["consult"]),))
        return tuple(rows)

    bot = _bot_from_details_url(verified["details"])
    find_url = f"https://t.me/{bot}?start=find"
    more_url = f"https://t.me/{bot}?start=find"
    find_btn = (CHANNEL_CTA_LABELS["find"], find_url)
    more_btn = (CHANNEL_CTA_LABELS["more"], more_url)
    consult_btn = (
        (CHANNEL_CTA_LABELS["consult"], verified["consult"])
        if has_consult
        else None
    )

    if status in {"pending", "high_demand"}:
        similar_btn = (CHANNEL_CTA_LABELS["similar"], more_url)
        row = [similar_btn]
        if consult_btn is not None:
            row.append(consult_btn)
        return (tuple(row),)

    if status in {"rented", "offline", "inactive", "withdrawn"}:
        terminal_more_btn = ("🔎 更多房源", more_url)
        rows = [(find_btn, terminal_more_btn)]
        if consult_btn is not None:
            rows.append((consult_btn,))
        return tuple(rows)

    row = [more_btn]
    if consult_btn is not None:
        row.append(consult_btn)
    return (tuple(row),)


def official_channel_action_identity(actions: dict[str, str]) -> dict[str, str]:
    """Require details/photos/book URLs that share one public listing id."""
    if not isinstance(actions, dict):
        raise ValueError("telegram_actions_invalid")
    legacy_order = ("details", "photos", "book")
    keys = set(actions)
    if keys == set(CHANNEL_ACTION_ORDER):
        order = CHANNEL_ACTION_ORDER
    elif keys == set(legacy_order):
        order = legacy_order
    else:
        raise ValueError("telegram_actions_must_be_details_photos_book_or_consult")
    ordered = {key: str(actions.get(key) or "").strip() for key in order}
    if any(not ordered[key] for key in order):
        raise ValueError("telegram_action_url_missing")
    public_ids: list[str] = []
    for action, url in ordered.items():
        public_id = _consult_public_id_from_url(url) if action == "consult" else _public_id_from_action_url(url, action)
        public_ids.append(public_id)
    if len(set(public_ids)) != 1:
        raise ValueError("telegram_action_public_listing_id_mismatch")
    return ordered


def _consult_public_id_from_url(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in {"http", "https"} or parsed.netloc.lower() not in {"t.me", "www.t.me"}:
        raise ValueError("telegram_action_url_invalid")
    payload = (parse_qs(parsed.query).get("start") or [""])[0]
    if payload:
        core_payload = payload.split("__", 1)[0]
        suffix = "_contact"
        if core_payload.startswith(_START_PAYLOAD_RE_PREFIX) and core_payload.lower().endswith(suffix):
            raw_id = core_payload[len(_START_PAYLOAD_RE_PREFIX) : -len(suffix)]
            public_id = normalize_public_id(raw_id)
            if public_id:
                return public_id
    message = (parse_qs(parsed.query).get("text") or [""])[0]
    for word in message.replace("：", " ").replace(":", " ").split():
        public_id = normalize_public_id(word.strip("，。,."))
        if public_id:
            return public_id
    raise ValueError(f"invalid_public_listing_id:{payload or message}")


def _public_id_from_action_url(url: str, expected_action: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in {"http", "https"} or parsed.netloc.lower() != "t.me":
        raise ValueError("telegram_action_url_invalid")
    if not str(parsed.path or "").strip("/"):
        raise ValueError("telegram_action_url_invalid")
    payload = (parse_qs(parsed.query).get("start") or [""])[0]
    if not payload.startswith(_START_PAYLOAD_RE_PREFIX):
        raise ValueError(f"invalid_public_listing_id:{payload}")
    core_payload = payload.split("__", 1)[0]
    suffix = f"_{expected_action}"
    if not core_payload.lower().endswith(suffix):
        raise ValueError(f"unsupported_channel_action:{payload}")
    raw_id = core_payload[len(_START_PAYLOAD_RE_PREFIX) : -len(suffix)]
    public_id = normalize_public_id(raw_id)
    if not public_id:
        raise ValueError(f"invalid_public_listing_id:{raw_id}")
    return public_id


__all__ = [
    "CHANNEL_ACTION_ORDER",
    "CHANNEL_CTA_LABELS",
    "channel_action_url",
    "channel_actions",
    "channel_start_payload",
    "assert_channel_identity",
    "channel_caption_public_id",
    "official_channel_action_identity",
    "official_channel_action_urls",
    "official_channel_button_spec",
    "official_channel_cta_keys",
    "sanitize_publisher_text",
]

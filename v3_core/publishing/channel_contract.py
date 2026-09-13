"""Database-free public channel action contract.

The caller supplies the already-assigned public QL listing id. This keeps URL
rendering deterministic and prevents channel rendering from reaching into
legacy listing/draft tables merely to discover an identifier.
"""
from __future__ import annotations

from urllib.parse import parse_qs, urlparse

from v3_core.status_labels import inventory_status_bookable

from .public_ids import normalize_public_id

CHANNEL_ACTION_ORDER = ("details", "photos", "book")
CHANNEL_CTA_LABELS = {
    "details": "📋 租赁详情",
    "photos": "📸 更多实拍",
    "book": "📅 预约看房",
}

_ACTION_SUFFIX = {
    "detail": "details",
    "details": "details",
    "photos": "photos",
    "book": "book",
}
_START_PAYLOAD_RE_PREFIX = "property_"
_CHANNEL_SOURCE_CODE = "ch"


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
    """Return the only three public listing actions in stable order."""
    return tuple(
        channel_start_payload(public_listing_id, action, source_code=_CHANNEL_SOURCE_CODE)
        for action in CHANNEL_ACTION_ORDER
    )


def official_channel_action_urls(
    username: str, public_listing_id: object
) -> dict[str, str]:
    """Build the frozen three-action URL map or raise."""
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
        for action in CHANNEL_ACTION_ORDER
    }
    return official_channel_action_identity(urls)


def official_channel_cta_keys(inventory_status: object = "active") -> tuple[str, ...]:
    keys = ("details", "photos")
    if inventory_status_bookable(inventory_status):
        keys += ("book",)
    return keys


def official_channel_button_spec(
    actions: dict[str, str],
    *,
    inventory_status: object = "active",
) -> tuple[tuple[tuple[str, str], ...], ...]:
    """Return keyboard rows as ``((label, url), ...)`` for publish and sync."""
    verified = official_channel_action_identity(actions)
    rows: list[tuple[tuple[str, str], ...]] = [
        (
            (CHANNEL_CTA_LABELS["details"], verified["details"]),
            (CHANNEL_CTA_LABELS["photos"], verified["photos"]),
        )
    ]
    if "book" in official_channel_cta_keys(inventory_status):
        rows.append(((CHANNEL_CTA_LABELS["book"], verified["book"]),))
    return tuple(rows)


def official_channel_action_identity(actions: dict[str, str]) -> dict[str, str]:
    """Require details/photos/book URLs that share one public listing id."""
    if not isinstance(actions, dict) or set(actions) != set(CHANNEL_ACTION_ORDER):
        raise ValueError("telegram_actions_must_be_details_photos_book")
    ordered = {key: str(actions.get(key) or "").strip() for key in CHANNEL_ACTION_ORDER}
    if any(not ordered[key] for key in CHANNEL_ACTION_ORDER):
        raise ValueError("telegram_action_url_missing")
    public_ids: list[str] = []
    for action, url in ordered.items():
        public_id = _public_id_from_action_url(url, action)
        public_ids.append(public_id)
    if len(set(public_ids)) != 1:
        raise ValueError("telegram_action_public_listing_id_mismatch")
    return ordered


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
    "official_channel_action_identity",
    "official_channel_action_urls",
    "official_channel_button_spec",
    "official_channel_cta_keys",
]

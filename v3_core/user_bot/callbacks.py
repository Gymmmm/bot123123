"""Explicit callback contract for the side-by-side V3 User Bot.

Fixed-SHA production routes nearly every callback through a catch-all dispatcher
and search navigation carries an internal listing id. V3 narrows the supported
surface and uses only public listing ids at the Telegram boundary.

This module is pure: it does not import Telegram, touch session state or query a
database. Handlers may use ``parse_callback`` and then validate card navigation
against the public-id sequence saved for the active search session.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from v3_core.publishing.public_ids import normalize_public_id

from .listing_responses import SemanticAction


ListingCallbackAction = Literal["details", "photos", "book", "consult", "similar"]
CallbackKind = Literal["listing", "card", "change_search"]

PREFIX = "v3u"
_LISTING_ACTIONS = frozenset({"details", "photos", "book", "consult", "similar"})


@dataclass(frozen=True)
class UserBotCallback:
    kind: CallbackKind
    action: str = ""
    public_listing_id: str = ""
    target_index: int | None = None


def _public_id(value: object) -> str:
    public_id = normalize_public_id(value)
    if public_id is None:
        raise ValueError("invalid_public_listing_id")
    return public_id


def encode_listing_callback(action: str, public_listing_id: object) -> str:
    clean_action = str(action or "").strip().lower()
    if clean_action not in _LISTING_ACTIONS:
        raise ValueError("unsupported_listing_callback_action")
    return f"{PREFIX}:listing:{clean_action}:{_public_id(public_listing_id)}"


def encode_card_callback(target_index: int, public_listing_id: object) -> str:
    index = int(target_index)
    if index < 0:
        raise ValueError("invalid_search_card_index")
    return f"{PREFIX}:card:{index}:{_public_id(public_listing_id)}"


def encode_change_search_callback() -> str:
    return f"{PREFIX}:change_search"


def encode_semantic_action(action: SemanticAction) -> str:
    clean = str(action.action or "").strip().lower()
    if clean in {"previous", "next"}:
        if action.target_index is None:
            raise ValueError("search_card_action_missing_index")
        return encode_card_callback(action.target_index, action.target_public_listing_id)
    if clean == "change_search":
        return encode_change_search_callback()
    if clean in _LISTING_ACTIONS:
        return encode_listing_callback(clean, action.target_public_listing_id)
    raise ValueError("unsupported_semantic_action")


def parse_callback(value: object) -> UserBotCallback | None:
    raw = str(value or "").strip()
    if raw == f"{PREFIX}:change_search":
        return UserBotCallback(kind="change_search")

    parts = raw.split(":")
    if len(parts) == 4 and parts[:2] == [PREFIX, "listing"]:
        action = parts[2].strip().lower()
        public_id = normalize_public_id(parts[3])
        if action not in _LISTING_ACTIONS or public_id is None:
            return None
        return UserBotCallback(
            kind="listing",
            action=action,
            public_listing_id=public_id,
        )

    if len(parts) == 4 and parts[:2] == [PREFIX, "card"]:
        try:
            index = int(parts[2])
        except (TypeError, ValueError):
            return None
        public_id = normalize_public_id(parts[3])
        if index < 0 or public_id is None:
            return None
        return UserBotCallback(
            kind="card",
            action="show_card",
            public_listing_id=public_id,
            target_index=index,
        )

    return None


def validate_card_navigation(
    callback: UserBotCallback,
    session_public_listing_ids: tuple[str, ...] | list[str],
) -> bool:
    """Reject stale/tampered card callbacks like fixed-SHA ``findcard`` did.

    Session positions are authoritative for the callback check. Invalid stored
    identities therefore invalidate the session instead of being filtered out,
    because filtering would shift later indices and could validate the wrong
    target.
    """
    if callback.kind != "card" or callback.target_index is None:
        return False
    ids: list[str] = []
    for value in session_public_listing_ids:
        normalized = normalize_public_id(value)
        if normalized is None:
            return False
        ids.append(normalized)
    index = int(callback.target_index)
    return (
        0 <= index < len(ids)
        and ids[index] == callback.public_listing_id
    )


__all__ = [
    "PREFIX",
    "UserBotCallback",
    "encode_card_callback",
    "encode_change_search_callback",
    "encode_listing_callback",
    "encode_semantic_action",
    "parse_callback",
    "validate_card_navigation",
]

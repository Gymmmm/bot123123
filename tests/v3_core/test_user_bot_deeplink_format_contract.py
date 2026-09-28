"""Regression test for the canonical Telegram User Bot deep link format.

Telegram's public bot-start deep link is::

    https://t.me/<user_bot_username>?start=<payload>

The legacy ``https://t.me/<user_bot_username>/start?start_param=<payload>``
form is parsed by Telegram as ``tg://resolve?domain=<bot>&appname=start``
and **never reaches the bot's ``/start`` handler** — every channel CTA,
broadcast button and admin deep link built that way silently breaks.

This test freezes the canonical V3 format end-to-end:

* ``build_bot_start_url`` is the single source of truth for the URL form
  (``v3_core.publishing.channel_contract.build_bot_start_url``).
* ``channel_action_url`` and ``channel_general_action_url`` both delegate to
  it, so every V3 producer renders the same shape.
* The frozen V3 payloads — ``property_<id>_<action>[__<source>]``,
  ``more_<area_slug>``, ``find`` — survive a round-trip through the parser
  unchanged.
"""
from __future__ import annotations

from v3_core.publishing.channel_contract import (
    TELEGRAM_BOT_DEEPLINK_HOST,
    build_bot_start_url,
    channel_action_url,
    channel_general_action_url,
    channel_start_payload,
)
from v3_core.user_bot.deeplink import (
    AREA_START_SLUGS,
    PUBLIC_CHANNEL_ACTIONS,
    SOURCE_CODE_MAP,
    parse_channel_start_payload,
    parse_search_start_payload,
)


USER_BOT = "XxxXiaopengbot"


# ---------------------------------------------------------------------------
# 1. build_bot_start_url — the canonical helper
# ---------------------------------------------------------------------------


def test_build_bot_start_url_property_details_payload_uses_canonical_format():
    payload = "property_QL-DD-N2J2_details"
    assert build_bot_start_url(USER_BOT, payload) == (
        f"https://t.me/{USER_BOT}?start={payload}"
    )


def test_build_bot_start_url_property_book_with_source_preserves_double_underscore():
    payload = "property_QL-PP-V9R7_book__sr"
    assert build_bot_start_url(USER_BOT, payload) == (
        f"https://t.me/{USER_BOT}?start={payload}"
    )


def test_build_bot_start_url_more_area_payload_uses_canonical_format():
    assert build_bot_start_url(USER_BOT, "more_bkk1") == (
        f"https://t.me/{USER_BOT}?start=more_bkk1"
    )


def test_build_bot_start_url_strips_leading_at_from_username():
    payload = "property_QL-DD-N2J2_details"
    assert build_bot_start_url(f"@{USER_BOT}", payload) == (
        f"https://t.me/{USER_BOT}?start={payload}"
    )


def test_build_bot_start_url_does_not_emit_double_encoding_or_escape_underscores():
    payload = "property_QL-PP-V9R7_book__sr"
    url = build_bot_start_url(USER_BOT, payload)
    assert "%2F" not in url
    assert "%5F" not in url
    assert payload in url  # underscores and slashes untouched


def test_build_bot_start_url_returns_empty_string_for_missing_username():
    assert build_bot_start_url("", "property_QL-DD-N2J2_details") == ""
    assert build_bot_start_url("   ", "property_QL-DD-N2J2_details") == ""


def test_build_bot_start_url_rejects_empty_or_whitespace_payload():
    import pytest

    with pytest.raises(ValueError):
        build_bot_start_url(USER_BOT, "")
    with pytest.raises(ValueError):
        build_bot_start_url(USER_BOT, "   ")
    with pytest.raises(ValueError):
        build_bot_start_url(USER_BOT, "find home")


def test_build_bot_start_url_uses_telegram_host_constant():
    assert TELEGRAM_BOT_DEEPLINK_HOST == "https://t.me"
    assert build_bot_start_url(USER_BOT, "find").startswith(
        f"{TELEGRAM_BOT_DEEPLINK_HOST}/"
    )


# ---------------------------------------------------------------------------
# 2. Higher-level helpers must delegate to build_bot_start_url
# ---------------------------------------------------------------------------


def test_channel_action_url_delegates_to_build_bot_start_url_for_each_action():
    for action in PUBLIC_CHANNEL_ACTIONS:
        url = channel_action_url(USER_BOT, "QL-DD-N2J2", action)
        assert url == (
            f"https://t.me/{USER_BOT}?start=property_QL-DD-N2J2_{action}"
        )


def test_channel_action_url_with_source_code_emits_double_underscore_payload():
    url = channel_action_url(USER_BOT, "QL-PP-V9R7", "book", source_code="sr")
    assert url == (
        f"https://t.me/{USER_BOT}?start=property_QL-PP-V9R7_book__sr"
    )


def test_channel_general_action_url_emits_canonical_find_and_more_payloads():
    assert channel_general_action_url(USER_BOT, "find") == (
        f"https://t.me/{USER_BOT}?start=find"
    )
    assert channel_general_action_url(USER_BOT, "more_bkk1") == (
        f"https://t.me/{USER_BOT}?start=more_bkk1"
    )


# ---------------------------------------------------------------------------
# 3. No canonical helper may ever emit the broken Telegram form
# ---------------------------------------------------------------------------


def test_canonical_helpers_never_emit_legacy_start_param_form():
    for payload in (
        "property_QL-DD-N2J2_details",
        "property_QL-PP-V9R7_book__sr",
        "more_bkk1",
        "find",
    ):
        url = build_bot_start_url(USER_BOT, payload)
        assert "/start?start_param=" not in url
        assert "start_param=" not in url
        assert url.startswith(f"https://t.me/{USER_BOT}?start=")


def test_canonical_helpers_never_emit_empty_payload():
    """`?start=` (empty payload) routes to the bot home view, not the target."""
    import pytest

    with pytest.raises(ValueError):
        build_bot_start_url(USER_BOT, "")


# ---------------------------------------------------------------------------
# 4. Parser regression — frozen payloads still resolve
# ---------------------------------------------------------------------------


PROPERTY_IDS = (
    "QL-DD-N2J2",
    "QL-PP-V9R7",
    "QL-RF-A2B3",
    "QL-BK-C4D5",
)


def test_property_payloads_round_trip_through_canonical_builder():
    for public_id in PROPERTY_IDS:
        for action in PUBLIC_CHANNEL_ACTIONS:
            payload = channel_start_payload(public_id, action)
            url = build_bot_start_url(USER_BOT, payload)
            assert url == (
                f"https://t.me/{USER_BOT}?start={payload}"
            ), f"unexpected url for {payload}: {url}"
            route = parse_channel_start_payload(payload)
            assert route is not None
            assert route.action == action
            assert route.public_listing_id == public_id


def test_property_payload_with_source_code_round_trips_through_canonical_builder():
    for source_code in SOURCE_CODE_MAP:
        payload = channel_start_payload(
            "QL-PP-V9R7", "book", source_code=source_code
        )
        url = build_bot_start_url(USER_BOT, payload)
        assert url == (
            f"https://t.me/{USER_BOT}?start=property_QL-PP-V9R7_book__{source_code}"
        )
        route = parse_channel_start_payload(payload)
        assert route is not None
        assert route.action == "book"
        assert route.public_listing_id == "QL-PP-V9R7"
        assert route.source_code == source_code
        assert route.source == SOURCE_CODE_MAP[source_code]


def test_all_frozen_area_slugs_round_trip_through_canonical_builder():
    for area_slug, location_key in AREA_START_SLUGS.items():
        payload = f"more_{area_slug}"
        url = build_bot_start_url(USER_BOT, payload)
        assert url == f"https://t.me/{USER_BOT}?start={payload}"
        route = parse_search_start_payload(payload)
        assert route is not None
        assert route.action == "more"
        assert route.area_slug == area_slug
        assert route.location_key == location_key


def test_find_payload_round_trips_through_canonical_builder():
    payload = "find"
    url = build_bot_start_url(USER_BOT, payload)
    assert url == f"https://t.me/{USER_BOT}?start=find"
    route = parse_search_start_payload(payload)
    assert route is not None
    assert route.action == "find"


# ---------------------------------------------------------------------------
# 5. V3 production — no inline f-string or string concatenation may ever
#    produce the broken Telegram form anywhere under v3_core/.
# ---------------------------------------------------------------------------


def test_v3_production_source_never_assembles_broken_start_param_url():
    """Sweep every V3 production file for the legacy broken pattern.

    The canonical helper ``build_bot_start_url`` is the single source of truth;
    nothing else may assemble ``https://t.me/<user>?start=<payload>`` or the
    broken ``/start?start_param=`` form by hand.
    """
    import re
    from pathlib import Path

    repo_root = Path("v3_core")
    assert repo_root.is_dir(), "v3_core/ must be present (run from repo root)"

    # Broken form Telegram actually parses as ``appname=start``.
    # The docstring of ``build_bot_start_url`` documents the broken form on
    # purpose so future readers know what NOT to ship. Allow that single
    # deliberate mention.
    broken_pattern = re.compile(r"start_param")
    broken_allowed_in = {"channel_contract.py"}  # canonical helper's own docstring
    # The canonical form: only ``build_bot_start_url`` is allowed to emit it.
    canonical_pattern = re.compile(r"\?start=")
    canonical_allowed_in = {"channel_contract.py"}  # canonical builder itself

    found_canonical = []
    for path in repo_root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        if broken_pattern.search(text) and path.name not in broken_allowed_in:
            raise AssertionError(
                f"broken `start_param` form found in {path}"
            )
        if canonical_pattern.search(text) and path.name not in canonical_allowed_in:
            raise AssertionError(
                f"v3 file {path} hand-assembles `?start=` instead of "
                "calling build_bot_start_url"
            )
        if path.name == "channel_contract.py":
            found_canonical.append(path)

    assert found_canonical, "build_bot_start_url not found in channel_contract.py"

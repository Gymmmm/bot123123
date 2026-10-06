"""Local-fix contract for the V3 User Bot 「更多实拍」exit/cleanup.

The paged album is sent as 4 photo frames + 1 action bar (5 message ids). The
album can be exited via:

* ``details`` (⬅️ 返回房源)         → in-place edit of the action bar
* ``book``   (📅 预约看房)         → in-place edit via _render_transition_view
* ``similar``(🔍 找相似)           → in-place edit via _render_transition_view
* ``change_search`` (🔄 调整条件)  → in-place edit via _render_transition_view
* ``consult``(💬 咨询这套)         → pure intent, no in-place edit, no new msg

For every exit path:

* the 4 photo frames are deleted from chat;
* the recorded ``message_ids`` / ``page`` state for that listing is cleared,
  so the next ``📷 查看实拍`` entry can re-issue the album (otherwise the
  page-idempotency guard would short-circuit and never re-send photos);
* for the four in-place edit paths, the action bar message is **not** deleted
  — ``query.edit_message_text/caption`` rewrites it in place.
* for ``consult`` (no in-place edit), the entire 5-message group is dropped.

These tests live alongside the existing paging tests and lock the contract
with a real ``RouterStub`` flow: ``v3u:listing:photos:<id>:pg:0`` → album
is recorded → exit via each button → album state is cleared → re-entering
``pg:0`` issues a fresh 4-photo album.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from v3_core.user_bot.callback_router import CallbackDispatchResult
from v3_core.user_bot.callbacks import parse_callback
from v3_core.user_bot.consult import ConsultIntent, ConsultResult
from v3_core.user_bot.listing_responses import (
    PublicDetailsResponse,
    PublicPhotosResponse,
    SemanticAction,
)
from v3_core.user_bot.public_flow import (
    PublicBookIntent,
    PublicListingFlowResult,
)
from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.telegram_callback_handler import (
    PHOTOS_ALBUM_KEY,
    handle_v3_callback,
)
from v3_core.user_bot.transition_views import (
    TransitionChoice,
    TransitionView,
)


PUBLIC_ID = "QL-RF-A2B3"


# ---------------------------------------------------------------------------
# Shared stubs
# ---------------------------------------------------------------------------


class RouterStub:
    def __init__(self, result):
        self.result = result
        self.calls: list[tuple] = []

    def dispatch(self, raw, *, session_public_listing_ids=()):
        self.calls.append((raw, tuple(session_public_listing_ids)))
        return self.result


class ViewServiceStub:
    def __init__(self, view):
        self.view = view
        self.plans: list[Any] = []

    def build(self, plan):
        self.plans.append(plan)
        return self.view


class FakeQuery:
    def __init__(self, data, *, has_photo=False):
        self.data = data
        self.message = SimpleNamespace(photo=[object()] if has_photo else [])
        self.calls: list[tuple] = []

    async def answer(self, *args, **kwargs):
        self.calls.append(("answer", args, kwargs))

    async def edit_message_text(self, *args, **kwargs):
        self.calls.append(("edit_text", args, kwargs))

    async def edit_message_caption(self, *args, **kwargs):
        self.calls.append(("edit_caption", args, kwargs))

    async def edit_message_media(self, *args, **kwargs):
        self.calls.append(("edit_media", args, kwargs))


class FakeBot:
    def __init__(self):
        self.calls: list[tuple] = []
        self.next_message_id = 1000
        self.returned_ids: list[int] = []

    def _alloc_id(self) -> int:
        current = self.next_message_id
        self.next_message_id += 1
        return current

    async def send_photo(self, **kwargs):
        self.calls.append(("send_photo", kwargs))
        mid = self._alloc_id()
        self.returned_ids.append(mid)
        return SimpleNamespace(chat_id=kwargs["chat_id"], message_id=mid)

    async def send_media_group(self, **kwargs):
        self.calls.append(("send_media_group", kwargs))
        frames = list(kwargs.get("media") or ())
        ids = [self._alloc_id() for _ in frames]
        self.returned_ids.extend(ids)
        return [
            SimpleNamespace(chat_id=kwargs["chat_id"], message_id=mid) for mid in ids
        ]

    async def send_message(self, **kwargs):
        self.calls.append(("send_message", kwargs))
        mid = self._alloc_id()
        self.returned_ids.append(mid)
        return SimpleNamespace(chat_id=kwargs["chat_id"], message_id=mid)

    async def delete_message(self, **kwargs):
        self.calls.append(("delete_message", kwargs))


def _update(query):
    return SimpleNamespace(
        callback_query=query,
        effective_chat=SimpleNamespace(id=12345),
        effective_user=SimpleNamespace(id=99, username="u", full_name="User"),
    )


def _context():
    return SimpleNamespace(bot=FakeBot(), user_data={})


# ---------------------------------------------------------------------------
# Album entry helper — drives a 9-photo listing into the paged album page 0
# via the real handle_v3_callback so the recorded state is identical to
# production.
# ---------------------------------------------------------------------------


def _nine_photo_view(tmp_path: Path) -> PublishedListingView:
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_LOCAL_1",
        "public_listing_id": PUBLIC_ID,
        "offer_id": "OFF_LOCAL_1",
        "canonical_record_id": "CAN_LOCAL_1",
        "canonical_facts_hash": "h-local",
        "canonical_facts": {},
        "adviser_copy": "",
        "listing": {
            "project_name": "富力城",
            "property_type": "公寓",
            "layout": "2房1厅",
            "public_location_display": "BKK1",
            "size_sqm": 80,
            "floor": "10",
        },
        "offer": {
            "offer_type": "rent",
            "monthly_rent_usd": 800,
            "deposit_terms": "押2付1",
            "payment_terms": "押1付1",
            "contract_term": "1年",
            "publication_policy": "telegram_rent",
        },
    }
    paths = []
    for index in range(9):
        path = tmp_path / f"local_{index + 1}.jpg"
        path.write_bytes(f"frame-{index}".encode("utf-8"))
        paths.append(str(path))
    return PublishedListingView(
        listing={
            "listing_id": "LST_LOCAL_1",
            "public_listing_id": PUBLIC_ID,
            "inventory_status": "active",
        },
        offer={
            "offer_id": "OFF_LOCAL_1",
            "offer_type": "rent",
            "offer_status": "active",
            "publication_policy": "telegram_rent",
        },
        publication={"instance_id": "PUB_LOCAL_1"},
        package={
            "snapshot_json": json.dumps(snapshot, ensure_ascii=False),
            "gallery_json": json.dumps(paths, ensure_ascii=False),
        },
    )


def _four_real_frames(tmp_path: Path, *, suffix: str) -> tuple[str, ...]:
    paths = []
    for index in range(4):
        path = tmp_path / f"{suffix}_{index + 1}.jpg"
        path.write_bytes(b"\xff\xd8\xff\xe0frame")
        paths.append(str(path))
    return tuple(paths)


def _photos_paged_dispatch(
    raw: str, page: int, *, frames: tuple[str, ...]
) -> CallbackDispatchResult:
    """Real PublicListingFlowService-free photos dispatch stub.

    Returns a 4-frame paged album whose slicing matches the public-list
    contract (4/4/1 for 9 originals). The keyboard mirrors the production
    paged album: 上一页/1/N/下一页 row + book/consult row + 返回房源 row.
    """
    return CallbackDispatchResult(
        status="ok",
        callback=parse_callback(raw),
        action="photos",
        listing=PublicListingFlowResult(
            status="ok",
            action="photos",
            public_listing_id=PUBLIC_ID,
            photos=PublicPhotosResponse(
                media_groups=(frames,),
                text=f"📷 实拍房源 {PUBLIC_ID}\n第 {page + 1}/3 页 · 共 9 张",
                media_caption=f"📷 实拍 {page + 1}/3",
                photo_path=frames[0],
                photo_total=9,
                action_rows=(
                    (
                        SemanticAction("⬅️ 上一页", "photos", PUBLIC_ID, target_index=page - 1 if page > 0 else 0),
                        SemanticAction(f"{page + 1}/3", "photos", PUBLIC_ID, target_index=page),
                        SemanticAction("下一页 ➡️", "photos", PUBLIC_ID, target_index=page + 1),
                    ),
                    (
                        SemanticAction("📅 预约看房", "book", PUBLIC_ID),
                        SemanticAction("💬 咨询这套", "consult", PUBLIC_ID),
                    ),
                    (SemanticAction("⬅️ 返回房源", "details", PUBLIC_ID),),
                ),
            ),
        ),
    )


async def _enter_paged_album_page0(tmp_path: Path):
    """Open the paged album page 0 and return (context, prior 5 ids, action_bar_id)."""
    view = _nine_photo_view(tmp_path)
    # We don't need the view — the dispatch stub returns a 4-frame payload —
    # but we keep it in scope so future readers see the real fixture.
    assert view.public_listing_id == PUBLIC_ID

    ctx = _context()
    frames = _four_real_frames(tmp_path, suffix="page0")
    first_raw = f"v3u:listing:photos:{PUBLIC_ID}:pg:0"
    await handle_v3_callback(
        _update(FakeQuery(first_raw)),
        ctx,
        router=RouterStub(_photos_paged_dispatch(first_raw, page=0, frames=frames)),
    )
    prior_ids = list(ctx.bot.returned_ids)
    assert len(prior_ids) == 5, prior_ids  # 4 photos + 1 action bar
    state_key = f"12345::{PUBLIC_ID}"
    state = ctx.user_data[PHOTOS_ALBUM_KEY][state_key]
    assert list(state["message_ids"]) == prior_ids
    assert state["page"] == 0
    return ctx, prior_ids


def _details_dispatch() -> CallbackDispatchResult:
    return CallbackDispatchResult(
        status="ok",
        callback=parse_callback(f"v3u:listing:details:{PUBLIC_ID}"),
        action="details",
        listing=PublicListingFlowResult(
            status="ok",
            action="details",
            public_listing_id=PUBLIC_ID,
            details=PublicDetailsResponse(
                text="details-body",
                action_rows=(
                    (SemanticAction("📅 预约看房", "book", PUBLIC_ID),),
                ),
            ),
        ),
    )


def _book_dispatch() -> CallbackDispatchResult:
    return CallbackDispatchResult(
        status="ok",
        callback=parse_callback(f"v3u:listing:book:{PUBLIC_ID}"),
        action="book",
        listing=PublicListingFlowResult(
            status="ok",
            action="book",
            public_listing_id=PUBLIC_ID,
            book=PublicBookIntent(
                listing_id="LST_1",
                public_listing_id=PUBLIC_ID,
                source="listing_callback",
                start_payload="",
            ),
        ),
    )


def _consult_dispatch() -> CallbackDispatchResult:
    return CallbackDispatchResult(
        status="ok",
        callback=parse_callback(f"v3u:listing:consult:{PUBLIC_ID}"),
        action="consult",
        consult=ConsultResult(
            status="ok",
            public_listing_id=PUBLIC_ID,
            intent=ConsultIntent(
                listing_id="LST_1",
                public_listing_id=PUBLIC_ID,
                source="listing_callback",
                inventory_status="active",
                offer_status="active",
                publication_instance_id="PUB_1",
            ),
        ),
    )


def _similar_dispatch() -> CallbackDispatchResult:
    from v3_core.user_bot.similar_intent import SimilarIntentResult, SimilarSearchIntent
    intent = SimilarSearchIntent(
        listing_id="LST_1",
        public_listing_id=PUBLIC_ID,
        source="listing_callback",
        goal="any",
        location_keys=("BKK1",),
        area_display="BKK1",
        next_step="search_submit",
        room_type="",
        property_type="",
    )
    return CallbackDispatchResult(
        status="ok",
        callback=parse_callback(f"v3u:listing:similar:{PUBLIC_ID}"),
        action="similar",
        similar=SimilarIntentResult(status="ok", public_listing_id=PUBLIC_ID, intent=intent),
    )


def _change_search_dispatch() -> CallbackDispatchResult:
    return CallbackDispatchResult(
        status="ok",
        callback=parse_callback("v3u:change_search"),
        action="change_search",
        change_search=True,
    )


def _appointment_view() -> TransitionView:
    return TransitionView(
        kind="appointment_date",
        text="📅 预约看房",
        rows=(
            (TransitionChoice("今天", "appointment_date", "09-08"),),
        ),
    )


def _change_search_view() -> TransitionView:
    return TransitionView(
        kind="search_entry",
        text="🔍 调整条件",
        rows=(
            (TransitionChoice("继续", "change_search"),),
        ),
    )


# ---------------------------------------------------------------------------
# Exit contract tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exit_details_deletes_4_photos_keeps_action_bar_clears_state(tmp_path):
    """⬅️ 返回房源 from the paged album: drop 4 photos, keep the action bar,
    edit the action bar in place to the details body, clear the album state.

    The next 「📷 查看实拍」entry must re-issue a fresh 4-photo album (the
    page-idempotency guard must NOT short-circuit).
    """
    ctx, prior_ids = await _enter_paged_album_page0(tmp_path)
    # prior_ids = [p1, p2, p3, p4, action_bar]
    action_bar_id = prior_ids[-1]

    # Click ⬅️ 返回房源
    details_query = FakeQuery(f"v3u:listing:details:{PUBLIC_ID}")
    update = _update(details_query)
    await handle_v3_callback(
        update,
        ctx,
        router=RouterStub(_details_dispatch()),
    )

    # 1) Exactly 4 photo frames deleted — the action bar is NOT deleted.
    deleted = [c[1]["message_id"] for c in ctx.bot.calls if c[0] == "delete_message"]
    assert deleted == prior_ids[:-1], deleted
    assert action_bar_id not in deleted

    # 2) The action bar was edited in place (not deleted + resent).
    edit_calls = [
        c for c in details_query.calls if c[0] in {"edit_text", "edit_caption"}
    ]
    assert edit_calls, "details must rewrite the action bar in place"

    # 3) Album state for this listing is fully cleared.
    state_key = f"12345::{PUBLIC_ID}"
    state = ctx.user_data[PHOTOS_ALBUM_KEY][state_key]
    assert "message_ids" not in state
    assert "page" not in state


@pytest.mark.asyncio
async def test_reenter_paged_album_page0_after_details_exit_reissues_fresh_album(tmp_path):
    """State-clearing contract: after 返回详情 → 再点 📷 查看实拍, the
    idempotency guard must NOT block the second entry. The user gets a
    fresh 4-photo album."""
    ctx, _ = await _enter_paged_album_page0(tmp_path)

    # Exit to details.
    await handle_v3_callback(
        _update(FakeQuery(f"v3u:listing:details:{PUBLIC_ID}")),
        ctx,
        router=RouterStub(_details_dispatch()),
    )
    # Reset the bot log so we can inspect the re-entry alone.
    ctx.bot.calls.clear()
    ctx.bot.returned_ids.clear()

    # Re-enter the paged album at page 0.
    reentry_raw = f"v3u:listing:photos:{PUBLIC_ID}:pg:0"
    await handle_v3_callback(
        _update(FakeQuery(reentry_raw)),
        ctx,
        router=RouterStub(
            _photos_paged_dispatch(reentry_raw, page=0, frames=_four_real_frames(tmp_path, suffix="reentry"))
        ),
    )

    # 4 photos + 1 action bar must be sent.
    send_kinds = [c[0] for c in ctx.bot.calls if c[0].startswith("send_")]
    assert send_kinds == ["send_media_group", "send_message"], send_kinds
    media = next(c for c in ctx.bot.calls if c[0] == "send_media_group")[1]["media"]
    assert len(media) == 4
    # State recorded with the new ids.
    state_key = f"12345::{PUBLIC_ID}"
    state = ctx.user_data[PHOTOS_ALBUM_KEY][state_key]
    assert state["page"] == 0
    assert len(state["message_ids"]) == 5


@pytest.mark.asyncio
async def test_exit_book_keeps_action_bar_so_edit_message_does_not_target_deleted(tmp_path):
    """📅 预约看房 from the paged album: drop 4 photos, KEEP the action bar
    so the downstream ``_render_transition_view`` can ``query.edit_message_*``
    it in place. Edit must NOT target a deleted message_id."""
    ctx, prior_ids = await _enter_paged_album_page0(tmp_path)
    action_bar_id = prior_ids[-1]

    book_query = FakeQuery(f"v3u:listing:book:{PUBLIC_ID}")
    views = ViewServiceStub(_appointment_view())
    await handle_v3_callback(
        _update(book_query),
        ctx,
        router=RouterStub(_book_dispatch()),
        transition_views=views,
    )

    # 1) Action bar must NOT be deleted — only the 4 photo frames.
    deleted = [c[1]["message_id"] for c in ctx.bot.calls if c[0] == "delete_message"]
    assert deleted == prior_ids[:-1], deleted
    assert action_bar_id not in deleted

    # 2) _render_transition_view rewrote the action bar in place.
    edit_calls = [c for c in book_query.calls if c[0] in {"edit_text", "edit_caption"}]
    assert edit_calls, "book transition must edit the action bar in place"

    # 3) The transition view was built exactly once.
    assert len(views.plans) == 1
    assert views.plans[0].kind == "book"

    # 4) Album state cleared (so a future re-entry is not idempotent-blocked).
    state_key = f"12345::{PUBLIC_ID}"
    state = ctx.user_data[PHOTOS_ALBUM_KEY][state_key]
    assert "message_ids" not in state
    assert "page" not in state


@pytest.mark.asyncio
async def test_exit_consult_drops_full_5_message_group_without_rendering_transition_view(tmp_path):
    """💬 咨询这套 from the paged album: drop ALL 5 messages (no in-place
    edit, no transition view) AND clear the album state."""
    ctx, prior_ids = await _enter_paged_album_page0(tmp_path)
    # Snapshot calls so we can inspect only what happens after the consult click.
    ctx.bot.calls.clear()

    await handle_v3_callback(
        _update(FakeQuery(f"v3u:listing:consult:{PUBLIC_ID}")),
        ctx,
        router=RouterStub(_consult_dispatch()),
    )

    # 1) All 5 messages deleted.
    deleted = [c[1]["message_id"] for c in ctx.bot.calls if c[0] == "delete_message"]
    assert deleted == prior_ids, deleted

    # 2) NO in-place edit (consult is a pure intent).
    edit_calls = [c for c in ctx.bot.calls if c[0] in {"edit_text", "edit_caption"}]
    assert not edit_calls, edit_calls

    # 3) NO new send — consult is delivered out-of-band by a higher-level handler.
    send_calls = [c for c in ctx.bot.calls if c[0].startswith("send_")]
    assert send_calls == [], send_calls

    # 4) Album state cleared.
    state_key = f"12345::{PUBLIC_ID}"
    state = ctx.user_data[PHOTOS_ALBUM_KEY][state_key]
    assert "message_ids" not in state
    assert "page" not in state

    # 5) user_data must not gain a stale PHOTOS_ALBUM_KEY container for empty states.
    # (The entry contract is: when the user is on an album, cleanup clears the
    # entry. When they're not, no entry is created at all — see next test.)


@pytest.mark.asyncio
async def test_consult_from_search_card_never_creates_photos_album_state():
    """Consult invoked outside the paged album must NOT create any
    ``PHOTOS_ALBUM_KEY`` state — search-card consult stays a pure intent."""
    ctx = _context()
    await handle_v3_callback(
        _update(FakeQuery(f"v3u:listing:consult:{PUBLIC_ID}")),
        ctx,
        router=RouterStub(_consult_dispatch()),
    )

    assert PHOTOS_ALBUM_KEY not in ctx.user_data
    assert ctx.user_data == {}
    # No bot calls at all — consult is intent-only.
    assert ctx.bot.calls == []


@pytest.mark.asyncio
async def test_exit_similar_keeps_action_bar_clears_state(tmp_path):
    """🔍 找相似 from the paged album: same shape as book — keep card for
    in-place edit, clear state."""
    ctx, prior_ids = await _enter_paged_album_page0(tmp_path)
    action_bar_id = prior_ids[-1]

    similar_query = FakeQuery(f"v3u:listing:similar:{PUBLIC_ID}")
    views = ViewServiceStub(_appointment_view())
    await handle_v3_callback(
        _update(similar_query),
        ctx,
        router=RouterStub(_similar_dispatch()),
        transition_views=views,
    )

    deleted = [c[1]["message_id"] for c in ctx.bot.calls if c[0] == "delete_message"]
    assert deleted == prior_ids[:-1], deleted
    assert action_bar_id not in deleted

    edit_calls = [c for c in similar_query.calls if c[0] in {"edit_text", "edit_caption"}]
    assert edit_calls, "similar transition must edit the action bar in place"

    state_key = f"12345::{PUBLIC_ID}"
    state = ctx.user_data[PHOTOS_ALBUM_KEY][state_key]
    assert "message_ids" not in state
    assert "page" not in state


@pytest.mark.asyncio
async def test_exit_change_search_keeps_action_bar_clears_state(tmp_path):
    """🔄 调整条件 from the paged album: same shape — keep card, clear state."""
    ctx, prior_ids = await _enter_paged_album_page0(tmp_path)
    action_bar_id = prior_ids[-1]

    change_query = FakeQuery("v3u:change_search")
    views = ViewServiceStub(_change_search_view())
    await handle_v3_callback(
        _update(change_query),
        ctx,
        router=RouterStub(_change_search_dispatch()),
        transition_views=views,
    )

    deleted = [c[1]["message_id"] for c in ctx.bot.calls if c[0] == "delete_message"]
    assert deleted == prior_ids[:-1], deleted
    assert action_bar_id not in deleted

    edit_calls = [c for c in change_query.calls if c[0] in {"edit_text", "edit_caption"}]
    assert edit_calls, "change_search must edit the action bar in place"

    state_key = f"12345::{PUBLIC_ID}"
    state = ctx.user_data[PHOTOS_ALBUM_KEY][state_key]
    assert "message_ids" not in state
    assert "page" not in state


# ---------------------------------------------------------------------------
# Paging still deletes old 5 and sends new 5 (regression guard for the
# existing requirement, restated after the keep_action_bar rework).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_paging_still_deletes_full_5_and_sends_new_5(tmp_path):
    """翻页: 先删旧 5 条, 再发新 5 条。"""
    ctx, prior_ids = await _enter_paged_album_page0(tmp_path)

    # Move to page 1.
    next_raw = f"v3u:listing:photos:{PUBLIC_ID}:pg:1"
    await handle_v3_callback(
        _update(FakeQuery(next_raw)),
        ctx,
        router=RouterStub(
            _photos_paged_dispatch(next_raw, page=1, frames=_four_real_frames(tmp_path, suffix="page1"))
        ),
    )

    # All 5 prior ids deleted.
    deleted = [c[1]["message_id"] for c in ctx.bot.calls if c[0] == "delete_message"]
    assert deleted == prior_ids, deleted

    # 4 new photos + 1 new action bar sent.
    new_ids = list(ctx.bot.returned_ids)
    new_send_ids = new_ids[len(prior_ids):]
    assert len(new_send_ids) == 5
    # Old and new ids are disjoint.
    assert set(new_send_ids).isdisjoint(set(prior_ids))

    # State re-recorded with the new ids and the new page index.
    state_key = f"12345::{PUBLIC_ID}"
    state = ctx.user_data[PHOTOS_ALBUM_KEY][state_key]
    assert list(state["message_ids"]) == new_send_ids
    assert state["page"] == 1

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from qiaolian_dual import admin_consult, callback_admin


def _lead(index: int, *, listing_id: str = "", area: str = "", action: str = "search_pref_submit") -> dict:
    return {
        "id": index,
        "user_id": 1000 + index,
        "display_name": f"客户{index}",
        "username": f"user{index}",
        "listing_id": listing_id,
        "area": area,
        "entry_action": action,
        "action": action,
        "lead_status": "new",
    }


def _buttons(markup):
    return [button for row in markup.inline_keyboard for button in row]


def _callbacks(markup):
    return [button.callback_data for button in _buttons(markup) if button.callback_data]


def _history_rows():
    rows = [_lead(index, area="BKK1") for index in range(17, 0, -1)]
    rows[0] = _lead(17, listing_id="QL-PP-A2B3", action="consult")
    rows[1] = _lead(16, listing_id="l_123", area="钻石岛", action="listing_detail_view")
    return rows


async def _render_history(monkeypatch, data: str, rows: list[dict]):
    monkeypatch.setattr(admin_consult, "_is_admin", lambda _user_id: True)
    monkeypatch.setattr(
        admin_consult,
        "list_leads_by_status",
        lambda status, limit=20: rows[:limit],
    )
    query = SimpleNamespace(
        id=f"history-{data}",
        data=data,
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
    )
    update = SimpleNamespace(callback_query=query, effective_user=SimpleNamespace(id=1))
    await admin_consult.handle_admin_query(update, SimpleNamespace())
    query.edit_message_text.assert_awaited_once()
    return query.edit_message_text.await_args.kwargs["reply_markup"]


@pytest.mark.asyncio
async def test_history_first_page_is_capped_at_eight_and_has_next(monkeypatch):
    keyboard = await _render_history(monkeypatch, "adminq:history", _history_rows())
    callbacks = _callbacks(keyboard)
    assert len([value for value in callbacks if value.startswith("adminq:lead:")]) == 8
    assert "adminq:history:1" in callbacks
    assert "adminq:history:-1" not in callbacks
    assert callbacks[-1] == "adminq:home"


@pytest.mark.asyncio
async def test_history_second_and_last_page_navigation(monkeypatch):
    rows = _history_rows()

    second = await _render_history(monkeypatch, "adminq:history:1", rows)
    second_callbacks = _callbacks(second)
    assert len([value for value in second_callbacks if value.startswith("adminq:lead:")]) == 8
    assert "adminq:history:0" in second_callbacks
    assert "adminq:history:2" in second_callbacks
    assert second_callbacks[-1] == "adminq:home"

    last = await _render_history(monkeypatch, "adminq:history:2", rows)
    last_callbacks = _callbacks(last)
    assert len([value for value in last_callbacks if value.startswith("adminq:lead:")]) == 1
    assert "adminq:history:1" in last_callbacks
    assert "adminq:history:3" not in last_callbacks
    assert last_callbacks[-1] == "adminq:home"


def test_history_button_is_readable_and_never_exposes_internal_listing_id():
    public_text = admin_consult._history_lead_button_text(
        _lead(1, listing_id="QL-PP-A2B3", action="consult")
    )
    assert "客户1" in public_text
    assert "QL-PP-A2B3" in public_text
    assert admin_consult.entry_action_zh("consult") in public_text

    internal_text = admin_consult._history_lead_button_text(
        _lead(2, listing_id="l_123", area="BKK1", action="search_pref_submit")
    )
    assert "客户2" in internal_text
    assert "BKK1" in internal_text
    assert "提交找房条件" in internal_text
    assert "l_123" not in internal_text


@pytest.mark.asyncio
async def test_adminq_lead_detail_still_uses_existing_detail_and_actions(monkeypatch):
    monkeypatch.setattr(admin_consult, "_is_admin", lambda _user_id: True)
    monkeypatch.setattr(admin_consult, "get_user_attribution", lambda _user_id: {})
    monkeypatch.setattr(
        admin_consult.db,
        "get_lead",
        lambda lead_id: {
            "id": lead_id,
            "user_id": 2001,
            "display_name": "黑桃♠ 8方支付",
            "listing_id": "",
            "entry_action": "consult",
            "lead_status": "new",
        },
    )

    class _Conn:
        def execute(self, *_args, **_kwargs):
            return SimpleNamespace(fetchone=lambda: None)

    monkeypatch.setattr(admin_consult.db, "connect", lambda: nullcontext(_Conn()))
    query = SimpleNamespace(
        id="lead-detail-8",
        data="adminq:lead:8",
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
    )
    update = SimpleNamespace(callback_query=query, effective_user=SimpleNamespace(id=1))

    await admin_consult.handle_admin_query(update, SimpleNamespace())

    query.edit_message_text.assert_awaited_once()
    text = query.edit_message_text.await_args.args[0]
    markup = query.edit_message_text.await_args.kwargs["reply_markup"]
    callbacks = _callbacks(markup)
    assert "黑桃♠ 8方支付" in text
    assert "adminlead:claim:8:0:2001" in callbacks
    assert "adminlead:contacted:8:0:2001" in callbacks
    assert "adminlead:done:8:0:2001" in callbacks
    assert "adminlead:invalid:8:0:2001" in callbacks


@pytest.mark.asyncio
@pytest.mark.parametrize("data", ["adminq:listings", "adminq:sources", "adminq:history"])
async def test_3858_callback_routes_adminq_pages_without_unhandled(monkeypatch, data):
    routed = AsyncMock()
    monkeypatch.setattr(admin_consult, "handle_admin_query", routed)
    query = SimpleNamespace(data=data)
    update = SimpleNamespace(callback_query=query, effective_user=SimpleNamespace(id=1))
    context = SimpleNamespace()

    result = await callback_admin.handle_admin_callback(
        update,
        context,
        query,
        data,
        update.effective_user,
    )

    assert result == callback_admin.MAIN
    routed.assert_awaited_once_with(update, context)

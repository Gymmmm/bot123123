"""Real empty V3 inventory behind the deployed legacy deep-link boundary."""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from qiaolian_dual import listing
from qiaolian_dual.start_routes import route_start_arg
from v3_core.storage.bootstrap import initialize_v3_storage


@pytest.mark.asyncio
async def test_unknown_public_id_is_not_misrepresented_as_pending(tmp_path, monkeypatch):
    db = tmp_path / "empty-inventory.db"
    initialize_v3_storage(db)
    monkeypatch.setattr(listing, "DB_PATH", str(db))
    render = AsyncMock()
    monkeypatch.setattr("qiaolian_dual.texts.render_panel", render)
    update = SimpleNamespace(effective_user=SimpleNamespace(id=1), effective_message=SimpleNamespace(chat_id=1))
    await route_start_arg(update, SimpleNamespace(user_data={}), "property_QL-Z9Z9Z9_details")
    render.assert_awaited_once()
    assert render.await_args.kwargs["text"] == "未找到该房源"
    assert "确认最新房态" not in render.await_args.kwargs["text"]


@pytest.mark.parametrize("reason,expected", [("pending", "正在确认最新房态"), ("rented", "已经租出"), ("offline", "已下架")])
def test_missing_id_fix_does_not_change_known_inventory_states(reason, expected):
    assert expected in listing.listing_unavailable_text(reason)

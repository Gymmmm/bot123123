import re
import importlib
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import autopilot_publish_bot as ap
from qiaolian_dual import admin_consult, callback_rental, callback_service, public_listing_id
from qiaolian_dual.channel_links import channel_start_payload
from qiaolian_dual.keyboards_search import local_life_keyboard, nearby_area_keyboard, service_hub_keyboard
from qiaolian_dual.public_listing_id import normalize_public_id, public_listing_id as make_public_id, resolve_listing_id
from qiaolian_dual.start_routes import route_start_arg


def _callbacks(markup):
    return [button.callback_data for row in markup.inline_keyboard for button in row if button.callback_data]


def test_public_listing_id_is_random_unique_and_permanent(tmp_path):
    db_path = tmp_path / "ids.db"
    values = [make_public_id(f"l_{index}", db_path=db_path) for index in range(1, 101)]
    assert all(re.fullmatch(r"QL-PP-[A-HJ-NP-Z][2-9][A-HJ-NP-Z][2-9]", value) for value in values)
    assert len(set(values)) == len(values)
    assert make_public_id("l_1", db_path=db_path) == values[0]
    assert make_public_id("QC0001", db_path=db_path) == values[0]
    assert resolve_listing_id(values[0], db_path=db_path) == "l_1"
    assert resolve_listing_id("QC0001", db_path=db_path) == "l_1"


def test_location_code_is_fixed_at_first_generation(tmp_path):
    db_path = tmp_path / "location.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE listings(listing_id TEXT PRIMARY KEY, project TEXT, area TEXT)")
        conn.execute("INSERT INTO listings VALUES ('l_7', '富力城', '洪森大道')")
    first = make_public_id("l_7", db_path=db_path)
    assert re.fullmatch(r"QL-RF-[A-HJ-NP-Z][2-9][A-HJ-NP-Z][2-9]", first)
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE listings SET project='钻石岛', area='钻石岛' WHERE listing_id='l_7'")
    assert make_public_id("l_7", db_path=db_path) == first


def test_public_id_normalization_and_legacy_lookup(tmp_path):
    db_path = tmp_path / "compat.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE listing_public_ids(listing_id TEXT PRIMARY KEY, public_id TEXT UNIQUE, created_at TEXT)")
        conn.execute("INSERT INTO listing_public_ids VALUES ('l_1', 'QL-A7K3M9', CURRENT_TIMESTAMP)")
    assert normalize_public_id("QL-A7K3M9") == "QL-A7K3M9"
    assert normalize_public_id("ql-a7k3m9") == "QL-A7K3M9"
    assert normalize_public_id("QLA7K3M9") == "QL-A7K3M9"
    assert normalize_public_id("QLRFK7M2") == "QL-RF-K7M2"
    for value in ("QL-A7K3M9", "ql-a7k3m9", "QLA7K3M9", "QC0001", "QC-0001", "QJ0001", "l_1"):
        assert resolve_listing_id(value, db_path=db_path) == "l_1"
    assert resolve_listing_id("QL-Z9Z9Z9", db_path=db_path) is None


@pytest.mark.asyncio
async def test_unknown_public_id_shows_not_found(tmp_path, monkeypatch):
    monkeypatch.setattr(public_listing_id, "DB_PATH", str(tmp_path / "unknown.db"))
    render = AsyncMock()
    monkeypatch.setattr("qiaolian_dual.texts.render_panel", render)
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=1),
        effective_message=SimpleNamespace(chat_id=1),
    )
    result = await route_start_arg(update, SimpleNamespace(user_data={}), "property_QL-Z9Z9Z9_details")
    assert result is not None
    assert render.await_args.kwargs["text"] == "未找到该房源"


def test_new_and_legacy_deep_links_resolve_same_listing(tmp_path, monkeypatch):
    monkeypatch.setattr(public_listing_id, "DB_PATH", str(tmp_path / "links.db"))
    public_id = make_public_id("l_2")
    assert channel_start_payload("l_2", "details") == f"property_{public_id}_details"
    assert resolve_listing_id(public_id) == "l_2"
    assert resolve_listing_id("QC0002") == "l_2"


def test_import_result_card_shows_required_state_and_hides_publish(monkeypatch, tmp_path):
    monkeypatch.setattr(public_listing_id, "DB_PATH", str(tmp_path / "card.db"))
    row = {"id": 8, "listing_id": "l_8", "project": "测试项目", "layout": "2房", "price": 800, "review_status": "pending"}
    text, keyboard = ap._intake_result_card(row, 2)
    assert all(label in text for label in ("房源编号", "解析状态", "图片数量", "缺失字段", "当前状态"))
    assert "QC编号" not in text
    callbacks = _callbacks(keyboard)
    assert "ap:u:8" in callbacks and "ap:e:8" in callbacks and "ap:h:8" in callbacks
    assert not any(button.text == "📤 发布到频道" for line in keyboard.inline_keyboard for button in line)


def test_publishable_import_result_has_publish_button(monkeypatch, tmp_path):
    monkeypatch.setattr(public_listing_id, "DB_PATH", str(tmp_path / "ready.db"))
    row = {"id": 8, "listing_id": "l_8", "project": "测试项目", "layout": "2房", "price": 800, "review_status": "approved"}
    _text, keyboard = ap._intake_result_card(row, 4)
    assert any(button.text == "📤 发布到频道" for line in keyboard.inline_keyboard for button in line)


@pytest.mark.asyncio
async def test_sample_preview_does_not_open_db_or_send_channel(monkeypatch):
    monkeypatch.setattr(ap, "_is_admin", lambda _uid: True)
    monkeypatch.setattr(ap, "_conn", lambda: (_ for _ in ()).throw(AssertionError("database must not be used")))
    bot = SimpleNamespace(send_photo=AsyncMock(), send_message=AsyncMock())
    update = SimpleNamespace(effective_user=SimpleNamespace(id=1), effective_chat=SimpleNamespace(id=99))
    await ap.cmd_sample_preview(update, SimpleNamespace(bot=bot))
    bot.send_photo.assert_awaited_once()
    bot.send_message.assert_awaited_once()
    assert bot.send_photo.await_args.kwargs["chat_id"] == 99
    assert "示例预览，不入库、不发频道" in bot.send_photo.await_args.kwargs["caption"]


def test_service_and_nearby_buttons_use_dedicated_paths():
    assert "service:general" in _callbacks(service_hub_keyboard())
    assert "service:general" in _callbacks(local_life_keyboard())
    assert "service:nearby" in _callbacks(local_life_keyboard())
    assert _callbacks(nearby_area_keyboard()) == ["local:rfcity", "local:other", "service:local_life"]


@pytest.mark.asyncio
async def test_general_service_clears_listing_and_does_not_enter_search(monkeypatch):
    render = AsyncMock()
    monkeypatch.setattr("qiaolian_dual.texts.render_panel", render)
    context = SimpleNamespace(user_data={"contact_listing_id": "l_8", "appt": {"listing_id": "l_8"}})
    update = SimpleNamespace(effective_user=SimpleNamespace(id=1))
    result = await callback_service.handle_service_callback(update, context, SimpleNamespace(), "service:general", update.effective_user)
    assert result is not None
    assert "contact_listing_id" not in context.user_data and "appt" not in context.user_data
    assert context.user_data["awaiting_service_general"] is True


@pytest.mark.asyncio
async def test_admin_today_appointments_stays_on_adminq(monkeypatch):
    monkeypatch.setattr(admin_consult, "_is_admin", lambda _uid: True)
    monkeypatch.setattr(admin_consult, "list_today_appointments", lambda *_args: [])
    query = SimpleNamespace(id="admin-appointments", data="adminq:appointments", answer=AsyncMock(), edit_message_text=AsyncMock())
    update = SimpleNamespace(callback_query=query, effective_user=SimpleNamespace(id=1))
    await admin_consult.handle_admin_query(update, SimpleNamespace())
    markup = query.edit_message_text.await_args.kwargs["reply_markup"]
    assert _callbacks(markup) == ["adminq:home"]


def test_handover_and_deposit_assets_generate_separately():
    for kind in ("handover", "deposit"):
        assert (callback_rental._ASSET_DIR / f"{kind}.png").read_bytes().startswith(b"\x89PNG")
        assert (callback_rental._ASSET_DIR / f"{kind}.pdf").read_bytes().startswith(b"%PDF")


def test_public_surfaces_do_not_generate_sequential_qc_codes():
    sources = "\n".join(Path(path).read_text(encoding="utf-8") for path in (
        "qiaolian_dual/channel_links.py", "qiaolian_dual/utils_formatting.py", "publication_package.py",
        "html_cover_renderer.py", "v2/qiaolian_publisher_v2/formatters.py",
    ))
    assert not re.search(r'f["\']QC\{', sources)


def test_missing_listing_code_uses_pending_label_only():
    from v2.qiaolian_publisher_v2.formatters import _format_listing_code
    assert _format_listing_code({}) == "待生成"
    assert _format_listing_code({"draft_id": "DRF_PRIVATE"}) == "待生成"
    assert _format_listing_code({"listing_id": ""}) == "待生成"


@pytest.mark.asyncio
async def test_broadcast_settings_save_and_echo(monkeypatch):
    from v2.qiaolian_publisher_v2 import daily_broadcast_patch

    settings = {}
    monkeypatch.setattr(ap, "_get_setting", lambda key, default="": settings.get(key, default))
    monkeypatch.setattr(ap, "_set_setting", lambda key, value: settings.__setitem__(key, str(value)))
    monkeypatch.setattr(ap, "_fetch_phnom_penh_daily_info", lambda: "🌤 日期 + 金边实时天气 + USD/CNY")
    monkeypatch.setattr(ap, "_is_admin", lambda _uid: True)
    patch = importlib.reload(daily_broadcast_patch)
    patch.install_daily_broadcast_patch()

    message = SimpleNamespace(reply_text=AsyncMock())
    query = SimpleNamespace(data="daily:tpl:weekly", answer=AsyncMock(), edit_message_text=AsyncMock(), message=message)
    update = SimpleNamespace(callback_query=query, effective_user=SimpleNamespace(id=1), message=message)
    context = SimpleNamespace(user_data={}, bot=SimpleNamespace())
    await ap.on_daily_callback(update, context)
    assert settings[ap.KEY_DAILY_TEMPLATE] == "weekly"
    assert "当前模板" in query.edit_message_text.await_args.args[0]

    query.data = "daily:fxset:-10"
    await ap.on_daily_callback(update, context)
    assert settings["daily_broadcast_fx_offset"] == "-0.10"
    assert "-0.10" in query.edit_message_text.await_args.args[0]

    query.data = "daily:time:1230"
    await ap.on_daily_callback(update, context)
    assert settings[ap.KEY_DAILY_TIME] == "12:30"
    assert "12:30" in query.edit_message_text.await_args.args[0]

    query.data = "daily:btn:combo"
    await ap.on_daily_callback(update, context)
    assert settings["daily_broadcast_button"] == "combo"
    page = query.edit_message_text.await_args.args[0]
    assert "已选择：<b>组合按钮</b>" in page
    assert "底部按钮：<b>组合按钮</b>" in page
    assert "combo" not in page

    query.data = "daily:preview"
    await ap.on_daily_callback(update, context)
    daily_preview_keyboard = message.reply_text.await_args.kwargs["reply_markup"]
    query.data = "daily:tplpreview:weekly"
    await ap.on_daily_callback(update, context)
    template_preview_keyboard = message.reply_text.await_args.kwargs["reply_markup"]
    assert daily_preview_keyboard == template_preview_keyboard

    channel_bot = SimpleNamespace(send_message=AsyncMock())
    context.bot = channel_bot
    monkeypatch.setattr(ap, "CHANNEL_ID", "-100123")
    query.data = "daily:send"
    await ap.on_daily_callback(update, context)
    assert channel_bot.send_message.await_args.kwargs["reply_markup"] == daily_preview_keyboard

    context.user_data["await"] = "daily_patch_text"
    message.text = "新的广播文案"
    await ap.on_text_private(update, context)
    assert settings[ap.KEY_DAILY_TEXT] == "新的广播文案"
    assert settings[ap.KEY_DAILY_TEMPLATE] == "custom"
    assert any("当前模板" in call.args[0] for call in message.reply_text.await_args_list if call.args)

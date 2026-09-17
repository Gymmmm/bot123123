from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.assurance_views import assurance_asset_bundle, build_assurance_home_view
from v3_core.user_bot.telegram_assurance_handler import build_assurance_keyboard, handle_v3_assurance_callback


class FakeMessage:
    def __init__(self, calls):
        self.calls = calls
        self.photo = None


class FakeQuery:
    def __init__(self, data, calls):
        self.data = data
        self.calls = calls
        self.message = FakeMessage(calls)

    async def answer(self):
        self.calls.append(("answer",))

    async def edit_message_text(self, text, **kwargs):
        self.calls.append(("edit", text, kwargs.get("reply_markup")))


class FakeBot:
    def __init__(self, calls):
        self.calls = calls

    async def send_document(self, **kwargs):
        self.calls.append(("document", kwargs["chat_id"], kwargs.get("document")))


def _update(query):
    return SimpleNamespace(callback_query=query, effective_chat=SimpleNamespace(id=888))


def _context(bot):
    return SimpleNamespace(bot=bot, user_data={})


def _write_pdf(root, kind):
    generated = root / "assets" / "v2_2" / "generated"
    generated.mkdir(parents=True)
    (generated / f"{kind}.pdf").write_bytes(b"pdf")


def test_rental_service_home_is_public_content_center_with_final_labels():
    view = build_assurance_home_view()
    callbacks = [choice.callback_data for row in view.rows for choice in row if choice.callback_data]
    assert callbacks == [
        "v3u:assure:handover",
        "v3u:home:search",
        "v3u:home:contact",
        "v3u:t:home",
    ]
    labels = [choice.label for row in view.rows for choice in row]
    assert labels == [
        "📋 入住交接留档",
        "🔍 开始找房",
        "💬 中文顾问",
        "⬅️ 回首页",
    ]
    assert "租到房，不代表服务就结束了" in view.text
    assert "入住时" in view.text
    assert "房屋、表计、家具家电拍照留档" in view.text

    markup = build_assurance_keyboard(view, advisor_url="https://t.me/advisor")
    contact = markup.inline_keyboard[1][1]
    assert contact.callback_data == "v3u:home:contact"


def test_assurance_asset_bundle_uses_existing_locked_pdf_paths(tmp_path):
    bundle = assurance_asset_bundle(tmp_path, "handover")
    assert bundle.pdf_path == tmp_path / "assets" / "v2_2" / "generated" / "handover.pdf"
    assert bundle.filename == "入住交接清单.pdf"


@pytest.mark.asyncio
async def test_first_click_opens_content_page_and_does_not_send_file(tmp_path):
    _write_pdf(tmp_path, "handover")
    calls = []
    query = FakeQuery("v3u:assure:handover", calls)
    outcome = await handle_v3_assurance_callback(_update(query), _context(FakeBot(calls)), repo_root=tmp_path)
    assert outcome.handled and outcome.rendered and not outcome.assets_sent
    assert [call[0] for call in calls] == ["answer", "edit"]
    assert "入住交接留档" in calls[1][1]
    markup = calls[1][2]
    callbacks = [button.callback_data for row in markup.inline_keyboard for button in row if button.callback_data]
    assert "v3u:assure:handover_download" in callbacks


@pytest.mark.asyncio
async def test_explicit_download_click_sends_only_requested_pdf(tmp_path):
    _write_pdf(tmp_path, "deposit")
    calls = []
    query = FakeQuery("v3u:assure:deposit_download", calls)
    outcome = await handle_v3_assurance_callback(_update(query), _context(FakeBot(calls)), repo_root=tmp_path)
    assert outcome.handled and outcome.assets_sent and not outcome.rendered
    assert [call[0] for call in calls] == ["answer", "document"]


@pytest.mark.asyncio
async def test_tenant_deposit_returns_to_terminate_flow(tmp_path):
    calls = []
    query = FakeQuery("v3u:assure:deposit_tenant", calls)
    outcome = await handle_v3_assurance_callback(
        _update(query), _context(FakeBot(calls)), repo_root=tmp_path
    )
    assert outcome.handled and outcome.rendered
    markup = calls[-1][2]
    callbacks = [
        button.callback_data
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data
    ]
    assert "v3u:service:tenant_terminate" in callbacks
    assert "v3u:home:rental" not in callbacks


@pytest.mark.asyncio
async def test_missing_pdf_fails_only_after_explicit_download(tmp_path):
    calls = []
    query = FakeQuery("v3u:assure:handover_download", calls)
    with pytest.raises(FileNotFoundError):
        await handle_v3_assurance_callback(_update(query), _context(FakeBot(calls)), repo_root=tmp_path)
    assert [call[0] for call in calls] == ["answer"]

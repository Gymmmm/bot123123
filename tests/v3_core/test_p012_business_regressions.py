import asyncio
import json
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from PIL import Image
from v3_core.user_bot.search_query import parse_budget_range
from v3_core.user_bot.admin_notifications import AdminNotification, TelegramAdminNotifier
from v3_core.user_bot.telegram_callback_handler import _detail_grid, _single_detail_keyboard
from v3_core.media.editorial_cover import _fit, CREAM


@pytest.mark.parametrize("text,expected", [
    ("11月15号入住，预算800，两房", (600,1000)),
    ("两房，面积100平，预算800", (600,1000)),
    ("2026-11-15入住，预算800以内", (None,800)),
    ("BKK1 100平 19层 500-800美元", (500,800)),
    ("11月15号入住，100平，19层", (None,None)),
    ("预算1,000-1,200美元", (1000,1200)),
])
def test_search_budget_excludes_nonmoney_quantities(text, expected):
    assert parse_budget_range(text) == expected


@pytest.mark.parametrize("size", [(1200,500), (500,1200), (900,900)])
def test_cover_contains_complete_image(size):
    photo = Image.new("RGB", size, "red")
    result = _fit(photo, (700,1350))
    assert result.size == (700,1350)
    # Complete foreground is centered on a uniform frame, no blurred/cropped fill.
    assert result.getpixel((0,0)) == CREAM
    assert result.getpixel((350,675)) == (255,0,0)


@pytest.mark.parametrize("count", [1,2,3,4,6,7,8,9])
def test_details_always_use_4x5_canvas(tmp_path, count):
    paths = []
    for i in range(count):
        p = tmp_path / f"{i}.png"
        Image.new("RGB", (350 + i*30,700), (i*20,50,100)).save(p)
        paths.append(str(p))
    for page in range((count+3)//4):
        with Image.open(BytesIO(_detail_grid(tuple(paths), page))) as frame:
            assert frame.size == (1080,1350)


def test_detail_controls_use_album_instead_of_download_tree():
    kb = _single_detail_keyboard(None, "QL-RF-A2B3", 0, 9)
    callbacks = [b.callback_data for row in kb.inline_keyboard for b in row]
    assert "v3u:listing:photos:QL-RF-A2B3" in callbacks
    assert not any("detailfull" in c or "detaildownload" in c for c in callbacks)


@pytest.mark.asyncio
async def test_notification_survives_restart_and_retries_only_unsent_attachment(tmp_path):
    outbox = tmp_path / "outbox"
    bot = SimpleNamespace(send_message=AsyncMock(), send_photo=AsyncMock(side_effect=RuntimeError("offline")), send_video=AsyncMock())
    notifier = TelegramAdminNotifier([10], outbox_dir=outbox)
    note = AdminNotification("住房问题", ("用户：测试",), media=(("photo","PHOTO"),("video","VIDEO")))
    result = await notifier.send(bot, note)
    assert result.failed_admin_ids == (10,)
    path, = outbox.glob("*.json")
    state = json.loads(path.read_text())
    assert not state["text_pending"] and len(state["media"]) == 2
    state["next_attempt"] = 0
    path.write_text(json.dumps(state))
    bot.send_photo.side_effect = None
    after_restart = TelegramAdminNotifier([10], outbox_dir=outbox)
    await after_restart.retry_pending(bot)
    assert bot.send_message.await_count == 1
    bot.send_video.assert_awaited_once_with(chat_id=10, video="VIDEO")
    assert not list(outbox.glob("*.json"))

@pytest.mark.asyncio
async def test_native_album_keeps_controls_on_original_card_and_can_be_reopened(tmp_path, monkeypatch):
    from v3_core.user_bot import telegram_callback_handler as handler
    from v3_core.user_bot import telegram_photos_render as albums
    from v3_core.user_bot.telegram_callback_response import TelegramCallbackResponse
    public_id = "QL-RF-A2B3"
    p = tmp_path / "photo.jpg"
    Image.new("RGB", (900,600), "blue").save(p)
    details = SimpleNamespace(photo_paths=(str(p), str(p)))
    router = SimpleNamespace(listings=SimpleNamespace(resolve_action=lambda *a: SimpleNamespace(ok=True, details=details)))
    response = TelegramCallbackResponse(kind="photos", status="ok")
    dispatched = SimpleNamespace(callback=SimpleNamespace(public_listing_id=public_id))
    monkeypatch.setattr(handler, "_dispatch", lambda *a: dispatched)
    monkeypatch.setattr(handler, "adapt_callback_response", lambda *a: response)
    sent = AsyncMock(side_effect=[[71,72],[81,82]])
    monkeypatch.setattr(albums, "send_listing_photos_album", sent)
    query = SimpleNamespace(data="v3u:listing:photos:"+public_id, answer=AsyncMock(), message=SimpleNamespace(chat_id=1, message_id=5))
    update = SimpleNamespace(callback_query=query, effective_chat=SimpleNamespace(id=1), effective_user=SimpleNamespace(id=123))
    context = SimpleNamespace(user_data={}, bot=SimpleNamespace(delete_message=AsyncMock()))
    await handler.handle_v3_callback(update,context,router=router)
    await handler.handle_v3_callback(update,context,router=router)
    assert sent.await_count == 2
    assert all(call.kwargs["expand_only"] is True for call in sent.await_args_list)
    assert [c.kwargs["message_id"] for c in context.bot.delete_message.await_args_list] == [71,72]


def test_startup_has_no_automatic_cover_acceptance_job(monkeypatch):
    from v3_core.publishing import publisher_app as module
    class Queue:
        names = []
        def run_repeating(self, callback, **kw): self.names.append(kw["name"])
        def run_once(self, callback, **kw): self.names.append(kw["name"])
    queue = Queue()
    app = SimpleNamespace(job_queue=queue, add_handler=lambda *a,**k:None)
    monkeypatch.setattr(module.PublisherAdminBot, "build_application", lambda self: app)
    controller = object.__new__(module.V3PublisherApplication)
    controller.autopilot = SimpleNamespace(scheduled_tick=lambda:None)
    controller.broadcast = SimpleNamespace(scheduled_tick=lambda:None)
    controller.manual_status_sync = SimpleNamespace(scheduled_tick=lambda:None)
    controller.build_application()
    assert set(queue.names) == {"v3_auto_publish_tick", "v3_broadcast_tick", "v3_status_sync_tick"}

def test_source_contact_is_detected_without_repainting_the_photo(tmp_path):
    from PIL import ImageDraw,ImageFont
    from v3_core.media.source_overlay import detect_source_overlay
    im = Image.new("RGB", (1200,800), "white")
    font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",48)
    ImageDraw.Draw(im).text((40,650),"WhatsApp +855 123456789",font=font,fill="black")
    path = tmp_path/"contact.png"
    im.save(path)
    assert detect_source_overlay(path)["blocked"] is True
    with Image.open(path) as original:
        assert original.size == (1200,800)

def test_gallery_copy_preserves_original_size_pixels_and_contains_no_new_mark(tmp_path):
    from v3_core.media.service import MediaPreparationService
    source = tmp_path/"original.png"
    Image.new("RGB", (331,997), (43,80,126)).save(source)
    service = MediaPreparationService(SimpleNamespace(db_path=str(tmp_path/"unused.db")), prepared_dir=tmp_path/"prepared")
    files = service._branded_gallery(source_post_id=1, paths=[str(source)], cover_style="premium_photo", cover_source_path=source)
    assert len(files) == 1
    with Image.open(files[0]) as actual, Image.open(source) as original:
        assert actual.size == original.size
        assert actual.tobytes() == original.tobytes()

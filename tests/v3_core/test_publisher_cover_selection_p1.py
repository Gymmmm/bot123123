from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image

from v3_core.media.cover_styles import cover_template_path
from v3_core.publishing.publisher_adviser_ui import PublisherAdviserAdminController
from v3_core.publishing.simple_admin import NEW_LISTING_STATE_KEY


class _Message:
    def __init__(self):
        self.calls = []

    async def reply_text(self, text, **kwargs):
        self.calls.append({"kind": "text", "text": text, **kwargs})

    async def reply_photo(self, photo, **kwargs):
        self.calls.append({"kind": "photo", "path": str(Path(photo.name).resolve()), **kwargs})


class _Repo:
    def __init__(self):
        self.status = "pending"
        self.items = []

    def set_listing_status(self, listing_id, status):
        self.status = status

    def mark_listing_pending_for_auto_publish(self, listing_id):
        self.status = "pending"

    def set_item(self, offer_id, **kwargs):
        self.items.append((offer_id, kwargs))


class _Workflow:
    def __init__(self, media):
        self.media_value = media
        self.build_calls = []
        self.review = {"review_id": "REV_" + "1" * 32, "review_status": "approved"}
        self.listing = {"listing_id": "l_cover", "inventory_status": "pending", "layout": "1房1厅"}
        self.offer = {"offer_id": "OFF_" + "2" * 32, "listing_id": "l_cover"}

    def review_detail(self, review_id):
        return SimpleNamespace(review=dict(self.review), listing=dict(self.listing), offer=dict(self.offer), canonical={"facts": {}})

    def review_media(self, *, review_id, manual_cover_path=None):
        return self.media_value

    def package(self, package_id):
        return SimpleNamespace(package_id=package_id, listing_id="l_cover", offer_id=self.offer["offer_id"])

    def build_package_for_review(self, **kwargs):
        self.build_calls.append(kwargs)
        index = len(self.build_calls)
        return SimpleNamespace(
            package_id=f"PKG3_{index:032x}",
            listing_id="l_cover",
            offer_id=self.offer["offer_id"],
            cover_path=kwargs["manual_cover_path"],
            post_text="preview",
            status="package_ready",
        )


def _image(path: Path, value: int):
    Image.new("RGB", (1000, 750), (value, 120, 180)).save(path)


def test_approved_cover_template_still_exists():
    # Legacy cover-generator variants are retired; current channel publishing
    # uses CoverRenderService -> cover_renderer -> editorial_cover (Pillow).
    assert cover_template_path("premium_photo").is_file()


@pytest.mark.asyncio
async def test_manual_cover_next_cycles_real_photos_and_preserves_template(tmp_path: Path):
    candidates = []
    for index, value in enumerate((40, 100, 180)):
        path = tmp_path / f"photo_{index}.jpg"
        _image(path, value)
        candidates.append(str(path.resolve()))
    media = SimpleNamespace(
        cover_source_path=candidates[0],
        gallery_paths=tuple(candidates),
        ranking=tuple({"file": path, "reject": False} for path in candidates),
    )
    workflow = _Workflow(media)
    controller = object.__new__(PublisherAdviserAdminController)
    controller.workflow = workflow
    controller.repository = _Repo()
    controller.channel_chat_id = "-100123"
    controller.home_row = lambda: []

    async def no_blockers(detail):
        return [], media

    controller._manual_blockers = no_blockers
    state = {
        "review_id": workflow.review["review_id"],
        "offer_id": workflow.offer["offer_id"],
        "listing_id": "l_cover",
        "package_id": "PKG3_" + "a" * 32,
        "mode": "preview",
    }
    context = SimpleNamespace(user_data={NEW_LISTING_STATE_KEY: state}, bot=object())

    preview = _Message()
    await controller.prepare_manual_preview(preview, context, review_id=workflow.review["review_id"], offer_id=workflow.offer["offer_id"])
    assert workflow.build_calls[-1]["manual_cover_path"] == candidates[0]

    # Preview-first UX: one tap advances the hero and immediately rebuilds preview.
    update = SimpleNamespace(callback_query=SimpleNamespace(data="v3smp|manual_cover_next", message=_Message()))
    assert await controller.handle_callback(update, context) is True
    assert workflow.build_calls[-1]["manual_cover_path"] == candidates[1]

    update = SimpleNamespace(callback_query=SimpleNamespace(data="v3smp|manual_cover_next", message=_Message()))
    assert await controller.handle_callback(update, context) is True
    assert workflow.build_calls[-1]["manual_cover_path"] == candidates[2]

    # Operator menu only exposes current styles; legacy render keys stay backend-compatible.
    menu_message = _Message()
    update = SimpleNamespace(callback_query=SimpleNamespace(data="v3smp|manual_templates", message=menu_message))
    assert await controller.handle_callback(update, context) is True
    callbacks = [
        b.callback_data
        for row in menu_message.calls[-1]["reply_markup"].inline_keyboard
        for b in row
        if getattr(b, "callback_data", None)
    ]
    assert "v3smp|manual_style|premium_photo" in callbacks
    assert "v3smp|manual_style|right_price" not in callbacks
    assert "v3smp|manual_style|black_gold" not in callbacks
    assert "v3smp|manual_style|classic_blue" not in callbacks
    assert "v3smp|manual_style|black_gold" not in callbacks
    assert "v3smp|manual_style|classic_blue" not in callbacks

    # Switching current template keeps the selected hero.
    update = SimpleNamespace(callback_query=SimpleNamespace(data="v3smp|manual_style|right_price", message=_Message()))
    assert await controller.handle_callback(update, context) is True
    assert workflow.build_calls[-1]["manual_cover_path"] == candidates[2]
    assert workflow.build_calls[-1]["cover_style"] == "right_price"
    assert context.user_data[NEW_LISTING_STATE_KEY]["cover_style"] == "right_price"

    await controller.prepare_manual_preview(_Message(), context, review_id=workflow.review["review_id"], offer_id=workflow.offer["offer_id"])
    assert workflow.build_calls[-1]["manual_cover_path"] == candidates[2]
    assert workflow.build_calls[-1]["cover_style"] == "right_price"


def test_cover_related_callbacks_stay_within_telegram_contract():
    callbacks = [
        "v3smp|manual_cover_next",
        "v3smp|manual_templates",
        "v3smp|manual_style|classic_blue",
        "v3smp|manual_style|right_price",
        "v3smp|manual_style|black_gold",
        "v3smp|manual_style|premium_photo",
        "v3smp|manual_cover_pick|999",
    ]
    assert all(len(value.encode("utf-8")) <= 64 for value in callbacks)

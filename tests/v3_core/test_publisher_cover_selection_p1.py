from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image, ImageFont

from v3_core.media.cover_generator import CoverRenderData, generate_cover
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


def test_cover_styles_map_to_distinct_template_paths_and_render_differently(tmp_path: Path, monkeypatch):
    paths = [cover_template_path(key, allow_video=False).resolve() for key in ("classic_blue", "right_price", "black_gold", "premium_photo")]
    assert len(set(paths)) == 4
    assert all(path.is_file() for path in paths)

    monkeypatch.setattr("v3_core.media.cover_generator._font", lambda size, bold=False: ImageFont.load_default(size=size))

    source = tmp_path / "source.jpg"
    _image(source, 80)
    data = CoverRenderData(public_listing_id="QL-TEST", project="富力城", layout="1房1厅", area="金边", price="400")
    outputs = []
    for style in ("classic_blue", "right_price", "black_gold", "premium_photo"):
        output = tmp_path / f"{style}.jpg"
        generate_cover(style=style, source_image=str(source), output_path=str(output), data=data)
        outputs.append(output)
    assert len({sha256(path.read_bytes()).hexdigest() for path in outputs}) == 4


@pytest.mark.asyncio
async def test_manual_cover_picker_selects_real_photo_and_preserves_it_across_template_change(tmp_path: Path):
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

    # Default preview uses candidate 0.
    preview = _Message()
    await controller.prepare_manual_preview(preview, context, review_id=workflow.review["review_id"], offer_id=workflow.offer["offer_id"])
    assert workflow.build_calls[-1]["manual_cover_path"] == candidates[0]

    # Picker shows every real candidate and uses short callbacks.
    picker_message = _Message()
    update = SimpleNamespace(callback_query=SimpleNamespace(data="v3smp|manual_cover", message=picker_message))
    assert await controller.handle_callback(update, context) is True
    photo_calls = [call for call in picker_message.calls if call["kind"] == "photo"]
    assert [call["path"] for call in photo_calls] == candidates
    callbacks = [call["reply_markup"].inline_keyboard[0][0].callback_data for call in photo_calls]
    assert callbacks == ["v3smp|manual_cover_pick|0", "v3smp|manual_cover_pick|1", "v3smp|manual_cover_pick|2"]
    assert all(len(value.encode("utf-8")) <= 64 for value in callbacks)

    # Select photo 1.
    update = SimpleNamespace(callback_query=SimpleNamespace(data="v3smp|manual_cover_pick|1", message=_Message()))
    assert await controller.handle_callback(update, context) is True
    assert workflow.build_calls[-1]["manual_cover_path"] == candidates[1]

    # Select photo 2.
    update = SimpleNamespace(callback_query=SimpleNamespace(data="v3smp|manual_cover_pick|2", message=_Message()))
    assert await controller.handle_callback(update, context) is True
    assert workflow.build_calls[-1]["manual_cover_path"] == candidates[2]

    # Switch template: selected photo 2 survives and the style is persisted.
    update = SimpleNamespace(callback_query=SimpleNamespace(data="v3smp|manual_style|black_gold", message=_Message()))
    assert await controller.handle_callback(update, context) is True
    assert workflow.build_calls[-1]["manual_cover_path"] == candidates[2]
    assert workflow.build_calls[-1]["cover_style"] == "black_gold"
    assert context.user_data[NEW_LISTING_STATE_KEY]["cover_style"] == "black_gold"

    # Regenerate without an explicit style: session style and selected photo still win.
    await controller.prepare_manual_preview(_Message(), context, review_id=workflow.review["review_id"], offer_id=workflow.offer["offer_id"])
    assert workflow.build_calls[-1]["manual_cover_path"] == candidates[2]
    assert workflow.build_calls[-1]["cover_style"] == "black_gold"


def test_cover_related_callbacks_stay_within_telegram_contract():
    callbacks = [
        "v3smp|manual_cover",
        "v3smp|manual_templates",
        "v3smp|manual_style|classic_blue",
        "v3smp|manual_style|right_price",
        "v3smp|manual_style|black_gold",
        "v3smp|manual_cover_pick|999",
    ]
    assert all(len(value.encode("utf-8")) <= 64 for value in callbacks)

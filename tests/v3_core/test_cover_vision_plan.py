"""Gym spec 2026-10-11 §九: vision-based cover planning regression + degradation.

7117 (公寓) and 6735 (独栋别墅) use real photos (downscaled copies) and RECORDED
vision fixtures (manual visual inspection, clearly marked in the JSON). The
network provider is exercised with a fake transport only.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
from PIL import Image, ImageFilter

from v3_core.media import cover_plan as cp
from v3_core.media.cover_plan import cross_validate, plan_cover
from v3_core.media.cover_renderer import CoverRenderData, render_cover
from v3_core.media.media_selection import select_publication_media
from v3_core.media.service import MediaPreparationService
from v3_core.media.straighten import correct_file
from v3_core.media.vision import (
    FixtureVisionClient,
    HeuristicVisionClient,
    OpenAICompatibleVisionClient,
    PhotoVision,
    analyze_photos,
    normalize_vision,
    vision_client_from_env,
)

FIXTURES = Path(__file__).parent / "fixtures" / "cover_vision"


def _plan(listing: str, property_type: str, client=None, *, manual=None, photos=None):
    paths = photos or sorted((FIXTURES / "photos" / listing).glob("*.jpg"))
    selected = select_publication_media(paths, cover_preference="living" if "公寓" in property_type else "exterior")
    client = client or FixtureVisionClient(FIXTURES)
    vision = analyze_photos(selected["gallery_paths"], client, ranking=selected["ranking"], use_cache=False)
    plan = plan_cover(
        gallery=selected["gallery_paths"], ranking=selected["ranking"], vision=vision,
        property_type=property_type, manual_cover=manual,
        correction_probe=lambda p, m: correct_file(p, Path(p).with_name(Path(p).stem + f"_{m}_probe.jpg"), mode=m),
    )
    return plan, {i.path: i for i in plan.photos}


def _name(path: str) -> str:
    return Path(path).stem


@pytest.fixture()
def photos_copy(tmp_path):
    """Copies so probes never write next to the committed fixtures."""
    def _copy(listing: str) -> list[Path]:
        out = []
        for src in sorted((FIXTURES / "photos" / listing).glob("*.jpg")):
            dst = tmp_path / listing / src.name
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_bytes(src.read_bytes())
            out.append(dst)
        return out
    return _copy


# ---------------------------------------------------------------- 7117 apartment
def test_7117_apartment_regression(photos_copy):
    plan, items = _plan("7117", "公寓", photos=photos_copy("7117"))
    rooms = {_name(p): i.room_type for p, i in items.items()}
    # 客厅 ≠ 厨房 (01 shows the open kitchen behind the sofa), 卧室 ≠ 卫生间.
    assert rooms["01"] == rooms["02"] == rooms["03"] == "客厅"
    assert rooms["04"] == "厨房"
    assert {rooms[k] for k in ("05", "06", "07", "08")} == {"卧室"}
    assert rooms["09"] == "卫生间"
    # Default hero = the high-quality living room, 3:2 apartment template.
    assert plan.template == "apartment_3x2"
    assert _name(plan.hero) == "01" and "客厅" in plan.hero_reason
    # Thumbs cover different rooms: 主卧 → 厨房 → 卫生间 with labels shown.
    assert [_name(p) for p in plan.thumbs] == ["05", "04", "09"]
    assert plan.labels == ["卧室", "厨房", "卫生间"]
    assert plan.first_batch == [plan.hero, *plan.thumbs]
    assert plan.low_quality is False


def test_7117_first_four_bot_photos_match_the_cover(tmp_path, photos_copy):
    from v3_core.user_bot.listing_responses import build_details_response
    from v3_core.user_bot.public_inventory import PublishedListingView

    paths = [str(p) for p in photos_copy("7117")]

    class Reader:
        db_path = str(tmp_path / "db" / "x.db")

        def source_image_paths(self, _pid):
            return paths

        def source_identity(self, pid):
            return {"source_post_id": int(pid)}

    service = MediaPreparationService(Reader(), prepared_dir=tmp_path / "prepared",
                                      vision_client=FixtureVisionClient(FIXTURES))
    media = service.prepare(source_post_id=7117, property_hints=("公寓",))
    plan = media.source_identity["cover_plan"]
    assert media.cover_template == "apartment_3x2"
    assert plan["first_batch"] == [media.cover_source_path, *media.cover_gallery_paths[:3]]
    assert list(media.cover_gallery_labels[:3]) == ["卧室", "厨房", "卫生间"]
    # Bot gallery = same 4 photos, same order (hero lightly corrected, small logo, uncropped).
    expected = service._branded_gallery(
        source_post_id=7117, paths=[media.cover_render_source, *media.cover_gallery_paths[:3]],
        cover_source_path=media.cover_source_path,
    )
    assert list(media.gallery_paths[:4]) == expected == plan["bot_first_batch"]
    # The bot detail page shows exactly these first 4 standalone photos.
    view = PublishedListingView(
        listing={"listing_id": "L", "public_listing_id": "QL-RF-A2B3", "inventory_status": "active"},
        offer={"offer_id": "O", "offer_type": "rent", "offer_status": "active", "publication_policy": "telegram_rent"},
        publication={"instance_id": "P"},
        package={"snapshot_json": json.dumps({"schema": "v3_publication_snapshot.v1", "listing_id": "L",
                                              "public_listing_id": "QL-RF-A2B3",
                                              "listing": {"project_name": "太子国际广场", "property_type": "公寓"},
                                              "offer": {"offer_type": "rent", "monthly_rent_usd": 800,
                                                        "publication_policy": "telegram_rent"}}),
                 "gallery_json": json.dumps(list(media.gallery_paths)), "cover_path": ""},
    )
    details = build_details_response(view)
    assert list(details.media_groups[0]) == expected
    labels = [a.label for row in details.action_rows for a in row]
    assert labels == ["📸 下一组实拍", "📅 预约看房", "💬 中文顾问"]


# ---------------------------------------------------------------- 6735 villa
def test_6735_villa_regression(photos_copy):
    plan, items = _plan("6735", "独栋别墅", photos=photos_copy("6735"))
    rooms = {_name(p): i.room_type for p, i in items.items()}
    assert rooms["01"] == rooms["02"] == "外观"          # 外观 ≠ 卧室
    assert rooms["07"] == "卧室"                          # bed + toilet through a door → 卧室, not 卫生间
    # Multiple exteriors: the complete, less obstructed one wins.
    assert plan.template == "villa_4x5"
    assert _name(plan.hero) == "01"
    assert items[plan.hero].obstruction < [i for p, i in items.items() if _name(p) == "02"][0].obstruction
    assert plan.hero_correction.get("status") == "corrected"
    assert plan.labels[0] == "客厅" and "卧室" in plan.labels and "外观" not in plan.labels


def test_6735_bad_exterior_switches_hero_and_template(photos_copy):
    photos = [p for p in photos_copy("6735") if p.stem != "01"]  # only the tree-blocked side view left
    plan, items = _plan("6735", "独栋别墅", photos=photos)
    assert plan.template == "landscape_3x2"
    assert items[plan.hero].room_type == "客厅"
    assert "外观不合格" in plan.hero_reason


def test_villa_perspective_correction_does_not_stretch_the_building(photos_copy, tmp_path):
    src = [p for p in photos_copy("6735") if p.stem == "01"][0]
    report = correct_file(src, tmp_path / "out.jpg", mode="exterior")
    assert report["status"] == "corrected"
    assert report["aspect_change"] <= 0.10          # centre of the building: <10 % wider/thinner
    assert report["edge_aspect_change"] <= 0.25
    assert report["crop_kept"] >= 0.7
    assert report["lean_after_deg"] < report["lean_before_deg"]


def test_correction_reverts_or_refuses_outside_bounds(tmp_path, monkeypatch):
    from v3_core.media import straighten

    monkeypatch.setattr(straighten, "MAX_ASPECT_CHANGE", 0.001)
    monkeypatch.setattr(straighten, "MAX_EDGE_ASPECT_CHANGE", 0.001)
    src = FIXTURES / "photos" / "6735" / "01.jpg"
    report = straighten.correct_file(src, tmp_path / "x.jpg", mode="exterior")
    assert report["status"] == "reverted:aspect_change" and "path" not in report
    small = tmp_path / "small.jpg"
    Image.open(src).resize((400, 252)).save(small)
    assert straighten.correct_file(small, tmp_path / "y.jpg")["status"] == "refused:low_resolution"


# ---------------------------------------------------------------- cross-validation
@pytest.mark.parametrize(
    "model_room,objects,expected",
    [
        ("厨房", ["沙发", "茶几", "橱柜"], "客厅"),     # never 客厅 → 厨房
        ("卫生间", ["床", "马桶"], "卧室"),              # never 卧室 → 卫生间
        ("卧室", ["立面", "屋顶"], "外观"),              # never 外观 → 卧室
        ("客厅", ["灶台", "水槽"], "厨房"),
        ("其他", ["马桶", "淋浴"], "卫生间"),
    ],
)
def test_evidence_rules(model_room, objects, expected):
    room, confidence, notes = cross_validate(normalize_vision(
        {"room_type": model_room, "objects": objects, "confidence": 0.95}))
    assert room == expected
    assert notes


def test_label_without_evidence_is_hidden():
    room, confidence, _ = cross_validate(normalize_vision({"room_type": "卧室", "confidence": 0.99}))
    assert room == "卧室" and confidence < cp.LABEL_MIN_CONFIDENCE


def test_unknown_answers_degrade_to_other():
    vision = normalize_vision({"room_type": "宇宙飞船", "property_type_hint": "城堡", "confidence": 3})
    assert vision.room_type == "其他" and vision.property_type_hint == "无法确认"
    assert vision.confidence == pytest.approx(0.03)


# ---------------------------------------------------------------- counts / degrade
def _synthetic(tmp_path, n, *, blur=False, dark=False):
    paths = []
    rng = np.random.default_rng(7)
    for i in range(n):
        base = rng.integers(0, 255, (60, 80, 3), dtype=np.uint8)
        image = Image.fromarray(base).resize((1200, 900), Image.Resampling.NEAREST)
        if blur:
            image = image.filter(ImageFilter.GaussianBlur(12))
        if dark:
            image = image.point(lambda v: v // 6)
        path = tmp_path / f"p{i}.jpg"
        image.save(path, quality=90)
        paths.append(path)
    return paths


class _Uniform:
    """Every photo: confident 客厅/卧室 alternating, given hero score."""

    name = "fixture"

    def __init__(self, hero=0.8, confidence=0.9, quality=0.9):
        self.hero, self.confidence, self.quality, self.calls = hero, confidence, quality, 0

    def analyze(self, path, *, ranking_row=None):
        self.calls += 1
        room, objects = (("客厅", ["沙发"]) if self.calls % 2 else ("卧室", ["床"]))
        return normalize_vision({"room_type": room, "objects": objects, "confidence": self.confidence,
                                 "hero_score": self.hero, "quality": {"sharpness": self.quality, "brightness": self.quality,
                                                                      "composition": self.quality,
                                                                      "completeness": self.quality}},
                                provider="fixture")


@pytest.mark.parametrize("count,template,thumbs", [(1, "single", 0), (3, "apartment_3x2", 2),
                                                   (4, "apartment_3x2", 3), (6, "apartment_3x2", 3),
                                                   (9, "apartment_3x2", 3)])
def test_photo_counts(tmp_path, count, template, thumbs):
    plan, _ = _plan("", "公寓", _Uniform(), photos=_synthetic(tmp_path, count))
    assert plan.template == template
    assert len(plan.thumbs) == thumbs                      # never padded with repeats
    assert len(set([plan.hero, *plan.thumbs])) == 1 + thumbs


def test_no_standout_hero_uses_four_grid(tmp_path):
    # Mediocre, similar shots with uncertain rooms: nothing deserves the big hero slot.
    plan, _ = _plan("", "公寓", _Uniform(hero=0.3, confidence=0.3, quality=0.45), photos=_synthetic(tmp_path, 5))
    assert plan.template == "grid_2x2"
    assert all(label == "" for label in plan.labels)       # low confidence → no labels


def test_all_low_quality_still_renders_and_is_flagged(tmp_path):
    photos = _synthetic(tmp_path, 4, blur=True, dark=True)
    plan, _ = _plan("", "公寓", HeuristicVisionClient(), photos=photos)
    assert plan.low_quality is True and "图片质量较低" in plan.notes
    out = render_cover(style="x", source_image=plan.hero, source_images=plan.thumbs, source_labels=plan.labels,
                       output_path=str(tmp_path / "c.jpg"), layout=plan.template,
                       data=CoverRenderData(public_listing_id="QL", property_type="公寓", price="500"))
    assert Path(out).is_file()


def test_duplicates_never_appear_twice_on_the_cover(tmp_path):
    src = sorted((FIXTURES / "photos" / "7117").glob("*.jpg"))
    dup = tmp_path / "01_copy.jpg"
    Image.open(src[0]).resize((990, 623)).save(dup, quality=70)   # same shot, re-encoded
    plan, items = _plan("7117", "公寓", photos=[*src, dup])
    used = [plan.hero, *plan.thumbs]
    assert len({items[p].cluster for p in used}) == len(used)


def test_blurry_photo_is_not_the_hero(tmp_path):
    src = sorted((FIXTURES / "photos" / "7117").glob("*.jpg"))
    blurred = tmp_path / "01.jpg"
    Image.open(src[0]).filter(ImageFilter.GaussianBlur(9)).save(blurred)
    plan, items = _plan("7117", "公寓", photos=[blurred, *src[1:]])
    assert plan.hero != str(blurred.resolve())
    assert items[plan.hero].room_type == "客厅"


def test_heuristic_provider_hides_every_label(photos_copy):
    plan, _ = _plan("7117", "公寓", HeuristicVisionClient(), photos=photos_copy("7117"))
    assert plan.labels == ["", "", ""]
    assert "不确定" in plan.hero_reason


def test_manual_cover_is_optional_override(photos_copy):
    photos = photos_copy("7117")
    auto, _ = _plan("7117", "公寓", photos=photos, manual=str(photos[0]))
    assert "手动" not in auto.hero_reason                # manual == auto choice → still automatic
    manual, _ = _plan("7117", "公寓", photos=photos, manual=str(photos[4]))
    assert _name(manual.hero) == "05" and "手动" in manual.hero_reason


# ---------------------------------------------------------------- providers
def test_env_selects_provider_without_keys_in_code():
    assert vision_client_from_env({}).name == "heuristic"
    assert vision_client_from_env({"V3_VISION_PROVIDER": "openai_compat"}).name == "heuristic"  # no key → safe
    client = vision_client_from_env({"V3_VISION_API_KEY": "k", "V3_VISION_MODEL": "m",
                                     "V3_VISION_BASE_URL": "https://api.example.com/v1"})
    assert client.name == "openai_compat" and client.base_url == "https://api.example.com/v1"
    assert vision_client_from_env({"V3_VISION_PROVIDER": "fixture", "V3_VISION_FIXTURE_DIR": str(FIXTURES)}).name == "fixture"


def test_openai_compatible_client_parses_and_falls_back():
    path = str(FIXTURES / "photos" / "7117" / "04.jpg")
    seen = {}

    def transport(url, payload):
        seen["url"], seen["payload"] = url, payload
        return {"choices": [{"message": {"content": "```json\n" + json.dumps(
            {"room_type": "厨房", "objects": ["灶台", "水槽"], "confidence": 0.93}, ensure_ascii=False) + "\n```"}}]}

    client = OpenAICompatibleVisionClient(api_key="k", model="m", base_url="https://x/v1", transport=transport)
    result = client.analyze(path)
    assert result.room_type == "厨房" and result.provider == "openai_compat:m"
    assert seen["url"] == "https://x/v1/chat/completions"
    image = seen["payload"]["messages"][1]["content"][1]["image_url"]["url"]
    assert image.startswith("data:image/jpeg;base64,")

    def broken(url, payload):
        raise TimeoutError

    degraded = OpenAICompatibleVisionClient(api_key="k", model="m", transport=broken).analyze(path)
    assert degraded.provider == "heuristic" and degraded.confidence < cp.LABEL_MIN_CONFIDENCE


def test_render_grid_and_single_templates(tmp_path):
    photos = [str(p) for p in sorted((FIXTURES / "photos" / "7117").glob("*.jpg"))[:4]]
    data = CoverRenderData(public_listing_id="QL", project="太子国际广场", property_type="公寓", layout="3房1厅", price="800")
    grid = render_cover(style="x", source_image=photos[0], source_images=photos[1:], source_labels=("", "厨房", ""),
                        output_path=str(tmp_path / "g.jpg"), data=data, layout="grid_2x2")
    single = render_cover(style="x", source_image=photos[0], source_images=photos[1:],
                          output_path=str(tmp_path / "s.jpg"), data=data, layout="single")
    with Image.open(grid) as g, Image.open(single) as s:
        assert g.size == (1200, 800) and s.size == (1200, 800)
        assert min(g.convert("RGB").getpixel((600, 200))) >= 245   # white separator between columns
        assert min(s.convert("RGB").getpixel((900, 300))) < 245     # single: no column split


def test_next_batch_continues_after_the_first_four(tmp_path):
    from v3_core.user_bot.callbacks import encode_semantic_action, parse_callback
    from v3_core.user_bot.listing_responses import build_details_response, build_photos_response
    from v3_core.user_bot.public_inventory import PublishedListingView

    gallery = []
    for i in range(7):
        path = tmp_path / f"g{i}.jpg"
        Image.new("RGB", (64, 48), (30 * i, 90, 140)).save(path)
        gallery.append(str(path))
    snapshot = {"schema": "v3_publication_snapshot.v1", "listing_id": "L", "public_listing_id": "QL-RF-A2B3",
                "listing": {"project_name": "BKK1", "property_type": "独栋别墅"},
                "offer": {"offer_type": "rent", "monthly_rent_usd": 4000, "publication_policy": "telegram_rent"}}
    view = PublishedListingView(
        listing={"listing_id": "L", "public_listing_id": "QL-RF-A2B3", "inventory_status": "active"},
        offer={"offer_id": "O", "offer_type": "rent", "offer_status": "active", "publication_policy": "telegram_rent"},
        publication={"instance_id": "P"},
        package={"snapshot_json": json.dumps(snapshot), "gallery_json": json.dumps(gallery), "cover_path": ""},
    )
    details = build_details_response(view)
    assert list(details.media_groups[0]) == gallery[:4]
    action = details.action_rows[0][0]
    assert action.label == "📸 下一组实拍"
    callback = parse_callback(encode_semantic_action(action))
    assert callback.action == "photos" and callback.page_index is None
    more = build_photos_response(view, offset=int(callback.target_index))
    assert list(more.media_groups[0]) == gallery[4:]


def test_real_provider_results_are_cached_per_photo(tmp_path):
    photos = []
    for i, src in enumerate(sorted((FIXTURES / "photos" / "7117").glob("*.jpg"))[:3]):
        dst = tmp_path / src.name
        dst.write_bytes(src.read_bytes())
        photos.append(str(dst))
    calls = []

    def transport(url, payload):
        calls.append(url)
        return {"choices": [{"message": {"content": json.dumps({"room_type": "客厅", "objects": ["沙发"], "confidence": 0.9})}}]}

    client = OpenAICompatibleVisionClient(api_key="k", model="m", transport=transport)
    first = analyze_photos(photos, client)
    second = analyze_photos(photos, client)
    assert len(calls) == 3                      # parallel first pass, cached second pass
    assert list(first) == list(second) == [str(Path(p).resolve()) for p in photos]
    assert all(v.room_type == "客厅" for v in second.values())

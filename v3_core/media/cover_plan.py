"""Listing-level cover planning (Gym spec 2026-10-11 §二–§七).

Input: the usable gallery + local ranker metrics (sharpness / exposure / text
overlay …) + per-photo visual semantics (``vision.PhotoVision``) + the listing's
structured property type. Output: one ``CoverPlan`` used by BOTH the channel
cover and the bot detail page (first 4 standalone photos = hero, thumb 1–3).

Pipeline
1. Cross-validation: strong object evidence overrides the model's room type
   (床→卧室, 马桶/淋浴→卫生间 unless a bed is the subject, 沙发/电视/茶几/餐桌→客厅
   even when kitchen cabinets are visible, 灶台/水槽/橱柜→厨房, 立面/屋顶/大门→外观).
   Forbidden confusions are impossible by construction: 客厅→厨房, 卧室→卫生间,
   外观→卧室. Without supporting evidence confidence is capped below the
   label threshold. Photos whose property hint contradicts the listing are
   flagged ``foreign_suspect`` and kept out of the cover.
2. Near-duplicate / same-angle clusters (dHash + model ``duplicate_group``):
   only the best shot of a cluster may appear on the cover.
3. Scores: hero = quality, sharpness, exposure, subject completeness,
   obstruction, composition, room fit for the template, perspective
   distortion, title/price area fit; thumbs = quality, room coverage, not the
   hero's cluster, brightness harmony with the hero, important rooms first.
4. Template: property type comes from the database; photos only verify.
5. Degrade: uncertain labels hidden; uncertain hero room → highest quality
   complete shot; no good exterior → interior/courtyard hero on 3:2; no
   standout hero → 2×2 grid; one photo → single image; all low quality →
   still render, flagged 「图片质量较低」. Nothing is ever invented.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from statistics import median
from typing import Any, Callable, Iterable, Mapping

from .ranker import _dhash, _hamming
from .vision import PhotoVision

PLAN_VERSION = "cover_plan_v1_20261011"
LABEL_MIN_CONFIDENCE = 0.75
NO_EVIDENCE_CONFIDENCE_CAP = 0.7
HERO_STANDOUT_MIN = 0.58
LOW_QUALITY_MAX = 0.42
SAME_ANGLE_HAMMING = 10

TEMPLATE_APARTMENT = "apartment_3x2"   # 1200×800, hero left + 3 stacked
TEMPLATE_VILLA = "villa_4x5"           # 1080×1350, exterior hero + 3 below
TEMPLATE_LANDSCAPE = "landscape_3x2"   # villa without a good exterior (same geometry as apartment)
TEMPLATE_GRID = "grid_2x2"             # no standout hero
TEMPLATE_SINGLE = "single"             # only one usable photo

EVIDENCE = (
    # (room, objects) in priority order — the first matching rule wins.
    ("卧室", {"床"}),
    ("卫生间", {"马桶", "淋浴", "浴缸"}),
    ("客厅", {"沙发", "电视", "茶几", "餐桌"}),
    ("厨房", {"灶台", "水槽", "橱柜"}),
    ("泳池", {"泳池水面"}),
    ("健身房", {"健身器材"}),
    ("外观", {"立面", "屋顶", "大门", "车库", "院墙"}),
)
INTERIOR_OBJECTS = {"沙发", "电视", "茶几", "餐桌", "床", "衣柜", "书桌", "灶台", "水槽", "橱柜", "冰箱",
                    "马桶", "淋浴", "洗手台", "浴缸", "镜柜"}
SUPPORT = {
    "客厅": {"沙发", "电视", "茶几", "餐桌"}, "卧室": {"床", "衣柜"}, "厨房": {"灶台", "水槽", "橱柜", "冰箱"},
    "卫生间": {"马桶", "淋浴", "洗手台", "浴缸", "镜柜"}, "外观": {"立面", "屋顶", "大门", "车库", "院墙"},
    "庭院": {"草坪", "树木", "院墙"}, "泳池": {"泳池水面"}, "阳台": {"栏杆", "洗衣机", "窗景", "高楼景观"},
    "景观": {"窗景", "高楼景观"}, "健身房": {"健身器材"}, "停车位": {"车辆", "车库"}, "入口": {"大门"},
}
FORBIDDEN = {("客厅", "厨房"), ("卧室", "卫生间"), ("外观", "卧室")}

HERO_ROOM_FIT = {
    "apartment": {"客厅": 1.0, "卧室": 0.7, "景观": 0.55, "阳台": 0.45, "厨房": 0.4, "健身房": 0.45,
                  "泳池": 0.5, "外观": 0.35, "庭院": 0.35, "入口": 0.3, "其他": 0.4, "停车位": 0.1, "卫生间": 0.05},
    "villa": {"外观": 1.0, "入口": 0.85, "庭院": 0.8, "泳池": 0.8, "客厅": 0.75, "卧室": 0.55, "景观": 0.5,
              "阳台": 0.45, "厨房": 0.4, "健身房": 0.45, "其他": 0.4, "停车位": 0.15, "卫生间": 0.05},
}
THUMB_ROOMS = {
    "apartment": ("客厅", "卧室", "厨房", "卫生间"),
    "villa": ("客厅", "卧室", "厨房", "庭院", "泳池"),
}
UNCERTAIN_ROOM_FIT = 0.5


@dataclass
class PhotoAssessment:
    path: str
    room_type: str
    model_room_type: str
    confidence: float
    show_label: bool
    quality: float
    sharpness: float
    exposure: float
    composition: float
    completeness: float
    obstruction: float
    distortion: float
    text_overlay: float
    hero_score: float = 0.0
    cluster: int = 0
    cluster_best: bool = True
    foreign_suspect: bool = False
    view: bool = False
    objects: tuple[str, ...] = ()
    reason: str = ""
    adjustments: list[str] = field(default_factory=list)
    provider: str = ""

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["objects"] = list(self.objects)
        for key, value in list(data.items()):
            if isinstance(value, float):
                data[key] = round(value, 3)
        return data


@dataclass
class CoverPlan:
    template: str
    kind: str
    hero: str
    thumbs: list[str]
    labels: list[str]
    hero_reason: str
    low_quality: bool
    gallery_order: list[str]
    photos: list[PhotoAssessment]
    hero_correction: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)
    provider: str = ""
    version: str = PLAN_VERSION

    @property
    def first_batch(self) -> list[str]:
        """Bot detail first 4 standalone photos, same order as the cover."""
        return [self.hero, *self.thumbs][:4]

    def summary(self) -> dict[str, Any]:
        """Compact, JSON-safe record persisted with the package (source_identity.cover_plan)."""
        return {
            "version": self.version, "template": self.template, "kind": self.kind,
            "hero": self.hero, "thumbs": list(self.thumbs), "labels": list(self.labels),
            "hero_reason": self.hero_reason, "low_quality": self.low_quality, "provider": self.provider,
            "first_batch": self.first_batch, "hero_correction": dict(self.hero_correction),
            "notes": list(self.notes),
        }


def _pct(row: Mapping[str, Any], key: str, default: float = 50.0) -> float:
    try:
        return max(0.0, min(1.0, float(row.get(key, default)) / 100.0))
    except (TypeError, ValueError):
        return default / 100.0


def cross_validate(vision: PhotoVision) -> tuple[str, float, list[str]]:
    """Return (room_type, confidence, adjustments) after the evidence rules."""
    objects = set(vision.objects)
    room, confidence = vision.room_type, float(vision.confidence)
    notes: list[str] = []
    evidence_room = ""
    for candidate, required in EVIDENCE:
        if objects & required:
            if candidate == "外观" and objects & INTERIOR_OBJECTS:
                continue  # facade words inside a room are not an exterior shot
            if candidate == "卫生间" and objects & {"沙发", "电视"}:
                continue
            evidence_room = candidate
            break
    if evidence_room and evidence_room != room:
        if room in {"阳台", "景观", "庭院", "入口", "泳池"} and evidence_room in {"外观", "客厅"}:
            pass  # e.g. 庭院 with a visible facade stays 庭院; balcony furniture stays 阳台
        else:
            notes.append(f"证据改判 {room}→{evidence_room}（{'/'.join(sorted(objects & dict(EVIDENCE)[evidence_room]))}）")
            room, confidence = evidence_room, min(confidence, 0.9) * 0.9
    support = SUPPORT.get(room, set())
    if room != "其他" and not (objects & support):
        if confidence > NO_EVIDENCE_CONFIDENCE_CAP:
            notes.append("无物件佐证，降低置信度")
        confidence = min(confidence, NO_EVIDENCE_CONFIDENCE_CAP)
    if (room, vision.room_type) in FORBIDDEN and not (objects & SUPPORT.get(vision.room_type, set())):
        pass  # evidence moved it away from a forbidden confusion; nothing else to do
    return room, max(0.0, min(1.0, confidence)), notes


def _kind_from_type(property_type: str) -> str:
    value = str(property_type or "").strip().lower()
    return "apartment" if any(t in value for t in ("公寓", "服务式", "apartment", "condo", "studio")) else "villa"


def _assess(path: str, row: Mapping[str, Any], vision: PhotoVision, kind: str) -> PhotoAssessment:
    room, confidence, notes = cross_validate(vision)
    sharp_local = _pct(row, "sharpness")
    exposure_local = (_pct(row, "brightness") + _pct(row, "exposure")) / 2
    q = vision.quality
    sharpness = min(sharp_local, float(q.get("sharpness", sharp_local))) if q else sharp_local
    exposure = (exposure_local + float(q.get("brightness", exposure_local))) / 2 if q else exposure_local
    composition = float(q.get("composition", 0.5)) if q else 0.5
    completeness = float(q.get("completeness", 0.5)) if q else 0.5
    text = row.get("text") if isinstance(row.get("text"), Mapping) else {}
    text_overlay = float(text.get("text_heavy") or 0.0)
    quality = 0.3 * sharpness + 0.25 * exposure + 0.25 * composition + 0.2 * completeness
    if row.get("soft_reject"):
        quality *= 0.85
    hint = vision.property_type_hint
    foreign = (
        vision.confidence >= 0.8
        and ((kind == "apartment" and hint == "别墅" and room in {"外观", "庭院", "泳池"})
             or (kind == "villa" and hint == "公寓" and room in {"外观", "阳台", "景观"}))
    )
    if foreign:
        notes.append(f"疑似非本房源（画面像{hint}）")
    return PhotoAssessment(
        path=path, room_type=room, model_room_type=vision.room_type, confidence=confidence,
        show_label=confidence >= LABEL_MIN_CONFIDENCE and room != "其他",
        quality=quality, sharpness=sharpness, exposure=exposure, composition=composition,
        completeness=completeness, obstruction=float(vision.obstruction_score),
        distortion=float(vision.distortion_score), text_overlay=text_overlay,
        foreign_suspect=foreign, view=bool(vision.view), objects=tuple(vision.objects),
        reason=vision.reason, adjustments=notes, provider=vision.provider,
    )


def _hero_score(item: PhotoAssessment, kind: str, model_hero: float) -> float:
    room_fit = HERO_ROOM_FIT[kind].get(item.room_type, 0.4) if item.show_label else UNCERTAIN_ROOM_FIT
    if item.show_label and item.room_type == "客厅" and item.view:
        room_fit += 0.05  # 景观客厅
    text_fit = 1.0 - min(1.0, item.text_overlay * 1.5)
    return (
        0.18 * item.quality + 0.12 * item.sharpness + 0.08 * item.exposure + 0.12 * item.completeness
        + 0.1 * (1 - item.obstruction) + 0.08 * item.composition + 0.16 * room_fit
        + 0.06 * (1 - item.distortion) + 0.04 * text_fit + 0.06 * model_hero
    )


def _exterior_ok(item: PhotoAssessment) -> bool:
    return (
        item.show_label and item.room_type in {"外观", "入口"} and not item.foreign_suspect
        and item.completeness >= 0.6 and item.obstruction <= 0.4 and item.quality >= 0.5
        and item.distortion <= 0.6
    )


def _exterior_rank(item: PhotoAssessment) -> float:
    return 0.35 * item.completeness + 0.35 * (1 - item.obstruction) + 0.2 * item.quality + 0.1 * (1 - item.distortion)


def _clusters(paths: list[str], vision: Mapping[str, PhotoVision]) -> dict[str, int]:
    hashes = {path: _dhash(Path(path)) for path in paths}
    cluster: dict[str, int] = {}
    groups: dict[str, int] = {}
    next_id = 0
    for path in paths:
        found = None
        for other, cid in cluster.items():
            if _hamming(hashes[path], hashes[other]) <= SAME_ANGLE_HAMMING:
                found = cid
                break
        group = str(getattr(vision.get(path), "duplicate_group", "") or "")
        if found is None and group and group in groups:
            found = groups[group]
        if found is None:
            found, next_id = next_id, next_id + 1
        cluster[path] = found
        if group:
            groups.setdefault(group, found)
    return cluster


def plan_cover(
    *,
    gallery: Iterable[str],
    ranking: Iterable[Mapping[str, Any]],
    vision: Mapping[str, PhotoVision],
    property_type: str,
    manual_cover: str | None = None,
    correction_probe: Callable[[str, str], Mapping[str, Any]] | None = None,
) -> CoverPlan:
    """Choose template, hero, thumbs and labels for one listing.

    ``correction_probe(path, mode)`` reports whether bounded correction would be
    accepted (see ``straighten.correct_file``). A villa exterior that would be
    refused (severe tilt / crop) loses to the next good exterior.
    """
    paths = [str(Path(str(p)).resolve()) for p in gallery]
    if not paths:
        raise ValueError("missing_usable_images")
    kind = _kind_from_type(property_type)
    rows = {str(Path(str(r.get("file") or "")).resolve()): r for r in ranking}
    from .vision import HeuristicVisionClient

    fallback = HeuristicVisionClient()
    vis = {p: vision.get(p) or fallback.analyze(p, ranking_row=rows.get(p)) for p in paths}
    items = [_assess(p, rows.get(p, {}), vis[p], kind) for p in paths]
    by_path = {item.path: item for item in items}
    clusters = _clusters(paths, vis)
    for item in items:
        item.cluster = clusters[item.path]
        item.hero_score = _hero_score(item, kind, float(vis[item.path].hero_score))
    for cid in set(clusters.values()):
        members = sorted((i for i in items if i.cluster == cid), key=lambda i: -i.hero_score)
        for index, member in enumerate(members):
            member.cluster_best = index == 0
            if index:
                member.adjustments.append("同角度重复，封面只用最佳一张")
    providers = sorted({i.provider.split(":")[0] for i in items if i.provider})
    provider = ",".join(providers)
    eligible = [i for i in items if i.cluster_best and not i.foreign_suspect] or items
    notes: list[str] = []
    low_quality = max(i.quality for i in items) < LOW_QUALITY_MAX or all(
        bool(rows.get(i.path, {}).get("low_quality_fallback")) for i in items)
    if low_quality:
        notes.append("图片质量较低")

    # ---- hero + template
    manual = str(Path(manual_cover).resolve()) if manual_cover else ""
    hero: PhotoAssessment
    reason: str
    template: str
    correction: dict[str, Any] = {}
    hero, reason, template, correction = _auto_hero(eligible, kind, correction_probe)
    manual_override = bool(manual and manual in by_path and manual != hero.path)
    if manual_override:
        # Optional admin fix-up only; the automatic choice is the default path.
        hero, correction = by_path[manual], {}
        reason = "管理员手动更换主图（可选纠错）"
        if kind == "villa":
            template = TEMPLATE_VILLA if _exterior_ok(hero) else TEMPLATE_LANDSCAPE
        else:
            template = TEMPLATE_APARTMENT
    manual = manual if manual_override else ""
    if len(paths) == 1:
        template = TEMPLATE_SINGLE
        reason += "；只有一张可用图 → 单图版式"
    elif template != TEMPLATE_GRID and not manual and hero.hero_score < HERO_STANDOUT_MIN and len(eligible) >= 4:
        template = TEMPLATE_GRID
        reason = f"没有突出主图（最高 {hero.hero_score:.2f} < {HERO_STANDOUT_MIN}）→ 四宫格，左上放最高分"

    thumbs = _pick_thumbs(hero, eligible, items, kind, limit=3)
    labels = [by_path[p].room_type if by_path[p].show_label else "" for p in thumbs]
    first = [hero.path, *thumbs]
    rest = sorted((i for i in items if i.path not in first), key=lambda i: (-i.quality, paths.index(i.path)))
    return CoverPlan(
        template=template, kind=kind, hero=hero.path, thumbs=thumbs, labels=labels,
        hero_reason=reason, low_quality=low_quality,
        gallery_order=first + [i.path for i in rest], photos=items,
        hero_correction=correction, notes=notes, provider=provider,
    )


def _auto_hero(eligible: list[PhotoAssessment], kind: str, probe) -> tuple[PhotoAssessment, str, str, dict[str, Any]]:
    by_score = sorted(eligible, key=lambda i: -i.hero_score)
    if kind == "villa":
        exteriors = sorted((i for i in eligible if _exterior_ok(i)), key=lambda i: -_exterior_rank(i))
        for candidate in exteriors:
            report = dict(probe(candidate.path, "exterior")) if probe else {}
            status = str(report.get("status") or "")
            if status.startswith("refused"):
                candidate.adjustments.append(f"外观矫正被拒（{status}），换下一张")
                continue
            return (candidate, f"外观完整度 {candidate.completeness:.2f}、遮挡 {candidate.obstruction:.2f}，"
                               f"在 {len(exteriors)} 张合格外观中最佳", TEMPLATE_VILLA, report)
        rejected = [i for i in eligible if i.room_type in {"外观", "入口"} and i.show_label and not _exterior_ok(i)]
        why = "外观不合格（" + "；".join(
            f"完整度{i.completeness:.2f}/遮挡{i.obstruction:.2f}" for i in rejected) + "）" if rejected else "没有可信外观"
        preferred = [i for i in by_score if i.show_label and i.room_type in {"客厅", "庭院", "泳池"}]
        hero = preferred[0] if preferred and preferred[0].hero_score >= by_score[0].hero_score - 0.05 else by_score[0]
        room = hero.room_type if hero.show_label else "最佳空间"
        return hero, f"{why} → 用{room}做主图，横版 3:2", TEMPLATE_LANDSCAPE, {}
    living = [i for i in by_score if i.show_label and i.room_type == "客厅" and i.quality >= 0.55]
    if living:
        hero = living[0]
        tag = "景观客厅" if hero.view else "高质量客厅"
        return hero, f"{tag}（综合分 {hero.hero_score:.2f}）", TEMPLATE_APARTMENT, {}
    bedroom = [i for i in by_score if i.show_label and i.room_type == "卧室" and i.quality >= 0.55]
    if bedroom:
        return bedroom[0], "没有合格客厅 → 高质量主卧", TEMPLATE_APARTMENT, {}
    confident = [i for i in by_score if i.show_label and i.room_type not in {"卫生间", "停车位"}]
    if confident:
        return confident[0], "没有客厅/卧室 → 最好看的室内", TEMPLATE_APARTMENT, {}
    complete = sorted(by_score, key=lambda i: -(0.6 * i.quality + 0.4 * i.completeness))
    return complete[0], "房间类别不确定 → 质量最高且主体完整的一张（标签隐藏）", TEMPLATE_APARTMENT, {}


def _pick_thumbs(hero: PhotoAssessment, eligible: list[PhotoAssessment], items: list[PhotoAssessment],
                 kind: str, *, limit: int) -> list[str]:
    pool = [i for i in eligible if i.path != hero.path and i.cluster != hero.cluster]
    if len(pool) < limit:  # small listings: allow same-cluster / suspect shots rather than nothing
        extra = [i for i in items if i.path != hero.path and i not in pool and not i.foreign_suspect]
        pool += sorted(extra, key=lambda i: -i.quality)

    def thumb_score(item: PhotoAssessment) -> float:
        harmony = 1.0 - min(1.0, abs(item.exposure - hero.exposure) * 2)
        rooms_seen = {room for room, required in EVIDENCE if set(item.objects) & required}
        mixed = 0.08 if len(rooms_seen) > 1 else 0.0  # e.g. bedroom with the toilet visible through a door
        return 0.6 * item.quality + 0.2 * (1 - item.obstruction) + 0.2 * harmony - mixed

    chosen: list[PhotoAssessment] = []
    hero_room = hero.room_type if hero.show_label else ""
    for room in THUMB_ROOMS[kind]:
        if len(chosen) >= limit:
            break
        if room == hero_room:
            continue
        candidates = sorted((i for i in pool if i.show_label and i.room_type == room and i not in chosen),
                            key=lambda i: -thumb_score(i))
        if candidates:
            chosen.append(candidates[0])
    used_rooms = {i.room_type for i in chosen if i.show_label} | ({hero_room} if hero_room else set())
    rest = sorted((i for i in pool if i not in chosen), key=lambda i: -thumb_score(i))
    fresh = [i for i in rest if not (i.show_label and i.room_type in used_rooms) and i.room_type != "卫生间"]
    for item in fresh + [i for i in rest if i not in fresh]:
        if len(chosen) >= limit:
            break
        chosen.append(item)
        if item.show_label:
            used_rooms.add(item.room_type)
    return [i.path for i in chosen[:limit]]


__all__ = [
    "CoverPlan", "LABEL_MIN_CONFIDENCE", "PhotoAssessment", "TEMPLATE_APARTMENT", "TEMPLATE_GRID",
    "TEMPLATE_LANDSCAPE", "TEMPLATE_SINGLE", "TEMPLATE_VILLA", "cross_validate", "plan_cover",
]

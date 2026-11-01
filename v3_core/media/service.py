"""Prepare immutable V3 media after read-only source overlay verification.
Contact/brand-bearing sources are rejected, never cropped or repainted.
Accepted sources retain their complete scene before ranking and cover layout.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
import shutil
from typing import Any, Iterable

from v3_core.ingest.source_reader import SourceReader
from .cover_plan import plan_cover
from .media_selection import select_publication_media
from .straighten import correct_file
from .vision import VisionClient, analyze_photos, vision_client_from_env
from .photo_formatter import gallery_canvas_key
from .source_overlay import detect_source_overlay

from .cover_renderer import VILLA_TEMPLATES, villa_template_from_env  # noqa: E402  (single source)


SCRUB_REVISION = "source_scrub_v4_nocrop_reject_20261101"
GALLERY_BRAND_REVISION = "qiaolian_gallery_v12_original_frames_20261101"


@dataclass(frozen=True)
class PreparedSourceMedia:
    source_post_id: int
    cover_source_path: str
    gallery_paths: tuple[str, ...]
    source_identity: dict[str, Any]
    duplicates: tuple[dict[str, str], ...]
    rejected_paths: tuple[str, ...]
    ranking: tuple[dict[str, Any], ...]
    # Clean derivatives used only inside the composite cover. Public gallery
    # files are separately branded and must never be fed back into the cover.
    cover_gallery_paths: tuple[str, ...] = ()
    cover_gallery_labels: tuple[str, ...] = ()
    # Cover plan (template / hero / thumbs / labels / per-photo assessment).
    cover_template: str = ""
    cover_render_source: str = ""
    cover_plan: dict[str, Any] = field(default_factory=dict)


class MediaPreparationService:
    def __init__(
        self,
        reader: SourceReader,
        *,
        prepared_dir: str | Path | None = None,
        vision_client: VisionClient | None = None,
    ):
        self.reader = reader
        # Provider comes from V3_VISION_* env vars; default is the offline heuristic.
        self.vision_client = vision_client
        db_path = Path(reader.db_path).expanduser().resolve()
        self.prepared_dir = (
            Path(prepared_dir).expanduser().resolve()
            if prepared_dir is not None
            else db_path.parent.parent / "media" / "prepared_v3"
        )

    @staticmethod
    def _digest(path: Path) -> str:
        digest = hashlib.sha256()
        digest.update((SCRUB_REVISION + "\0").encode("utf-8"))
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _safe_suffix(path: Path) -> str:
        suffix = path.suffix.lower()
        return suffix if suffix in {".jpg", ".jpeg", ".png", ".webp"} else ".img"

    def _original_derivative(self, src: Path, target_dir: Path, digest: str) -> Path:
        fallback = target_dir / f"{digest[:24]}_source{self._safe_suffix(src)}"
        if not fallback.is_file():
            shutil.copy2(src, fallback)
        return fallback

    @staticmethod
    def _gallery_style_key(cover_style: str | None) -> str:
        """Stable cache key per cover mark: classic_blue / right_price / black_gold."""
        try:
            from .cover_styles import cover_style_family

            key = cover_style_family(cover_style)
        except Exception:
            key = str(cover_style or "").strip().lower()
            if key.endswith("_portrait"):
                key = key[: -len("_portrait")]
        if key in {"black_gold", "villa_premium", "dark_glass", "premium_photo", "premium"}:
            return "black_gold"
        if key in {
            "classic_blue",
            "classic",
            "left_info",
            "minimal",
            "minimal_white",
            "blue_banner",
            "premium_4image",
        } or str(key).startswith("video_"):
            return "classic_blue"
        return "right_price"

    @classmethod
    def _gallery_digest(
        cls,
        path: Path,
        cover_style: str | None = None,
        *,
        gallery_orientation: str | None = None,
    ) -> str:
        digest = hashlib.sha256()
        style_key = cls._gallery_style_key(cover_style)
        orient_key = gallery_canvas_key(gallery_orientation)
        digest.update(
            (GALLERY_BRAND_REVISION + "\0" + style_key + "\0" + orient_key + "\0").encode("utf-8")
        )
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _gallery_orientation_for_cover(cover_source_path: str | Path | None) -> str:
        """Lock every 更多实拍 frame to the main-cover orientation for smooth flips."""
        if not cover_source_path:
            return "landscape"
        try:
            from .cover_styles import source_image_orientation

            detected = source_image_orientation(cover_source_path)
        except Exception:
            return "landscape"
        return gallery_canvas_key(detected)

    def _branded_gallery(
        self,
        *,
        source_post_id: int | str,
        paths: list[str],
        cover_style: str | None = None,
        cover_source_path: str | Path | None = None,
    ) -> list[str]:
        """Cache complete original gallery frames, separately from marketing covers."""
        target_dir = self.prepared_dir / str(int(source_post_id)) / "gallery"
        target_dir.mkdir(parents=True, exist_ok=True)
        style_key = self._gallery_style_key(cover_style)
        gallery_orientation = self._gallery_orientation_for_cover(cover_source_path)
        branded: list[str] = []
        for raw in paths:
            source = Path(str(raw)).expanduser().resolve()
            if not source.is_file():
                raise FileNotFoundError(f"gallery_source_not_found:{source}")
            digest = self._gallery_digest(
                source,
                cover_style=cover_style,
                gallery_orientation=gallery_orientation,
            )
            target = target_dir / f"{style_key}_{gallery_orientation}_{digest[:16]}_gallery.jpg"
            if not target.is_file():
                # Gallery is factual evidence: keep original dimensions and
                # pixels, no watermark, forced canvas, enhancement or crop.
                shutil.copy2(source, target)

            branded.append(str(target.resolve()))
        return branded

    def _scrubbed_paths(
        self,
        *,
        source_post_id: int | str,
        paths: list[str],
    ) -> tuple[list[str], dict[str, str], list[str]]:
        target_dir = self.prepared_dir / str(int(source_post_id))
        target_dir.mkdir(parents=True, exist_ok=True)
        accepted: list[str] = []
        raw_to_clean: dict[str, str] = {}
        rejected: list[str] = []

        for raw in paths:
            src = Path(str(raw)).expanduser().resolve()
            if not src.is_file():
                rejected.append(str(src))
                continue
            digest = self._digest(src)
            dst = target_dir / f"{digest[:24]}_clean.jpg"
            chosen: Path | None = None
            try:
                decision_path = target_dir / f"{digest[:24]}_overlay.json"
                if decision_path.is_file():
                    decision = json.loads(decision_path.read_text(encoding="utf-8"))
                else:
                    decision = detect_source_overlay(src)
                    decision_path.write_text(json.dumps(decision), encoding="utf-8")
                if decision.get("blocked"):
                    rejected.append(str(src))
                    continue
                chosen = self._original_derivative(src, target_dir, digest)

            except Exception:
                dst.unlink(missing_ok=True)
                # Failed/uncertain verification must never publish the original.
                chosen = None

            if chosen is None:
                rejected.append(str(src))
                continue
            clean = str(chosen.resolve())
            accepted.append(clean)
            raw_to_clean[str(src)] = clean

        return accepted, raw_to_clean, rejected

    def prepare(
        self,
        *,
        source_post_id: int | str,
        manual_cover_path: str | None = None,
        cover_style: str | None = None,
        cover_preference: str | None = None,
        property_hints: Iterable[object] = (),
    ) -> PreparedSourceMedia:
        from .cover_styles import resolve_cover_room_preference

        raw_paths = self.reader.source_image_paths(source_post_id)
        cleaned_paths, raw_to_clean, scrub_rejected = self._scrubbed_paths(
            source_post_id=source_post_id,
            paths=raw_paths,
        )
        if not cleaned_paths:
            raise ValueError("missing_usable_images_after_source_scrub")

        manual_clean = ""
        if manual_cover_path:
            manual_raw = str(Path(manual_cover_path).expanduser().resolve())
            manual_clean = raw_to_clean.get(manual_raw, "")

        preference = str(cover_preference or "").strip().lower()
        if preference not in {"living", "exterior"}:
            preference = resolve_cover_room_preference(*tuple(property_hints))

        selected = select_publication_media(
            cleaned_paths,
            manual_cover_path=manual_clean or None,
            cover_preference=preference,
        )
        client = self.vision_client or vision_client_from_env()
        vision = analyze_photos(selected["gallery_paths"], client, ranking=selected["ranking"])
        hero_dir = self.prepared_dir / str(int(source_post_id)) / "hero"

        def _probe(path: str, mode: str) -> dict[str, Any]:
            target = hero_dir / f"{Path(path).stem}_{mode}_corrected.jpg"
            return correct_file(path, target, mode=mode)

        plan = plan_cover(
            gallery=selected["gallery_paths"],
            ranking=selected["ranking"],
            vision=vision,
            property_type="别墅" if preference == "exterior" else "公寓",
            manual_cover=manual_clean or None,
            correction_probe=_probe,
        )
        villa_template = villa_template_from_env()
        if plan.template == "villa_4x5":
            plan.template = villa_template
        elif plan.kind == "villa" and plan.template == "landscape_3x2" and villa_template == "villa_black_gold":
            # No usable exterior: the best living room is the hero, still in the villa black-gold layout.
            plan.template = villa_template
        correction = dict(plan.hero_correction)
        if not correction:
            hero_item = next((i for i in plan.photos if i.path == plan.hero), None)
            exterior_hero = hero_item is not None and hero_item.show_label and hero_item.room_type in {"外观", "入口"}
            mode = "exterior" if exterior_hero else "interior"
            correction = _probe(plan.hero, mode)
            plan.hero_correction = correction
        render_source = str(correction.get("path") or "")
        if not render_source or not Path(render_source).is_file():
            render_source = plan.hero
        cover_resolved = plan.hero
        # One ordered list for both surfaces: channel cover = hero + thumbs 1–3;
        # bot detail first 4 standalone photos = the same 4, same order.
        gallery_sources = list(plan.gallery_order)
        bot_sources = list(gallery_sources)
        cover_thumbs = list(plan.thumbs) + [p for p in gallery_sources if p != cover_resolved and p not in plan.thumbs]
        thumb_labels = list(plan.labels) + [""] * (len(cover_thumbs) - len(plan.labels))
        branded_gallery = self._branded_gallery(
            source_post_id=source_post_id,
            paths=bot_sources,
            cover_style=cover_style,
            cover_source_path=cover_resolved,
        )
        rejected = [str(path) for path in scrub_rejected]
        rejected.extend(str(path) for path in selected["rejected_paths"])
        gallery_orientation = self._gallery_orientation_for_cover(cover_resolved)
        plan_summary = plan.summary()
        plan_summary["bot_first_batch"] = list(branded_gallery[:4])
        return PreparedSourceMedia(
            source_post_id=int(source_post_id),
            cover_source_path=cover_resolved,
            gallery_paths=tuple(branded_gallery),
            source_identity={
                **self.reader.source_identity(source_post_id),
                "gallery_brand_revision": GALLERY_BRAND_REVISION,
                "gallery_cover_style": self._gallery_style_key(cover_style),
                "gallery_orientation": gallery_orientation,
                "cover_room_preference": preference,
                "cover_plan": plan_summary,
            },
            duplicates=tuple(dict(item) for item in selected["duplicates"]),
            rejected_paths=tuple(rejected),
            ranking=tuple(dict(item) for item in selected["ranking"]),
            cover_gallery_paths=tuple(cover_thumbs),
            cover_gallery_labels=tuple(thumb_labels),
            cover_template=plan.template,
            cover_render_source=render_source,
            cover_plan={**plan_summary, "photos": [item.as_dict() for item in plan.photos]},
        )


__all__ = [
    "GALLERY_BRAND_REVISION",
    "MediaPreparationService",
    "PreparedSourceMedia",
]

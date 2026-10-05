"""Album hero frame for the public 「更多实拍」 album.

The prepared gallery deliberately leaves out the cover-source photo (it is
already the big picture on the channel cover). Inside the User Bot album that
meant villas opened on a kitchen shot instead of the facade. This module gives
the album its hero back:

* ``brand_album_hero`` brands the clean cover-source exactly like every other
  gallery frame (same formatter, same top-left corner mark, same canvas
  orientation), so the album stays visually consistent.
* ``album_hero_for_package`` returns the hero for a frozen package. New
  packages carry ``source_identity["album_hero_path"]`` (written at media
  preparation time). Older packages fall back to a conservative read-only
  recovery of the clean cover source next to the gallery files; when anything
  is ambiguous it returns ``""`` and the album simply keeps gallery order.

Nothing here mutates source evidence or the frozen package.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Iterable, Mapping

ALBUM_HERO_REVISION = "qiaolian_album_hero_v1_20261005"
_CLEAN_SUFFIXES = (".jpg", ".jpeg", ".png", ".webp")

# package_id -> resolved hero path ("" when none). Read path is hot; recovery
# hashes a handful of files, so remember the answer per process.
_HERO_CACHE: dict[str, str] = {}


def _existing(raw: object) -> Path | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        path = Path(text).expanduser()
        return path.resolve() if path.is_file() else None
    except OSError:
        return None


def _cache_dir() -> Path | None:
    import os

    override = str(os.environ.get("QIAOLIAN_ALBUM_HERO_CACHE_DIR") or "").strip()
    for candidate in (
        *((Path(override),) if override else ()),
        Path("/opt/qiaolian_v3/runtime/media/album_hero_cache"),
        Path.home() / ".cache" / "qiaolian_album_hero",
        Path("/tmp/qiaolian_album_hero"),
    ):
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            probe = candidate / ".write_test"
            probe.touch()
            probe.unlink()
            return candidate
        except OSError:
            continue
    return None


def brand_album_hero(
    source: str | Path,
    *,
    target_dir: str | Path,
    cover_style: str | None,
    gallery_orientation: str,
) -> str:
    """Brand ``source`` like a gallery frame; deterministic + cached by content."""
    from .photo_formatter import format_gallery_photo, gallery_canvas_key, resolve_gallery_logo_path

    src = Path(str(source)).expanduser().resolve()
    if not src.is_file():
        raise FileNotFoundError(f"album_hero_source_not_found:{src}")
    orientation = gallery_canvas_key(gallery_orientation)
    style = str(cover_style or "").strip()
    digest = hashlib.sha256()
    digest.update((ALBUM_HERO_REVISION + "\0" + style + "\0" + orientation + "\0").encode("utf-8"))
    with src.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    out_dir = Path(str(target_dir)).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / f"hero_{orientation}_{digest.hexdigest()[:16]}_gallery.jpg"
    if not target.is_file():
        format_gallery_photo(
            src,
            target,
            logo_path=resolve_gallery_logo_path(style or None),
            logo_position="top_left",
            add_logo=True,
            enhance=True,
            cover_style=style or None,
            force_orientation=orientation,
        )
    return str(target.resolve())


def _json_dict(raw: object) -> dict[str, Any]:
    if isinstance(raw, Mapping):
        return dict(raw)
    import json

    try:
        value = json.loads(str(raw or "{}"))
    except (TypeError, ValueError):
        return {}
    return dict(value) if isinstance(value, dict) else {}


def _recover_cover_source(gallery: Iterable[str], identity: Mapping[str, Any]) -> Path | None:
    """Find the clean cover-source for a legacy package, or ``None``.

    The cover source is the only clean derivative in the post's prepared dir
    that was never branded into ``gallery/`` (gallery names embed a digest of
    their clean source). Operator-excluded frames were branded, so they never
    come back. If several never-branded files remain (dupes / hard rejects),
    the ranker picks the best non-reject using the stored room preference —
    the same rule that chose the cover in the first place.
    """
    from .service import GALLERY_BRAND_REVISION, MediaPreparationService

    if str(identity.get("gallery_brand_revision") or "") != GALLERY_BRAND_REVISION:
        return None
    style_key = str(identity.get("gallery_cover_style") or "").strip()
    orientation = str(identity.get("gallery_orientation") or "").strip()
    if not style_key or not orientation:
        return None

    files = [p for p in (_existing(raw) for raw in gallery) if p is not None]
    if not files:
        return None
    gallery_dir = files[0].parent
    if gallery_dir.name != "gallery" or any(p.parent != gallery_dir for p in files):
        return None
    post_dir = gallery_dir.parent

    leftovers: list[Path] = []
    for candidate in sorted(post_dir.iterdir()):
        name = candidate.name.lower()
        if not candidate.is_file() or not name.endswith(_CLEAN_SUFFIXES):
            continue
        if not (name.rsplit(".", 1)[0].endswith("_clean") or "_source." in name):
            continue
        digest = MediaPreparationService._gallery_digest(
            candidate,
            cover_style=style_key,
            gallery_orientation=orientation,
        )
        branded = gallery_dir / f"{style_key}_{orientation}_{digest[:16]}_gallery.jpg"
        if branded.exists():
            continue
        leftovers.append(candidate.resolve())

    if not leftovers:
        return None
    if len(leftovers) == 1:
        return leftovers[0]
    from .ranker import rank_photo_paths

    ranking = rank_photo_paths(
        leftovers,
        cover_preference=str(identity.get("cover_room_preference") or "living"),
    )
    for item in ranking:
        if not item.get("reject"):
            return Path(str(item["file"])).resolve()
    return None


def album_hero_for_package(package: Mapping[str, Any], gallery: Iterable[str]) -> str:
    """Return the branded hero frame path for a frozen package, or ``""``."""
    identity = _json_dict(package.get("source_identity_json") or package.get("source_identity"))
    explicit = _existing(identity.get("album_hero_path"))
    if explicit is not None:
        return str(explicit)

    key = str(package.get("package_id") or "").strip()
    if key and key in _HERO_CACHE:
        cached = _HERO_CACHE[key]
        if not cached or Path(cached).is_file():
            return cached

    hero = ""
    try:
        source = _recover_cover_source(tuple(gallery), identity)
        cache = _cache_dir() if source is not None else None
        if source is not None and cache is not None:
            hero = brand_album_hero(
                source,
                target_dir=cache,
                cover_style=str(package.get("cover_style") or identity.get("gallery_cover_style") or ""),
                gallery_orientation=str(identity.get("gallery_orientation") or "landscape"),
            )
    except Exception:
        hero = ""
    if key:
        _HERO_CACHE[key] = hero
    return hero


__all__ = ["ALBUM_HERO_REVISION", "album_hero_for_package", "brand_album_hero"]

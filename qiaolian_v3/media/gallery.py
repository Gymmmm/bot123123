from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from PIL import Image, ImageOps, ImageStat

IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.webp'}
MIN_WIDTH = 700
MIN_HEIGHT = 500
NEAR_DUPLICATE_HAMMING = 2


@dataclass(frozen=True)
class MediaSelection:
    cover_path: str
    cover_source: str
    cover_style: str
    gallery_paths: tuple[str, ...]
    cover_candidates: tuple[str, ...]
    duplicates: tuple[dict[str, str], ...]
    rejected_paths: tuple[str, ...]
    warnings: tuple[str, ...]
    source_count: int
    usable_count: int
    meets_photo_minimum: bool


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _dhash(path: Path, hash_size: int = 8) -> int | None:
    try:
        with Image.open(path) as source:
            image = ImageOps.exif_transpose(source).convert('L')
            image = image.resize((hash_size + 1, hash_size), Image.Resampling.LANCZOS)
            pixels = list(image.get_flattened_data())
    except Exception:
        return None
    width = hash_size + 1
    result = 0
    bit = 0
    for y in range(hash_size):
        row = y * width
        for x in range(hash_size):
            if pixels[row + x] > pixels[row + x + 1]:
                result |= 1 << bit
            bit += 1
    return result


def _hamming(a: int | None, b: int | None) -> int:
    if a is None or b is None:
        return 10_000
    return (a ^ b).bit_count()


def _quality(path: Path) -> tuple[bool, float, str]:
    try:
        with Image.open(path) as source:
            image = ImageOps.exif_transpose(source).convert('RGB')
            width, height = image.size
            if width < MIN_WIDTH or height < MIN_HEIGHT:
                return False, -1000.0, 'low_resolution'
            gray = image.convert('L')
            stat = ImageStat.Stat(gray)
            mean = float(stat.mean[0])
            contrast = float(stat.stddev[0])
            if mean < 20 or mean > 245:
                return False, -900.0, 'bad_brightness'
            ratio = width / max(height, 1)
            orientation = 100.0 if 1.05 <= ratio <= 1.85 else (70.0 if ratio >= 0.9 else 45.0)
            resolution = min((width * height) / 2_000_000 * 100.0, 100.0)
            score = orientation * 0.55 + resolution * 0.25 + min(contrast * 2.0, 100.0) * 0.20
            return True, round(score, 2), ''
    except Exception:
        return False, -1000.0, 'decode_error'


def _normalize_style(requested: str | None, *, manual_style_selected: bool) -> tuple[str, list[str]]:
    requested = str(requested or 'classic_blue').strip().lower()
    aliases = {
        'classic': 'classic_blue', 'left_info': 'classic_blue', 'minimal': 'classic_blue',
        'price_tag': 'right_price', 'right_price_fixed': 'right_price',
        'villa_premium': 'black_gold', 'dark_glass': 'black_gold',
    }
    requested = aliases.get(requested, requested)
    warnings: list[str] = []
    if requested == 'right_price' and not manual_style_selected:
        warnings.append('right_price_requires_manual_selection')
        return 'classic_blue', warnings
    if requested not in {'classic_blue', 'right_price', 'black_gold'}:
        return 'classic_blue', warnings
    return requested, warnings


def build_media_selection(
    paths: Iterable[str | Path],
    *,
    manual_cover_path: str | Path | None = None,
    requested_cover_style: str | None = None,
    manual_style_selected: bool = False,
    minimum_photos: int = 4,
) -> MediaSelection:
    source_paths: list[Path] = []
    for raw in paths:
        path = Path(str(raw or '')).expanduser().resolve()
        if path.is_file() and path.suffix.lower() in IMAGE_EXTS and path not in source_paths:
            source_paths.append(path)

    duplicates: list[dict[str, str]] = []
    unique: list[Path] = []
    seen_exact: dict[str, Path] = {}
    seen_near: list[tuple[int | None, Path]] = []
    for path in source_paths:
        digest = _sha256(path)
        dhash = _dhash(path)
        duplicate_of = seen_exact.get(digest)
        kind = 'exact' if duplicate_of is not None else ''
        if duplicate_of is None:
            for previous_hash, previous_path in seen_near:
                if _hamming(dhash, previous_hash) <= NEAR_DUPLICATE_HAMMING:
                    duplicate_of = previous_path
                    kind = 'near'
                    break
        if duplicate_of is not None:
            duplicates.append({'file': str(path), 'duplicate_of': str(duplicate_of), 'kind': kind})
            continue
        seen_exact[digest] = path
        seen_near.append((dhash, path))
        unique.append(path)

    rejected: list[str] = []
    ranked: list[tuple[float, int, str]] = []
    gallery: list[str] = []
    for index, path in enumerate(unique):
        usable, score, _reason = _quality(path)
        if not usable:
            rejected.append(str(path))
            continue
        resolved = str(path)
        gallery.append(resolved)
        ranked.append((score, -index, resolved))

    ranked.sort(reverse=True)
    candidates = [path for _score, _order, path in ranked]
    if gallery:
        manual = str(Path(str(manual_cover_path)).expanduser().resolve()) if manual_cover_path else ''
        if manual and manual in gallery:
            cover_path, cover_source = manual, 'manual'
        else:
            cover_path, cover_source = (candidates[0] if candidates else gallery[0]), 'automatic'
    else:
        cover_path, cover_source = '', 'none'

    style, warnings = _normalize_style(requested_cover_style, manual_style_selected=manual_style_selected)
    return MediaSelection(
        cover_path=cover_path,
        cover_source=cover_source,
        cover_style=style,
        gallery_paths=tuple(gallery),
        cover_candidates=tuple(candidates[:5]),
        duplicates=tuple(duplicates),
        rejected_paths=tuple(rejected),
        warnings=tuple(warnings),
        source_count=len(source_paths),
        usable_count=len(gallery),
        meets_photo_minimum=len(gallery) >= max(1, int(minimum_photos)),
    )


__all__ = ['MediaSelection', 'build_media_selection']

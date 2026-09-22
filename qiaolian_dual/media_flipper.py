"""Cover-first single-photo flipper for search cards and channel entry."""
from __future__ import annotations

import os
from pathlib import Path

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.constants import ParseMode

from .common import he


def _existing(path: object) -> str:
    raw = str(path or '').strip()
    if not raw:
        return ''
    try:
        candidate = Path(raw).expanduser()
        if candidate.is_file():
            return str(candidate.resolve())
    except OSError:
        return ''
    return ''


def _is_cover_name(path: str) -> bool:
    name = os.path.basename(path).lower()
    return name in {'cover.jpg', 'cover.jpeg', 'cover.png', 'cover.webp'} or name.startswith('cover')


def _looks_like_rendered_cover(path: Path, *, public_id: str, style: str) -> bool:
    name = path.name.lower()
    pid = str(public_id or '').strip().lower()
    if not pid or pid not in name:
        return False
    if path.suffix.lower() not in {'.png', '.jpg', '.jpeg', '.webp'}:
        return False
    if style and style.lower() in name:
        return True
    return name.startswith(pid + '_') or name.startswith(pid + '.')


def _cover_search_roots(item: dict) -> list[Path]:
    roots: list[Path] = []
    seen: set[str] = set()

    def _add(path: Path) -> None:
        try:
            resolved = path.expanduser().resolve()
        except OSError:
            return
        key = str(resolved)
        if key in seen:
            return
        seen.add(key)
        roots.append(resolved)

    explicit = str(item.get('cover_path') or '').strip()
    if explicit:
        parent = Path(explicit).expanduser().parent
        _add(parent)
        _add(parent.parent)

    media = item.get('media_files') if isinstance(item.get('media_files'), list) else []
    for raw in media:
        gallery_path = Path(str(raw or '').strip())
        if not str(gallery_path):
            continue
        parent = gallery_path.expanduser().parent
        _add(parent)
        for up in list(parent.parents)[:6]:
            _add(up)
            _add(up / 'covers_v3')
            _add(up / 'covers')
            if up.name in {'gallery', 'prepared_v3', 'media'}:
                _add(up.parent / 'covers_v3')
                _add(up.parent / 'covers')
                _add(up.parent.parent / 'covers_v3')

    # Common production mounts when absolute frozen paths are stale.
    for root in (
        Path('/opt/qiaolian_dual_bots/media/covers_v3'),
        Path('/opt/qiaolian_dual_bots/media/covers'),
        Path('/opt/qiaolian_v3/runtime/media/covers_v3'),
    ):
        _add(root)
    return roots


def resolve_cover_path(item: dict) -> str:
    """Prefer frozen cover_path; if missing on disk, rediscover rendered cover."""
    explicit = _existing(item.get('cover_path'))
    if explicit:
        return explicit

    public_id = str(item.get('public_listing_id') or item.get('listing_id') or '').strip()
    style = str(item.get('cover_style') or '').strip()
    if not public_id:
        return ''

    candidates: list[str] = []
    for root in _cover_search_roots(item):
        if style:
            for suffix in ('.png', '.jpg', '.jpeg', '.webp'):
                candidates.append(str(root / f'{public_id}_{style}{suffix}'))
        try:
            if not root.is_dir():
                continue
            for child in root.iterdir():
                if child.is_file() and _looks_like_rendered_cover(child, public_id=public_id, style=style):
                    candidates.append(str(child))
        except OSError:
            continue

    for candidate in candidates:
        found = _existing(candidate)
        if found and _looks_like_rendered_cover(Path(found), public_id=public_id, style=style):
            return found
    return ''


def listing_media_paths(item: dict) -> list[str]:
    """Cover first, then unique gallery files that exist on disk."""
    ordered: list[str] = []
    seen: set[str] = set()

    def _add(path: object) -> None:
        resolved = _existing(path)
        if not resolved or resolved in seen:
            return
        seen.add(resolved)
        ordered.append(resolved)

    cover = resolve_cover_path(item if isinstance(item, dict) else {})
    _add(cover)
    media = item.get('media_files') if isinstance(item.get('media_files'), list) else []
    covers = [p for p in media if isinstance(p, str) and _is_cover_name(p)]
    others = [p for p in media if isinstance(p, str) and not _is_cover_name(p)]
    for path in covers + others:
        # Skip duplicate cover-named assets once rendered cover is already first.
        if cover and _is_cover_name(str(path)):
            continue
        _add(path)
    if not ordered:
        _add(item.get('media_file_id'))
    return ordered


def flipper_caption(item: dict, *, photo_index: int, photo_total: int) -> str:
    from .text_utils import clean_inline_text
    from .utils_formatting import _display_layout, _fmt_price

    project = clean_inline_text(str(item.get('project') or item.get('community') or item.get('area') or '房源'))
    layout = _display_layout(item.get('layout') or item.get('property_type'), item.get('property_type'))
    price = _fmt_price(item.get('price')) if item.get('price') not in (None, '', 0, '0') else ''
    head = ' · '.join(part for part in (project, layout, price) if part) or '房源'
    total = max(0, int(photo_total or 0))
    if total > 0:
        index = min(max(0, int(photo_index or 0)), total - 1)
        return f'{he(head)} · 📸 {index + 1}/{total}'
    return he(head)


def search_card_keyboard(
    listing_id: str,
    *,
    available: bool,
    listing_nav: list[InlineKeyboardButton] | None = None,
) -> InlineKeyboardMarkup:
    """Search result card: 上一套/下一套 for listings; 📷 房源详情 opens flipper."""
    rows: list[list[InlineKeyboardButton]] = []
    if listing_nav:
        rows.append(listing_nav)
    rows.append([InlineKeyboardButton('📷 房源详情', callback_data=f'listing:photos:{listing_id}')])
    if available:
        rows.append([InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}')])
    rows.append([InlineKeyboardButton('💬 咨询这套', callback_data=f'listing:consult:{listing_id}')])
    rows.append([InlineKeyboardButton('✏️ 换个条件', callback_data='home_smart_search')])
    return InlineKeyboardMarkup(rows)


def photo_flipper_keyboard(
    listing_id: str,
    *,
    available: bool,
    photo_index: int,
    photo_total: int,
    return_to_results: bool = False,
) -> InlineKeyboardMarkup:
    """Edge-aware pager: hide 上一张 on first, 下一张 on last (no wrap)."""
    rows: list[list[InlineKeyboardButton]] = []
    total = max(0, int(photo_total or 0))
    index = max(0, int(photo_index or 0))
    if total > 1:
        nav: list[InlineKeyboardButton] = []
        if index > 0:
            nav.append(InlineKeyboardButton('⬅️ 上一张', callback_data=f'album:{listing_id}:{index - 1}'))
        if index < total - 1:
            nav.append(InlineKeyboardButton('下一张 ➡️', callback_data=f'album:{listing_id}:{index + 1}'))
        if nav:
            rows.append(nav)
    action: list[InlineKeyboardButton] = []
    if available:
        action.append(InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}'))
    action.append(InlineKeyboardButton('💬 咨询这套', callback_data=f'listing:consult:{listing_id}'))
    rows.append(action)
    if return_to_results:
        rows.append([InlineKeyboardButton('🔍 返回结果', callback_data='find:show_current')])
    else:
        rows.append([InlineKeyboardButton('🔍 继续找房', callback_data='home_smart_search')])
    return InlineKeyboardMarkup(rows)


async def send_or_edit_photo_flipper(
    *,
    bot,
    chat_id: int,
    listing_id: str,
    item: dict,
    photo_index: int = 0,
    query=None,
    return_to_results: bool = False,
) -> None:
    """Show one photo at a time. Prefer editing an existing photo message."""
    paths = listing_media_paths(item if isinstance(item, dict) else {})
    status = str((item or {}).get('status') or 'active').strip().lower()
    available = status in {'active', 'reserved'}
    total = len(paths)
    if total <= 0:
        keyboard = photo_flipper_keyboard(
            listing_id,
            available=available,
            photo_index=0,
            photo_total=0,
            return_to_results=return_to_results,
        )
        text = '📷 <b>房源详情</b>\n\n这套房的实拍暂时没有加载出来。\n可以稍后再试，或直接咨询中文顾问。'
        if query is not None:
            try:
                if getattr(query.message, 'photo', None):
                    await query.edit_message_caption(caption=text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
                else:
                    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
                return
            except Exception:
                pass
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return

    index = min(max(0, int(photo_index or 0)), total - 1)
    path = paths[index]
    caption = flipper_caption(item, photo_index=index, photo_total=total)
    keyboard = photo_flipper_keyboard(
        listing_id,
        available=available,
        photo_index=index,
        photo_total=total,
        return_to_results=return_to_results,
    )

    if query is not None and getattr(query.message, 'photo', None):
        with open(path, 'rb') as photo:
            await query.edit_message_media(
                media=InputMediaPhoto(media=photo.read(), caption=caption, parse_mode=ParseMode.HTML),
                reply_markup=keyboard,
            )
        return

    with open(path, 'rb') as photo:
        await bot.send_photo(
            chat_id=chat_id,
            photo=photo,
            caption=caption,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )


__all__ = [
    'flipper_caption',
    'listing_media_paths',
    'photo_flipper_keyboard',
    'resolve_cover_path',
    'search_card_keyboard',
    'send_or_edit_photo_flipper',
]

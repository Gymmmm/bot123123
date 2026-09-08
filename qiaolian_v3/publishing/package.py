from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, Iterable


class PackageBlocked(RuntimeError):
    pass


BUTTON_TEXTS = ('🏠 房源详情', '📸 更多实拍', '📅 预约看房')


@dataclass(frozen=True)
class FrozenPublicationPackage:
    package_id: str
    listing_id: int
    offer_id: int
    canonical_record_id: int
    target_channel_id: str
    deal_type: str
    offer_type: str
    publication_policy: str
    source_mode: str
    quality_result: str
    canonical_hash: str
    cover_path: str
    gallery: tuple[str, ...]
    caption_html: str
    keyboard: tuple[tuple[str, str], ...]
    content_hash: str
    frozen: bool = True

    def payload(self) -> dict[str, Any]:
        return {
            'package_id': self.package_id,
            'listing_id': self.listing_id,
            'offer_id': self.offer_id,
            'canonical_record_id': self.canonical_record_id,
            'target_channel_id': self.target_channel_id,
            'deal_type': self.deal_type,
            'offer_type': self.offer_type,
            'publication_policy': self.publication_policy,
            'source_mode': self.source_mode,
            'quality_result': self.quality_result,
            'canonical_hash': self.canonical_hash,
            'cover_path': self.cover_path,
            'gallery': list(self.gallery),
            'caption_html': self.caption_html,
            'keyboard': [{'text': t, 'url': u} for t, u in self.keyboard],
            'content_hash': self.content_hash,
            'frozen': True,
        }


def _stable_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def build_frozen_rent_package(
    *, listing_id: int, offer_id: int, canonical_record_id: int,
    target_channel_id: str, canonical: dict[str, Any], offer: dict[str, Any],
    source_mode: str, quality_result: str, canonical_hash: str,
    cover_path: str, gallery: Iterable[str], caption_html: str,
    bot_username: str, public_listing_id: str,
) -> FrozenPublicationPackage:
    if str(canonical.get('deal_type') or '') != 'rent':
        raise PackageBlocked('package_requires_rent_canonical')
    if str(offer.get('offer_type') or '') != 'rent':
        raise PackageBlocked('package_requires_rent_offer')
    if str(offer.get('publication_policy') or '') != 'telegram_rent':
        raise PackageBlocked('package_requires_telegram_rent_policy')
    if str(source_mode or '') not in {'collector', 'admin_import'}:
        raise PackageBlocked('invalid_source_mode')
    if not str(canonical_hash or ''):
        raise PackageBlocked('missing_canonical_hash')
    caption = str(caption_html or '')
    if not caption or len(caption) > 1024:
        raise PackageBlocked('caption_out_of_range')
    cover = str(cover_path or '').strip()
    if not cover:
        raise PackageBlocked('missing_cover')
    username = str(bot_username or '').lstrip('@').strip()
    public_id = str(public_listing_id or '').strip()
    if not username or not public_id:
        raise PackageBlocked('missing_deep_link_identity')
    keyboard = tuple(
        (text, f'https://t.me/{username}?start={action}_{public_id}')
        for text, action in zip(BUTTON_TEXTS, ('details', 'photos', 'book'))
    )
    frozen_fields = {
        'listing_id': int(listing_id), 'offer_id': int(offer_id),
        'canonical_record_id': int(canonical_record_id),
        'target_channel_id': str(target_channel_id), 'deal_type': 'rent',
        'offer_type': 'rent', 'publication_policy': 'telegram_rent',
        'source_mode': str(source_mode), 'quality_result': str(quality_result),
        'canonical_hash': str(canonical_hash), 'cover_path': cover,
        'gallery': tuple(str(v) for v in gallery if str(v)),
        'caption_html': caption, 'keyboard': keyboard,
    }
    content_hash = _stable_hash(frozen_fields)
    package_id = 'PKG_' + content_hash[:20].upper()
    return FrozenPublicationPackage(package_id=package_id, content_hash=content_hash, **frozen_fields)


__all__ = ['BUTTON_TEXTS','FrozenPublicationPackage','PackageBlocked','build_frozen_rent_package']

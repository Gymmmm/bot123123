from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .package import FrozenPublicationPackage


class ChannelEditBlocked(RuntimeError):
    pass


@dataclass(frozen=True)
class ChannelEditPlan:
    action: str
    channel_id: str
    message_id: int
    package_id: str
    listing_id: int
    offer_id: int
    previous_content_hash: str
    next_content_hash: str
    dry_run: bool = True


class ChannelEditor:
    def __init__(self, *, gateway: Any, dry_run: bool = True) -> None:
        self.gateway = gateway
        self.dry_run = bool(dry_run)

    def plan_edit(
        self, *, package: FrozenPublicationPackage, mapping: dict[str, Any],
        channel_id: str, message_id: int, expected_previous_hash: str,
    ) -> ChannelEditPlan:
        if package.deal_type != 'rent' or package.offer_type != 'rent' or not package.frozen:
            raise ChannelEditBlocked('editor_requires_frozen_rent_package')
        if int(message_id or 0) <= 0:
            raise ChannelEditBlocked('explicit_message_id_required')
        if str(channel_id) != package.target_channel_id:
            raise ChannelEditBlocked('channel_mismatch')
        if str(mapping.get('channel_id') or '') != str(channel_id):
            raise ChannelEditBlocked('mapping_channel_mismatch')
        if int(mapping.get('message_id') or 0) != int(message_id):
            raise ChannelEditBlocked('mapping_message_mismatch')
        if int(mapping.get('listing_id') or 0) != package.listing_id or int(mapping.get('offer_id') or 0) != package.offer_id:
            raise ChannelEditBlocked('mapping_listing_offer_mismatch')
        current_hash = str(mapping.get('content_hash') or '')
        if current_hash != str(expected_previous_hash or ''):
            raise ChannelEditBlocked('previous_hash_conflict')
        action = 'noop' if current_hash == package.content_hash else 'edit'
        return ChannelEditPlan(
            action=action, channel_id=str(channel_id), message_id=int(message_id),
            package_id=package.package_id, listing_id=package.listing_id, offer_id=package.offer_id,
            previous_content_hash=current_hash, next_content_hash=package.content_hash,
            dry_run=self.dry_run,
        )

    def execute(self, plan: ChannelEditPlan, package: FrozenPublicationPackage) -> dict[str, Any]:
        if not package.frozen or package.deal_type != 'rent' or package.offer_type != 'rent':
            raise ChannelEditBlocked('editor_requires_frozen_rent_package')
        if plan.package_id != package.package_id:
            raise ChannelEditBlocked('plan_package_mismatch')
        if plan.next_content_hash != package.content_hash:
            raise ChannelEditBlocked('plan_content_hash_mismatch')
        if plan.channel_id != package.target_channel_id:
            raise ChannelEditBlocked('plan_channel_mismatch')
        if plan.listing_id != package.listing_id or plan.offer_id != package.offer_id:
            raise ChannelEditBlocked('plan_listing_offer_mismatch')
        if int(plan.message_id or 0) <= 0:
            raise ChannelEditBlocked('explicit_message_id_required')
        if plan.action == 'noop':
            return {'action': 'noop', 'dry_run': True, 'writes': 0, 'message_id': plan.message_id}
        if plan.action != 'edit':
            raise ChannelEditBlocked('unsupported_edit_action')
        if plan.dry_run or self.dry_run:
            return {'action': 'edit', 'dry_run': True, 'writes': 0, 'message_id': plan.message_id}
        return self.gateway.edit(
            channel_id=plan.channel_id, message_id=plan.message_id,
            caption=package.caption_html,
            keyboard=[{'text': text, 'url': url} for text, url in package.keyboard],
        )


__all__ = ['ChannelEditBlocked','ChannelEditPlan','ChannelEditor']

from __future__ import annotations

from typing import Any

from .eligibility import evaluate_rent_publication_eligibility
from .package import FrozenPublicationPackage
from .delivery import DeliveryCoordinator, DeliveryBlocked


class PublicationBlocked(RuntimeError):
    pass


class RentPublisher:
    def __init__(self, *, gateway: Any, delivery: DeliveryCoordinator, dry_run: bool = True) -> None:
        self.gateway = gateway
        self.delivery = delivery
        self.dry_run = bool(dry_run)

    def publish(self, package: FrozenPublicationPackage) -> dict[str, Any]:
        eligibility = evaluate_rent_publication_eligibility(
            canonical={'deal_type': package.deal_type},
            offer={'offer_type': package.offer_type, 'publication_policy': package.publication_policy},
            source_mode=package.source_mode,
            quality_result=package.quality_result,
            frozen=package.frozen,
        )
        if not eligibility.allowed:
            raise PublicationBlocked(','.join(eligibility.reason_codes))
        if self.dry_run:
            return {'action': 'publish', 'dry_run': True, 'package_id': package.package_id, 'writes': 0}
        try:
            state = self.delivery.begin(package)
        except DeliveryBlocked as exc:
            if 'published' in str(exc):
                return {'action': 'publish', 'status': 'already_published'}
            raise
        if state.state == 'published':
            return {'action': 'publish', 'status': 'already_published', 'message_id': state.message_id}
        try:
            result = self.gateway.send(
                channel_id=package.target_channel_id,
                cover_path=package.cover_path,
                caption=package.caption_html,
                keyboard=[{'text': text, 'url': url} for text, url in package.keyboard],
            )
        except Exception as exc:
            # Unknown external outcome: deliberately do NOT reset to frozen.
            raise PublicationBlocked('telegram_outcome_unknown_reconcile_required') from exc
        message_id = int(result.get('message_id') or 0)
        if message_id <= 0:
            raise PublicationBlocked('telegram_outcome_unknown_reconcile_required')
        self.delivery.commit_sent(package, message_id=message_id)
        return {'action': 'publish', 'status': 'published', 'message_id': message_id}


__all__ = ['PublicationBlocked','RentPublisher']

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from .gate import AutoPublishGate
from .package import FrozenPublicationPackage


@dataclass(frozen=True)
class AutoPublishPlan:
    package_id: str
    listing_id: int
    offer_id: int
    target_channel_id: str
    action: str = 'publish'
    dry_run: bool = True


class AutoPublishPlanner:
    def __init__(self, gate: AutoPublishGate | None = None) -> None:
        self.gate = gate or AutoPublishGate()

    def plan(self, candidates: Iterable[tuple[FrozenPublicationPackage, dict[str, Any]]]) -> list[AutoPublishPlan]:
        plans: list[AutoPublishPlan] = []
        seen: set[str] = set()
        for package, context in candidates:
            result = self.gate.evaluate(
                package,
                source_mode=str(context.get('source_mode') or package.source_mode),
                quality_result=str(context.get('quality_result') or package.quality_result),
            )
            if not result.allowed or package.package_id in seen:
                continue
            seen.add(package.package_id)
            plans.append(AutoPublishPlan(
                package_id=package.package_id,
                listing_id=package.listing_id,
                offer_id=package.offer_id,
                target_channel_id=package.target_channel_id,
            ))
        return plans


__all__ = ['AutoPublishPlan','AutoPublishPlanner']

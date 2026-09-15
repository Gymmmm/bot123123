"""Materialize canonical facts into V3 listing identity and offers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from v3_core.storage.inventory_repository import InventoryRepository


@dataclass(frozen=True)
class MaterializationResult:
    listing_id: str
    public_listing_id: str
    offer_ids: tuple[str, ...]
    review_ids: tuple[str, ...]


class InventoryMaterializationService:
    def __init__(self, repository: InventoryRepository):
        self.repository = repository

    def materialize(
        self,
        *,
        canonical_record_id: str,
        listing_id: str,
        public_listing_id: str,
        create_review: bool = True,
    ) -> MaterializationResult:
        facts = self.repository.canonical_facts(canonical_record_id)
        self.repository.upsert_listing(
            listing_id=listing_id,
            public_listing_id=public_listing_id,
            canonical_record_id=canonical_record_id,
            facts=facts,
        )
        offers = self.repository.sync_offers(listing_id=listing_id, facts=facts)

        review_ids: list[str] = []
        if create_review:
            quality = facts.get("quality") if isinstance(facts.get("quality"), dict) else {}
            score = int(quality.get("score") or 0)
            flags = [str(value) for value in (quality.get("all_flags") or []) if str(value)]
            if offers:
                for offer in offers:
                    review = self.repository.create_review_item(
                        canonical_record_id=canonical_record_id,
                        listing_id=listing_id,
                        offer_id=str(offer["offer_id"]),
                        review_score=score,
                        review_note=",".join(flags)[:500],
                    )
                    review_ids.append(str(review["review_id"]))
            else:
                review = self.repository.create_review_item(
                    canonical_record_id=canonical_record_id,
                    listing_id=listing_id,
                    review_score=score,
                    review_note=(",".join(flags) or "missing_offer")[:500],
                )
                review_ids.append(str(review["review_id"]))

        return MaterializationResult(
            listing_id=str(listing_id),
            public_listing_id=str(public_listing_id),
            offer_ids=tuple(str(row["offer_id"]) for row in offers),
            review_ids=tuple(review_ids),
        )


__all__ = ["InventoryMaterializationService", "MaterializationResult"]

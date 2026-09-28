"""Side-effect-free renter search flow for the V3 User Bot.

The flow owns composition only:

raw text -> SearchCriteria -> published-only search -> search cards

It does not write users/leads, mutate session state, or call Telegram. Normal
search preserves the fixed-SHA one-strict-result carousel enhancement. Explicit
"similar listings" remains a separate method and therefore cannot silently turn
a strict no-match into a relaxed search.
"""
from __future__ import annotations

from dataclasses import dataclass

from .public_search import PublicSearchService, SearchExecution
from .search_cards import SearchCardResponse, build_search_cards
from .search_query import SearchCriteria, parse_search_criteria


@dataclass(frozen=True)
class SearchFlowResult:
    criteria: SearchCriteria
    mode: str
    has_similar: bool
    cards: tuple[SearchCardResponse, ...]
    public_listing_ids: tuple[str, ...]

    @property
    def matched(self) -> bool:
        return bool(self.cards)


class SearchFlowService:
    """Compose already-locked search policy into Telegram-neutral responses."""

    def __init__(self, search: PublicSearchService):
        self.search = search

    @staticmethod
    def _result(criteria: SearchCriteria, execution: SearchExecution) -> SearchFlowResult:
        cards = build_search_cards(execution.items)
        return SearchFlowResult(
            criteria=criteria,
            mode=execution.mode,
            has_similar=execution.has_similar,
            cards=cards,
            public_listing_ids=tuple(card.public_listing_id for card in cards),
        )

    def search_text(self, text: object, *, limit: int = 5) -> SearchFlowResult:
        criteria = parse_search_criteria(str(text or ""))
        execution = self.search.strict_for_results(criteria, limit=limit)
        return self._result(criteria, execution)

    def search_criteria(
        self,
        criteria: SearchCriteria,
        *,
        limit: int = 5,
    ) -> SearchFlowResult:
        """Run normal strict search for an already-collected guided preference."""
        execution = self.search.strict_for_results(criteria, limit=limit)
        return self._result(criteria, execution)

    def similar(
        self,
        criteria: SearchCriteria,
        *,
        limit: int = 5,
    ) -> SearchFlowResult:
        """Run explicit user-requested relaxed search only."""
        execution = self.search.similar(criteria, limit=limit)
        return self._result(criteria, execution)


__all__ = ["SearchFlowResult", "SearchFlowService"]

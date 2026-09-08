"""Pure read-only executor for guided V3 search submission."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .public_search import PublicSearchReader, PublicSearchService
from .search_flow import SearchFlowResult, SearchFlowService
from .transition_actions import SearchSubmitIntent


@dataclass(frozen=True)
class SearchSubmitExecution:
    intent: SearchSubmitIntent
    result: SearchFlowResult


class SearchSubmitExecutor:
    def __init__(self, flow: SearchFlowService):
        self.flow = flow

    def execute(
        self,
        intent: SearchSubmitIntent,
        *,
        limit: int = 5,
    ) -> SearchSubmitExecution:
        result = self.flow.search_criteria(intent.criteria, limit=limit)
        return SearchSubmitExecution(intent=intent, result=result)


def build_sqlite_search_submit_executor(
    db_path: str | Path,
) -> SearchSubmitExecutor:
    """Build the V3 published-only reader chain without schema initialization."""
    search = PublicSearchService(PublicSearchReader(db_path))
    return SearchSubmitExecutor(SearchFlowService(search))


__all__ = [
    "SearchSubmitExecution",
    "SearchSubmitExecutor",
    "build_sqlite_search_submit_executor",
]

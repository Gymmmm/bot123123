"""Dependency wiring for the read-only/public V3 User Bot surface.

This factory composes the already-extracted published inventory, listing,
search, callback, consult, and similar-intent services.  It deliberately does
not initialize schema, import the legacy User Bot database facade, create a
Telegram Application, or register production handlers.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .callback_router import CallbackRouter
from .consult import ConsultService
from .public_flow import PublicListingFlowService
from .public_inventory import PublicInventoryReader
from .public_search import PublicSearchReader, PublicSearchService
from .route_service import PublicRouteService
from .search_flow import SearchFlowService
from .search_session import SearchSessionService
from .similar_intent import SimilarIntentService


@dataclass(frozen=True)
class UserBotReadRuntime:
    db_path: Path
    inventory: PublicInventoryReader
    routes: PublicRouteService
    listings: PublicListingFlowService
    search_reader: PublicSearchReader
    search: PublicSearchService
    search_flow: SearchFlowService
    search_sessions: SearchSessionService
    consults: ConsultService
    similars: SimilarIntentService
    callbacks: CallbackRouter


def build_read_runtime(db_path: str | Path) -> UserBotReadRuntime:
    """Build one side-by-side public runtime without opening or creating the DB."""
    path = Path(db_path).expanduser().resolve()
    inventory = PublicInventoryReader(path)
    routes = PublicRouteService(inventory)
    listings = PublicListingFlowService(routes)
    search_reader = PublicSearchReader(path)
    search = PublicSearchService(search_reader)
    search_flow = SearchFlowService(search)
    search_sessions = SearchSessionService(inventory)
    consults = ConsultService(inventory)
    similars = SimilarIntentService(inventory)
    callbacks = CallbackRouter(
        listings=listings,
        search_sessions=search_sessions,
        consults=consults,
        similars=similars,
    )
    return UserBotReadRuntime(
        db_path=path,
        inventory=inventory,
        routes=routes,
        listings=listings,
        search_reader=search_reader,
        search=search,
        search_flow=search_flow,
        search_sessions=search_sessions,
        consults=consults,
        similars=similars,
        callbacks=callbacks,
    )


__all__ = ["UserBotReadRuntime", "build_read_runtime"]

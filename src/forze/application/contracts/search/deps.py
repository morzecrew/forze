"""Search dependency keys and routers."""

from typing import Any, TypeVar

from pydantic import BaseModel

from ..base import ConfigurableDepPort, ConvenientDeps, DepKey
from .models import FederatedSearchReadModel
from .ports import SearchCommandPort, SearchQueryPort, SearchResultSnapshotPort
from .specs import (
    FederatedSearchSpec,
    HubSearchSpec,
    SearchResultSnapshotSpec,
    SearchSpec,
)

# ----------------------- #

SearchQueryDepPort = ConfigurableDepPort[
    SearchSpec[Any],
    SearchQueryPort[Any],
]
"""Search query dependency port."""

SearchCommandDepPort = ConfigurableDepPort[
    SearchSpec[Any],
    SearchCommandPort[Any],
]
"""Search command dependency port."""

HubSearchQueryDepPort = ConfigurableDepPort[
    HubSearchSpec[Any],
    SearchQueryPort[Any],
]
"""Hub (multi-leg) search query dependency port."""

FederatedSearchQueryDepPort = ConfigurableDepPort[
    FederatedSearchSpec[Any],
    SearchQueryPort[FederatedSearchReadModel[Any]],
]
"""Federated search query dependency port."""

SearchResultSnapshotDepPort = ConfigurableDepPort[
    SearchResultSnapshotSpec,
    SearchResultSnapshotPort,
]
"""Builder for :class:`SearchResultSnapshotPort` (e.g. Redis KV snapshot store)."""

# ....................... #

SearchQueryDepKey = DepKey[SearchQueryDepPort]("search_query")
"""Key used to register the :class:`SearchQueryPort` builder implementation."""

SearchCommandDepKey = DepKey[SearchCommandDepPort]("search_command")
"""Key used to register the :class:`SearchCommandPort` builder implementation."""

HubSearchQueryDepKey = DepKey[HubSearchQueryDepPort]("hub_search_query")
"""Key used to register the hub :class:`SearchQueryPort` builder implementation."""

FederatedSearchQueryDepKey = DepKey[FederatedSearchQueryDepPort](
    "federated_search_query"
)
"""Key used to register the federated :class:`SearchQueryPort` builder implementation."""

SearchResultSnapshotDepKey = DepKey[SearchResultSnapshotDepPort](
    "search_result_snapshot",
)
"""Key used to register the :class:`SearchResultSnapshotPort` implementation."""

# ....................... #

T = TypeVar("T", bound=BaseModel)


class SearchDeps(ConvenientDeps):
    """Convenience wrapper for search dependencies."""

    def query(self, spec: SearchSpec[T]) -> SearchQueryPort[T]:
        """Resolve a search query port for the given spec."""

        ctx = self._require_ctx()

        f = ctx.deps.provide(SearchQueryDepKey, route=spec.name)
        return f(ctx, spec)

    # ....................... #

    def command(self, spec: SearchSpec[T]) -> SearchCommandPort[T]:
        """Resolve a search command port for the given spec."""

        ctx = self._require_ctx()

        f = ctx.deps.provide(SearchCommandDepKey, route=spec.name)
        return f(ctx, spec)

    # ....................... #

    def hub(self, spec: HubSearchSpec[T]) -> SearchQueryPort[T]:
        """Resolve a hub search query port for the given spec."""

        ctx = self._require_ctx()

        f = ctx.deps.provide(HubSearchQueryDepKey, route=spec.name)
        return f(ctx, spec)

    # ....................... #

    def federated(
        self,
        spec: FederatedSearchSpec[T],
    ) -> SearchQueryPort[FederatedSearchReadModel[T]]:
        """Resolve a federated search query port for the given spec."""

        ctx = self._require_ctx()

        f = ctx.deps.provide(FederatedSearchQueryDepKey, route=spec.name)
        return f(ctx, spec)

    # ....................... #

    def snapshot(self, spec: SearchResultSnapshotSpec) -> SearchResultSnapshotPort:
        """Resolve a search result snapshot port for the given spec."""

        ctx = self._require_ctx()

        f = ctx.deps.provide(SearchResultSnapshotDepKey, route=spec.name)
        return f(ctx, spec)

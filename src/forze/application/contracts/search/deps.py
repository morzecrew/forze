"""Search dependency keys and routers."""

from typing import Any, TypeVar

from pydantic import BaseModel

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .models import FederatedSearchReadModel
from .ports import (
    SearchCommandPort,
    SearchManagementPort,
    SearchQueryPort,
    SearchResultSnapshotPort,
)
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

SearchManagementDepPort = ConfigurableDepPort[
    SearchSpec[Any],
    SearchManagementPort,
]
"""Search management (control-plane) dependency port."""

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

SearchManagementDepKey = DepKey[SearchManagementDepPort]("search_management")
"""Key used to register the :class:`SearchManagementPort` builder implementation."""

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

        return self._resolve_configurable(SearchQueryDepKey, spec, route=spec.name)

    # ....................... #

    def command(self, spec: SearchSpec[T]) -> SearchCommandPort[T]:
        """Resolve a search command port for the given spec."""

        return self._resolve_command(
            SearchCommandDepKey,
            spec,
            route=spec.name,
        )

    # ....................... #

    def management(self, spec: SearchSpec[T]) -> SearchManagementPort:
        """Resolve a search management (provisioning) port for the given spec.

        Control-plane: ``ensure_index`` / ``delete_all``. Acquired via the command
        path, so a read-only (``QUERY``) operation cannot provision or wipe an index.
        """

        return self._resolve_command(
            SearchManagementDepKey,
            spec,
            route=spec.name,
        )

    # ....................... #

    def hub(self, spec: HubSearchSpec[T]) -> SearchQueryPort[T]:
        """Resolve a hub search query port for the given spec."""

        return self._resolve_configurable(
            HubSearchQueryDepKey,
            spec,
            route=spec.name,
        )

    # ....................... #

    def federated(
        self,
        spec: FederatedSearchSpec[T],
    ) -> SearchQueryPort[FederatedSearchReadModel[T]]:
        """Resolve a federated search query port for the given spec."""

        return self._resolve_configurable(
            FederatedSearchQueryDepKey,
            spec,
            route=spec.name,
        )

    # ....................... #

    def snapshot(self, spec: SearchResultSnapshotSpec) -> SearchResultSnapshotPort:
        """Resolve a search result snapshot port for the given spec."""

        return self._resolve_configurable(
            SearchResultSnapshotDepKey,
            spec,
            route=spec.name,
        )

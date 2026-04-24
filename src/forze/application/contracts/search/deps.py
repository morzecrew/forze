"""Search dependency keys and routers."""

from typing import Any

from ..base import BaseDepPort, DepKey
from .models import FederatedSearchReadModel
from .ports import SearchCommandPort, SearchQueryPort, SearchResultSnapshotPort
from .specs import (
    FederatedSearchSpec,
    HubSearchSpec,
    SearchResultSnapshotSpec,
    SearchSpec,
)

# ----------------------- #

SearchQueryDepPort = BaseDepPort[SearchSpec[Any], SearchQueryPort[Any]]
"""Search query dependency port."""

SearchCommandDepPort = BaseDepPort[SearchSpec[Any], SearchCommandPort[Any]]
"""Search command dependency port."""

HubSearchQueryDepPort = BaseDepPort[HubSearchSpec[Any], SearchQueryPort[Any]]
"""Hub (multi-leg) search query dependency port."""

FederatedSearchQueryDepPort = BaseDepPort[
    FederatedSearchSpec[Any],
    SearchQueryPort[FederatedSearchReadModel[Any]],
]
"""Federated search query dependency port."""

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

SearchResultSnapshotDepPort = BaseDepPort[SearchResultSnapshotSpec, SearchResultSnapshotPort]
"""Builder for :class:`SearchResultSnapshotPort` (e.g. Redis KV snapshot store)."""

SearchResultSnapshotDepKey = DepKey[SearchResultSnapshotDepPort](
    "search_result_snapshot",
)
"""Key used to register the :class:`SearchResultSnapshotPort` implementation."""

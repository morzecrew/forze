"""Search dependency keys and routers."""

from typing import Any

from ..base import BaseDepPort, DepKey
from .ports import SearchCommandPort, SearchQueryPort
from .specs import HubSearchSpec, SearchSpec

# ----------------------- #

SearchQueryDepPort = BaseDepPort[SearchSpec[Any], SearchQueryPort[Any]]
"""Search query dependency port."""

SearchCommandDepPort = BaseDepPort[SearchSpec[Any], SearchCommandPort[Any]]
"""Search command dependency port."""

HubSearchQueryDepPort = BaseDepPort[HubSearchSpec[Any], SearchQueryPort[Any]]
"""Hub (multi-leg) search query dependency port."""

SearchQueryDepKey = DepKey[SearchQueryDepPort]("search_query")
"""Key used to register the :class:`SearchQueryPort` builder implementation."""

SearchCommandDepKey = DepKey[SearchCommandDepPort]("search_command")
"""Key used to register the :class:`SearchCommandPort` builder implementation."""

HubSearchQueryDepKey = DepKey[HubSearchQueryDepPort]("hub_search_query")
"""Key used to register the hub :class:`SearchQueryPort` builder implementation."""

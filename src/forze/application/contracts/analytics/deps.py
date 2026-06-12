"""Analytics dependency keys and routers."""

from typing import Any, TypeVar

from pydantic import BaseModel

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import AnalyticsIngestPort, AnalyticsQueryPort
from .specs import AnalyticsSpec

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
Ing = TypeVar("Ing", bound=BaseModel)

# ....................... #

AnalyticsQueryDepPort = ConfigurableDepPort[
    AnalyticsSpec[Any, Any],
    AnalyticsQueryPort[Any],
]
"""Analytics query dependency port."""

AnalyticsIngestDepPort = ConfigurableDepPort[
    AnalyticsSpec[Any, Any],
    AnalyticsIngestPort[Any],
]
"""Analytics ingest dependency port."""

# ....................... #

AnalyticsQueryDepKey = DepKey[AnalyticsQueryDepPort]("analytics_query")
"""Key used to register the :class:`AnalyticsQueryPort` builder implementation."""

AnalyticsIngestDepKey = DepKey[AnalyticsIngestDepPort]("analytics_ingest")
"""Key used to register the :class:`AnalyticsIngestPort` builder implementation."""

# ....................... #


class AnalyticsDeps(ConvenientDeps):
    """Convenience wrapper for analytics dependencies."""

    def query(self, spec: AnalyticsSpec[R, Any]) -> AnalyticsQueryPort[R]:
        """Resolve an analytics query port for the given spec."""

        return self._resolve_configurable(
            AnalyticsQueryDepKey,
            spec,
            route=spec.name,
        )

    # ....................... #

    def ingest(self, spec: AnalyticsSpec[Any, Ing]) -> AnalyticsIngestPort[Ing]:
        """Resolve an analytics ingest port for the given spec (a write — guarded)."""

        return self._resolve_command(
            AnalyticsIngestDepKey,
            spec,
            route=spec.name,
        )

    # ....................... #

    def command(self, spec: AnalyticsSpec[Any, Ing]) -> AnalyticsIngestPort[Ing]:
        """Alias for :meth:`ingest`, for accessor-shape consistency across deps.

        Other convenient deps expose ``query``/``command`` pairs; ``ingest``
        remains the domain-precise name for the analytics write side. Both
        resolve the same port and carry the same write guard.
        """

        return self.ingest(spec)

"""Dependency keys, factory protocols, and resolver for graph module ports."""

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import GraphCommandPort, GraphQueryPort, GraphRawQueryPort
from .specs import GraphModuleSpec

# ----------------------- #

GraphQueryDepPort = ConfigurableDepPort[GraphModuleSpec, GraphQueryPort]
"""Graph query dependency port."""

GraphCommandDepPort = ConfigurableDepPort[GraphModuleSpec, GraphCommandPort]
"""Graph command dependency port."""

GraphRawQueryDepPort = ConfigurableDepPort[GraphModuleSpec, GraphRawQueryPort]
"""Graph raw-query (escape hatch) dependency port."""

# ....................... #

GraphQueryDepKey = DepKey[GraphQueryDepPort]("graph_query")
"""Key to register a ``GraphQueryDepPort`` implementation."""

GraphCommandDepKey = DepKey[GraphCommandDepPort]("graph_command")
"""Key to register a ``GraphCommandDepPort`` implementation."""

GraphRawQueryDepKey = DepKey[GraphRawQueryDepPort]("graph_raw_query")
"""Key to register a ``GraphRawQueryDepPort`` implementation (opt-in)."""

# ....................... #


class GraphDeps(ConvenientDeps):
    """Convenience wrapper for graph dependencies."""

    def query(self, spec: GraphModuleSpec) -> GraphQueryPort:
        """Resolve a graph query port for the given module spec."""

        return self._resolve_configurable(GraphQueryDepKey, spec, route=spec.name)

    # ....................... #

    def command(self, spec: GraphModuleSpec) -> GraphCommandPort:
        """Resolve a graph command port for the given module spec."""

        return self._resolve_command(GraphCommandDepKey, spec, route=spec.name)

    # ....................... #

    def raw(self, spec: GraphModuleSpec) -> GraphRawQueryPort:
        """Resolve the engine-specific raw-query escape hatch (opt-in, non-portable)."""

        return self._resolve_configurable(GraphRawQueryDepKey, spec, route=spec.name)

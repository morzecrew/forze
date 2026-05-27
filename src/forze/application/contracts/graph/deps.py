"""Dependency keys and factory protocols for graph module ports."""

from ..deps import ConfigurableDepPort, DepKey
from .ports import GraphCommandPort, GraphQueryPort
from .specs import GraphModuleSpec

# ----------------------- #

GraphQueryDepPort = ConfigurableDepPort[GraphModuleSpec, GraphQueryPort]
"""Graph query dependency port."""

GraphCommandDepPort = ConfigurableDepPort[GraphModuleSpec, GraphCommandPort]
"""Graph command dependency port."""

# ....................... #

GraphQueryDepKey = DepKey[GraphQueryDepPort]("graph_query")
"""Key to register a ``GraphQueryDepPort`` implementation."""

GraphCommandDepKey = DepKey[GraphCommandDepPort]("graph_command")
"""Key to register a ``GraphCommandDepPort`` implementation."""

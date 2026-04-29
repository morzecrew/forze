"""Dependency keys and factory protocols for graph module ports."""

from ..base import BaseDepPort, DepKey
from .ports import GraphCommandPort, GraphQueryPort
from .specs import GraphModuleSpec

# ----------------------- #

GraphQueryDepPort = BaseDepPort[GraphModuleSpec, GraphQueryPort]
"""Graph query dependency port."""

GraphCommandDepPort = BaseDepPort[GraphModuleSpec, GraphCommandPort]
"""Graph command dependency port."""

# ....................... #

GraphQueryDepKey = DepKey[GraphQueryDepPort]("graph_query")
"""Key to register a :class:`GraphQueryDepPort` implementation."""

GraphCommandDepKey = DepKey[GraphCommandDepPort]("graph_command")
"""Key to register a :class:`GraphCommandDepPort` implementation."""

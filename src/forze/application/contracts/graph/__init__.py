"""Bounded graph module contracts: node/edge specs, refs, and ports."""

from .deps import (
    GraphCommandDepKey,
    GraphCommandDepPort,
    GraphDeps,
    GraphQueryDepKey,
    GraphQueryDepPort,
    GraphRawQueryDepKey,
    GraphRawQueryDepPort,
)
from .ports import (
    BaseGraphModulePort,
    GraphCommandPort,
    GraphQueryPort,
    GraphRawQueryPort,
)
from .specs import (
    GraphEdgeIdentity,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
    resolve_query_directions,
    validate_graph_module_spec,
)
from .types import GraphDirection, GraphEdgeDirectionality
from .value_objects import (
    EdgeRef,
    GraphEdgeEndpoint,
    GraphWalkParams,
    GraphWalkStep,
    NeighborRow,
    ShortestPathParams,
    ShortestPathResult,
    VertexRef,
)

# ----------------------- #

__all__ = [
    "BaseGraphModulePort",
    "EdgeRef",
    "GraphCommandDepKey",
    "GraphCommandDepPort",
    "GraphCommandPort",
    "GraphDeps",
    "GraphDirection",
    "GraphEdgeDirectionality",
    "GraphEdgeEndpoint",
    "GraphEdgeIdentity",
    "GraphEdgeSpec",
    "GraphModuleSpec",
    "GraphNodeSpec",
    "GraphQueryDepKey",
    "GraphQueryDepPort",
    "GraphQueryPort",
    "GraphRawQueryDepKey",
    "GraphRawQueryDepPort",
    "GraphRawQueryPort",
    "GraphWalkParams",
    "GraphWalkStep",
    "NeighborRow",
    "ShortestPathParams",
    "ShortestPathResult",
    "VertexRef",
    "resolve_query_directions",
    "validate_graph_module_spec",
]

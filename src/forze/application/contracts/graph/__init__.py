"""Bounded graph module contracts: node/edge specs, refs, and ports."""

#! TODO: review and repurpose properly

from .deps import (
    GraphCommandDepKey,
    GraphCommandDepPort,
    GraphQueryDepKey,
    GraphQueryDepPort,
)
from .ports import BaseGraphModulePort, GraphCommandPort, GraphQueryPort
from .specs import (
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
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
    "GraphDirection",
    "GraphEdgeDirectionality",
    "GraphEdgeEndpoint",
    "GraphEdgeSpec",
    "GraphModuleSpec",
    "GraphNodeSpec",
    "GraphQueryDepKey",
    "GraphQueryDepPort",
    "GraphQueryPort",
    "GraphWalkParams",
    "GraphWalkStep",
    "NeighborRow",
    "ShortestPathParams",
    "ShortestPathResult",
    "VertexRef",
    "validate_graph_module_spec",
]

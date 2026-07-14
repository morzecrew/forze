"""Bounded graph module contracts: node/edge specs, refs, and ports."""

from .capabilities import GraphReadCapabilities, GraphStreamingAware
from .deps import (
    GraphCommandDepKey,
    GraphCommandDepPort,
    GraphDeps,
    GraphManagementDepKey,
    GraphManagementDepPort,
    GraphQueryDepKey,
    GraphQueryDepPort,
    GraphRawQueryDepKey,
    GraphRawQueryDepPort,
)
from .filters import is_valid_filter_key, validate_property_filter_keys
from .ports import (
    BaseGraphModulePort,
    GraphCommandPort,
    GraphManagementPort,
    GraphQueryPort,
    GraphRawQueryPort,
)
from .specs import (
    GraphEdgeIdentity,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
    assert_key_field_not_sealed,
    resolve_query_directions,
    validate_graph_module_spec,
)
from .streamable import (
    edge_cursor_fields,
    edge_stream_blocker,
    endpoint_key_fields,
    graph_stream_blockers,
    vertex_stream_blocker,
)
from .types import GraphDirection, GraphEdgeDirectionality
from .value_objects import (
    EdgeRef,
    GraphEdgeEndpoint,
    GraphPathStep,
    GraphWalkParams,
    GraphWalkStep,
    NeighborRow,
    ScopedWalkParams,
    ShortestPathParams,
    ShortestPathResult,
    VertexRef,
)

# ----------------------- #

__all__ = [
    "GraphReadCapabilities",
    "GraphStreamingAware",
    "edge_cursor_fields",
    "edge_stream_blocker",
    "endpoint_key_fields",
    "graph_stream_blockers",
    "vertex_stream_blocker",
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
    "GraphManagementDepKey",
    "GraphManagementDepPort",
    "GraphManagementPort",
    "GraphModuleSpec",
    "GraphNodeSpec",
    "GraphQueryDepKey",
    "GraphQueryDepPort",
    "GraphQueryPort",
    "GraphRawQueryDepKey",
    "GraphRawQueryDepPort",
    "GraphRawQueryPort",
    "GraphPathStep",
    "GraphWalkParams",
    "GraphWalkStep",
    "NeighborRow",
    "ScopedWalkParams",
    "ShortestPathParams",
    "ShortestPathResult",
    "VertexRef",
    "is_valid_filter_key",
    "resolve_query_directions",
    "assert_key_field_not_sealed",
    "validate_graph_module_spec",
    "validate_property_filter_keys",
]

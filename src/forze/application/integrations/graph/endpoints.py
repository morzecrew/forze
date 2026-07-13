"""Resolve which declared endpoint pair an edge create/ensure targets.

Shared by every graph adapter so the routing rule is identical across backends (and the
adapter-conformance harness can rely on it).
"""

from typing import Any

from forze.application.contracts.graph import GraphEdgeEndpoint, GraphEdgeSpec
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #


def resolve_write_endpoint(edge: GraphEdgeSpec[Any], data: JsonDict) -> GraphEdgeEndpoint:
    """Pick the ``(from_kind, to_kind)`` pair for an edge create/ensure, popping routing hints.

    A **single-endpoint** kind resolves to its only pair. A **multi-endpoint** kind (several
    declared ``(from, to)`` pairs) needs the create command to name the pair via ``from_kind``
    / ``to_kind`` — these are transient routing fields (like ``from_key`` / ``to_key``), popped
    from *data* so they are never stored as edge properties, and the named pair must be one the
    spec declares.

    :raises CoreException: ``graph_edge_endpoint_kind_required`` when a multi-endpoint create
        omits the kinds; ``graph_edge_unknown_endpoint`` when the named pair is not declared.
    """

    from_kind = data.pop("from_kind", None)
    to_kind = data.pop("to_kind", None)

    if len(edge.endpoints) == 1:
        return edge.endpoints[0]

    if from_kind is None or to_kind is None:
        raise exc.validation(
            f"Multi-endpoint edge {edge.name!r} create must name its endpoints via "
            "'from_kind' and 'to_kind'.",
            code="graph_edge_endpoint_kind_required",
        )

    for endpoint in edge.endpoints:
        if endpoint.from_kind == from_kind and endpoint.to_kind == to_kind:
            return endpoint

    raise exc.validation(
        f"({from_kind!r}, {to_kind!r}) is not a declared endpoint pair of edge {edge.name!r}.",
        code="graph_edge_unknown_endpoint",
    )

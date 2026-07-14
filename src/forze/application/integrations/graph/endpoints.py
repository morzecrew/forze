"""Resolve which declared endpoint pair an edge create/ensure targets, and enforce its identity.

Shared by every graph adapter so the routing rule is identical across backends (and the
adapter-conformance harness can rely on it).
"""

from typing import Any

from forze.application.contracts.graph import GraphEdgeEndpoint, GraphEdgeSpec
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #

ENDPOINTS_CONFLICT_CODE = "graph_edge_endpoints_conflict"


def endpoints_conflict(edge_kind: str, from_key: Any, to_key: Any) -> Exception:
    """The error a ``create_edge`` raises when an endpoint-identified pair is already taken.

    Shared so both backends say the same thing, and worded to offer **both** ways out — because
    which one is right is a modelling question the framework cannot answer, and the message is
    the only moment it gets to ask it.

    The tempting wording ("use ensure_edge or update_edge") quietly assumes the author's *model*
    is right and only their *call* is wrong. That assumption is not safe: forze's own weighted-
    path tests declared ``identity="endpoints"`` on a kind that deliberately carries **parallel**
    edges between the same pair (two routes of different weight — a perfectly good graph, and one
    that declaration forbids). If the framework's own tests landed on the wrong side of this, an
    error that only offers one side will send the next person the wrong way.
    """

    return exc.conflict(
        f"Edge kind {edge_kind!r} is declared identity='endpoints', so at most one of its edges "
        f"exists per (from, to) pair — and {from_key} -> {to_key} already has one. If that is "
        f"the model, use ensure_edge to leave the existing edge alone, or update_edge to change "
        f"it. If two of these edges can legitimately run between the same pair (two flights "
        f"between two cities, two roads between two towns), then they are distinct entities and "
        f"the kind is mis-declared: give it identity='key' and a key_field, because an edge that "
        f"is a distinct entity needs a key to be one — under identity='endpoints' it has no "
        f"identity at all, and get_edge would return an arbitrary one of them.",
        code=ENDPOINTS_CONFLICT_CODE,
    )


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

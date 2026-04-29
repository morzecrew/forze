"""References and result shapes for graph read operations."""

from typing import final

import attrs
from pydantic import BaseModel

from .types import GraphDirection

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GraphEdgeEndpoint:
    """One allowed (tail, head) pair for an edge kind.

    ``from_kind`` and ``to_kind`` are logical node names: they must match
    :attr:`GraphNodeSpec.name` entries in the enclosing :class:`GraphModuleSpec`.
    """

    from_kind: str
    """Logical name of the tail vertex kind."""

    to_kind: str
    """Logical name of the head vertex kind."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class VertexRef:
    """Opaque pointer to a vertex within a graph module."""

    kind: str
    """Logical vertex kind; matches :attr:`GraphNodeSpec.name` in the module spec."""

    key: str
    """Engine-specific stable key (e.g. document key, business key) — adapter-defined."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class EdgeRef:
    """Opaque pointer to an edge within a graph module."""

    kind: str
    """Logical edge kind; matches :attr:`GraphEdgeSpec.name` in the module spec."""

    key: str
    """Engine-specific stable key for the relationship or edge document."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class NeighborRow:
    """One neighbor reachable in one hop from an origin vertex."""

    other: BaseModel
    """The vertex at the far end, typed per the target node :class:`GraphNodeSpec.read`."""

    via_edge: BaseModel
    """The connecting edge, typed per :class:`GraphEdgeSpec.read`."""

    direction: GraphDirection
    """How this hop was traversed from the origin (outgoing, incoming, or both semantics)."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GraphWalkParams:
    """Bounds for :meth:`GraphQueryPort.expand`."""

    max_depth: int
    """Maximum hops from the start vertex (>= 1)."""

    max_results: int
    """Maximum total returned steps or frontier size — adapter defines which (document both)."""

    direction: GraphDirection = GraphDirection.BOTH
    """Traversal direction for each hop."""

    edge_kinds: frozenset[str] = frozenset()
    """
    If non-empty, only these logical edge :attr:`GraphEdgeSpec.name` values may be followed.
    If empty, the adapter may follow all edge kinds in the spec (subject to ``max_depth``/limits).
    """


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GraphWalkStep:
    """A single row in a bounded expansion."""

    depth: int
    """Hop count from the start (0 = start vertex)."""

    vertex: BaseModel
    """Vertex at this step, typed per the matching :class:`GraphNodeSpec.read`."""

    from_parent: BaseModel | None
    """Edge from the parent vertex, or ``None`` at the root step."""

    parent_ref: VertexRef | None
    """Reference to the parent vertex, or ``None`` at the root step."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ShortestPathParams:
    """Parameters for :meth:`GraphQueryPort.shortest_path`."""

    max_hops: int
    """Upper bound on path length (number of edges)."""

    max_paths: int = 1
    """How many distinct paths to return (if the backend supports k shortest paths)."""

    edge_kinds: frozenset[str] = frozenset()
    """
    If non-empty, only these logical edge kinds may appear on the path; if empty, all
    allowed kinds in the module spec are considered.
    """


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ShortestPathResult:
    """A simple path as parallel vertex and edge sequences."""

    vertices: tuple[BaseModel, ...]
    """Ordered vertices; length ``n + 1`` for a path with ``n`` edges."""

    edges: tuple[BaseModel, ...]
    """Ordered edges; length ``n``; ``edges[i]`` connects ``vertices[i]`` to ``vertices[i + 1]``."""

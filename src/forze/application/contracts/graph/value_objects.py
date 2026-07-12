"""References and result shapes for graph read operations."""

from typing import final

import attrs
from pydantic import BaseModel

from forze.base.exceptions import exc

from .types import GraphDirection

# ----------------------- #


def _require_int(value: object, *, name: str, minimum: int, code: str) -> None:
    """Fail closed on a non-integer or out-of-range traversal bound.

    These bounds end up inlined into backend query text (e.g. a Cypher ``*1..n``
    quantifier cannot be parameterized), so a loosely-typed caller's value must be
    rejected here — not silently truncated or interpolated downstream.
    """

    if isinstance(value, bool) or not isinstance(value, int):
        raise exc.validation(
            f"{name} must be an integer, got {type(value).__name__}", code=code
        )

    if value < minimum:
        raise exc.validation(f"{name} must be >= {minimum}", code=code)


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
    """Pointer to an edge within a graph module, in one of two addressing modes.

    The mode must match the edge kind's :attr:`GraphEdgeSpec.identity`:

    - **key mode** (:meth:`by_key`): a stable per-edge key — an ArangoDB ``_key`` or a
      Neo4j business-key property. Use when edges carry their own identity (and for
      multigraphs, where several edges of one kind may join the same pair).
    - **endpoints mode** (:meth:`by_endpoints`): identifies the at-most-one edge of
      ``kind`` between ``from_ref`` and ``to_ref`` — maps to a Cypher ``MERGE`` by
      endpoints / an ArangoDB unique index on ``[_from, _to]``. Use for simple
      relationships that have no business key.
    """

    kind: str
    """Logical edge kind; matches :attr:`GraphEdgeSpec.name` in the module spec."""

    key: str | None = None
    """Stable per-edge key (key mode); ``None`` in endpoints mode."""

    from_ref: VertexRef | None = None
    """Tail vertex (endpoints mode); ``None`` in key mode."""

    to_ref: VertexRef | None = None
    """Head vertex (endpoints mode); ``None`` in key mode."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        has_key = self.key is not None
        has_endpoints = self.from_ref is not None or self.to_ref is not None

        if has_key and has_endpoints:
            raise exc.validation(
                "EdgeRef accepts a key or a (from_ref, to_ref) pair, not both",
                code="graph_edge_ref_mode",
            )

        if not has_key and not has_endpoints:
            raise exc.validation(
                "EdgeRef requires either a key or a (from_ref, to_ref) pair",
                code="graph_edge_ref_mode",
            )

        if has_endpoints and (self.from_ref is None or self.to_ref is None):
            raise exc.validation(
                "EdgeRef endpoints mode requires both from_ref and to_ref",
                code="graph_edge_ref_endpoints",
            )

    # ....................... #

    @classmethod
    def by_key(cls, kind: str, key: str) -> "EdgeRef":
        """Build a key-mode edge ref."""

        return cls(kind=kind, key=key)

    # ....................... #

    @classmethod
    def by_endpoints(
        cls,
        kind: str,
        from_ref: "VertexRef",
        to_ref: "VertexRef",
    ) -> "EdgeRef":
        """Build an endpoints-mode edge ref (at-most-one edge of ``kind`` per pair)."""

        return cls(kind=kind, from_ref=from_ref, to_ref=to_ref)

    # ....................... #

    @property
    def is_keyed(self) -> bool:
        """Whether this ref is in key mode (``True``) or endpoints mode (``False``)."""

        return self.key is not None


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
    """Maximum total returned steps or frontier size — adapter defines which (document both).
    Must be >= 1."""

    direction: GraphDirection = GraphDirection.BOTH
    """Traversal direction for each hop."""

    edge_kinds: frozenset[str] = frozenset()
    """
    If non-empty, only these logical edge :attr:`GraphEdgeSpec.name` values may be followed.
    If empty, the adapter may follow all edge kinds in the spec (subject to ``max_depth``/limits).
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        _require_int(
            self.max_depth,
            name="GraphWalkParams.max_depth",
            minimum=1,
            code="graph_walk_params_bounds",
        )
        _require_int(
            self.max_results,
            name="GraphWalkParams.max_results",
            minimum=1,
            code="graph_walk_params_bounds",
        )


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
class GraphPathStep:
    """One variable-length segment of a :class:`ScopedWalkParams` traversal."""

    edge_kinds: frozenset[str] = frozenset()
    """Logical edge kinds allowed on this segment; empty = any kind in the spec."""

    direction: GraphDirection = GraphDirection.OUT
    """Direction for this segment's hops."""

    min_hops: int = 1
    """Minimum hops for this segment (``>= 0``)."""

    max_hops: int = 1
    """Maximum hops for this segment (``>= min_hops``)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        _require_int(
            self.min_hops,
            name="GraphPathStep.min_hops",
            minimum=0,
            code="graph_path_step_bounds",
        )
        _require_int(
            self.max_hops,
            name="GraphPathStep.max_hops",
            minimum=self.min_hops,
            code="graph_path_step_bounds",
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ScopedWalkParams:
    """Parameters for :meth:`GraphQueryPort.scoped_walk` — a tenant-safe multi-segment walk.

    The traversal is fully adapter-owned and tenant-scoped end to end (anchor, every
    intermediate node, and the typed target), so it is a safe alternative to the raw hatch
    for multi-segment patterns the fixed traversal ports cannot express.
    """

    steps: tuple[GraphPathStep, ...] = attrs.field(
        converter=tuple,
        validator=attrs.validators.deep_iterable(
            member_validator=attrs.validators.instance_of(GraphPathStep),
        ),
    )
    """Ordered segments; each is a variable-length hop chained to the next."""

    target_kind: str
    """Logical node kind of the terminal vertex; results are typed per its ``read`` model."""

    limit: int = 100
    """Maximum number of distinct target vertices returned."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.steps:
            raise exc.validation(
                "ScopedWalkParams requires at least one step",
                code="graph_scoped_walk_steps",
            )

        _require_int(
            self.limit,
            name="ScopedWalkParams.limit",
            minimum=1,
            code="graph_scoped_walk_limit",
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ShortestPathParams:
    """Parameters for :meth:`GraphQueryPort.shortest_path` and ``k_shortest_paths``.

    The number of paths is **not** a field here: :meth:`GraphQueryPort.shortest_path`
    returns a single path, and :meth:`GraphQueryPort.k_shortest_paths` takes an explicit
    ``k``. This keeps each method's return type unambiguous.
    """

    max_hops: int
    """Upper bound on path length (number of edges); must be >= 0 (with 0, only a
    zero-length path — from == to — can qualify)."""

    edge_kinds: frozenset[str] = frozenset()
    """
    If non-empty, only these logical edge kinds may appear on the path; if empty, all
    allowed kinds in the module spec are considered.
    """

    weight_property: str | None = attrs.field(default=None)
    """Optional edge property to minimize (a *weighted* shortest path).

    ``None`` (default) = unweighted, shortest by hop count. When set, the path minimizes the
    sum of this numeric edge property; it must exist on every traversed edge kind. Weighted
    paths require a backend with a graph-algorithms engine (Neo4j GDS); a backend without one
    rejects the request (``graph_algorithm_unavailable``). ``max_hops`` bounds the search: the
    cheapest path using at most ``max_hops`` edges is returned — a cheaper path that exists only
    beyond the bound does not suppress a valid bounded one."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        _require_int(
            self.max_hops,
            name="ShortestPathParams.max_hops",
            minimum=0,
            code="graph_shortest_path_bounds",
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ShortestPathResult:
    """A simple path as parallel vertex and edge sequences."""

    vertices: tuple[BaseModel, ...]
    """Ordered vertices; length ``n + 1`` for a path with ``n`` edges."""

    edges: tuple[BaseModel, ...]
    """Ordered edges; length ``n``; ``edges[i]`` connects ``vertices[i]`` to ``vertices[i + 1]``."""

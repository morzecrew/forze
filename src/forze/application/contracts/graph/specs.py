"""Declarative graph module, node, and edge specifications."""

from typing import Literal, final

import attrs
from pydantic import BaseModel

from forze.base.exceptions import exc

from ..base import BaseSpec
from .types import GraphDirection, GraphEdgeDirectionality
from .value_objects import GraphEdgeEndpoint

# ----------------------- #

GraphEdgeIdentity = Literal["key", "endpoints"]
"""How an edge kind is addressed by :class:`~forze.application.contracts.graph.EdgeRef`.

``"key"`` — each edge has a stable business key (the :attr:`GraphEdgeSpec.key_field`
property; an ArangoDB ``_key`` or a Neo4j property). ``"endpoints"`` — at most one edge
of this kind per ``(from, to)`` pair, addressed by its endpoints (Cypher ``MERGE`` /
ArangoDB unique ``[_from, _to]`` index).
"""

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GraphNodeSpec[R: BaseModel](BaseSpec):
    """One vertex (node) kind in a ``GraphModuleSpec``.

    The ``name`` (a :class:`~forze.application.contracts.base.BaseSpec` field) is the
    vertex kind — the primary label (Neo4j) or collection (ArangoDB). Multi-label
    nodes are out of scope for now.
    """

    read: type[R]
    """Read DTO for vertices of this kind."""

    key_field: str = attrs.field(default="id")
    """Name of the ``read`` field that supplies :attr:`VertexRef.key` (defaults to ``id``)."""

    create: type[BaseModel] | None = attrs.field(default=None)
    """Optional create command DTO; when set, commands can create this kind."""

    update: type[BaseModel] | None = attrs.field(default=None)
    """Optional update/patch DTO; when set, commands can update by ref."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GraphEdgeSpec[R: BaseModel](BaseSpec):
    """One edge (relationship) kind, possibly with several allowed endpoint pairs."""

    read: type[R]
    """Read DTO for edges of this kind (relationship or edge document)."""

    identity: GraphEdgeIdentity = attrs.field(default="key")
    """How edges of this kind are addressed by ``EdgeRef`` (``"key"`` or ``"endpoints"``)."""

    key_field: str | None = attrs.field(default=None)
    """Name of the ``read`` field supplying :attr:`EdgeRef.key`; required when ``identity="key"``."""

    endpoints: tuple[GraphEdgeEndpoint, ...]
    """
    Allowed tail/head node kind pairs. Logical names must match
    ``GraphNodeSpec.name`` entries in the same ``GraphModuleSpec``.
    Use more than one pair when a single logical edge kind links different
    node kinds (e.g. one ``TAGGED`` kind from ``Post``→``Tag`` and ``Note``→``Tag``).
    """

    directionality: GraphEdgeDirectionality
    """``~GraphEdgeDirectionality.DIRECTED`` for a canonical tail→head edge;
    ``GraphEdgeDirectionality.SYMMETRIC`` for semantically undirected links."""

    query_directions: frozenset[GraphDirection] | None = attrs.field(default=None)
    """
    Allowed directions for neighborhood and walk queries over this kind.

    If ``None``, adapters derive defaults (e.g. both ``OUT`` and ``IN`` for
    ``GraphEdgeDirectionality.DIRECTED``, and ``GraphDirection.BOTH``
    for ``GraphEdgeDirectionality.SYMMETRIC``).
    """


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GraphModuleSpec(BaseSpec):
    """Bounded-context graph: a module-level bundle of node and edge kinds.

    The module ``name`` identifies this graph area in the application; each
    ``GraphNodeSpec`` / ``GraphEdgeSpec`` ``BaseSpec.name`` is a
    logical *kind* name used by refs and port methods.
    """

    nodes: tuple[GraphNodeSpec[BaseModel], ...]
    """All vertex kinds in this module."""

    edges: tuple[GraphEdgeSpec[BaseModel], ...]
    """All edge kinds in this module."""

    # ....................... #

    def graph_node_by_kind(self, kind: str) -> GraphNodeSpec[BaseModel] | None:
        """Return the ``GraphNodeSpec`` whose name matches *kind*, or ``None``."""

        for n in self.nodes:
            if _kind_key(n.name) == kind:
                return n

        return None

    # ....................... #

    def graph_edge_by_kind(self, kind: str) -> GraphEdgeSpec[BaseModel] | None:
        """Return the ``GraphEdgeSpec`` whose name matches *kind*, or ``None``."""

        for e in self.edges:
            if _kind_key(e.name) == kind:
                return e

        return None


# ....................... #


def _kind_key(name: object) -> str:
    return str(name)


# ....................... #


def _model_has_field(model: type[BaseModel], field: str) -> bool:
    return field in model.model_fields


# ....................... #


def resolve_query_directions(edge: GraphEdgeSpec[BaseModel]) -> frozenset[GraphDirection]:
    """Resolve the directions a kind may be traversed, applying canonical defaults.

    Returns :attr:`GraphEdgeSpec.query_directions` verbatim when set, otherwise derives
    the default: ``DIRECTED`` → ``{OUT, IN}``; ``SYMMETRIC`` → ``{BOTH}``. Centralising
    this keeps adapters from deriving defaults divergently.
    """

    if edge.query_directions is not None:
        return edge.query_directions

    if edge.directionality is GraphEdgeDirectionality.SYMMETRIC:
        return frozenset({GraphDirection.BOTH})

    return frozenset({GraphDirection.OUT, GraphDirection.IN})


# ....................... #


def validate_graph_module_spec(
    spec: GraphModuleSpec,
    *,
    require_non_empty_nodes: bool = True,
) -> None:
    """Check internal consistency; raise a ``configuration`` :class:`CoreException` on violation.

    :param spec: Module to validate.
    :param require_non_empty_nodes: When ``True``, ``spec.nodes`` must be non-empty.
    :raises CoreException: duplicate kind names, unknown endpoint kinds, empty endpoints,
        a keyed edge without ``key_field``, or a ``key_field`` absent from the read model.
    """

    if require_non_empty_nodes and not spec.nodes:
        raise exc.configuration(
            "GraphModuleSpec.nodes must be non-empty when require_non_empty_nodes is True",
            code="graph_spec_empty_nodes",
        )

    node_kinds: set[str] = set()

    for n in spec.nodes:
        k = _kind_key(n.name)

        if k in node_kinds:
            raise exc.configuration(
                f"Duplicate graph node kind name: {k!r}",
                code="graph_spec_duplicate_node",
            )
        node_kinds.add(k)

        if not _model_has_field(n.read, n.key_field):
            raise exc.configuration(
                f"GraphNodeSpec {k!r} key_field {n.key_field!r} is not a field of its read model",
                code="graph_spec_missing_key_field",
            )

    edge_kinds: set[str] = set()

    for e in spec.edges:
        ek = _kind_key(e.name)

        if ek in edge_kinds:
            raise exc.configuration(
                f"Duplicate graph edge kind name: {ek!r}",
                code="graph_spec_duplicate_edge",
            )
        edge_kinds.add(ek)

        if not e.endpoints:
            raise exc.configuration(
                f"GraphEdgeSpec {ek!r} must declare at least one GraphEdgeEndpoint",
                code="graph_spec_empty_endpoints",
            )

        if e.identity == "key":
            if e.key_field is None:
                raise exc.configuration(
                    f"GraphEdgeSpec {ek!r} uses identity='key' but declares no key_field",
                    code="graph_spec_missing_key_field",
                )
            if not _model_has_field(e.read, e.key_field):
                raise exc.configuration(
                    f"GraphEdgeSpec {ek!r} key_field {e.key_field!r} is not a field of its read model",
                    code="graph_spec_missing_key_field",
                )

        for end in e.endpoints:
            if end.from_kind not in node_kinds:
                raise exc.configuration(
                    f"GraphEdgeSpec {ek!r} references unknown from_kind {end.from_kind!r} "
                    f"(not in GraphModuleSpec.nodes)",
                    code="graph_spec_unknown_endpoint",
                )
            if end.to_kind not in node_kinds:
                raise exc.configuration(
                    f"GraphEdgeSpec {ek!r} references unknown to_kind {end.to_kind!r} "
                    f"(not in GraphModuleSpec.nodes)",
                    code="graph_spec_unknown_endpoint",
                )

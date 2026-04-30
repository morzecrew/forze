"""Declarative graph module, node, and edge specifications."""

from typing import final

import attrs
from pydantic import BaseModel

from ..base import BaseSpec
from .types import GraphDirection, GraphEdgeDirectionality
from .value_objects import GraphEdgeEndpoint

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GraphNodeSpec[R: BaseModel](BaseSpec):
    """One vertex (node) kind in a ``GraphModuleSpec``."""

    read: type[R]
    """Read DTO for vertices of this kind."""

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
#! TODO: replace value errors with core errors or so


def _kind_key(name: object) -> str:
    return str(name)


def validate_graph_module_spec(
    spec: GraphModuleSpec,
    *,
    require_non_empty_nodes: bool = True,
) -> None:
    """Check internal consistency; raise ``ValueError`` on violation.

    :param spec: Module to validate.
    :param require_non_empty_nodes: When ``True``, ``spec.nodes`` must be non-empty.
    :raises ValueError: duplicate kind names, unknown endpoint kinds, or empty endpoints.
    """

    if require_non_empty_nodes and not spec.nodes:
        msg = "GraphModuleSpec.nodes must be non-empty when require_non_empty_nodes is True"
        raise ValueError(msg)

    node_kinds: set[str] = set()

    for n in spec.nodes:
        k = _kind_key(n.name)

        if k in node_kinds:
            msg = f"Duplicate graph node kind name: {k!r}"
            raise ValueError(msg)
        node_kinds.add(k)

    edge_kinds: set[str] = set()

    for e in spec.edges:
        ek = _kind_key(e.name)

        if ek in edge_kinds:
            msg = f"Duplicate graph edge kind name: {ek!r}"
            raise ValueError(msg)
        edge_kinds.add(ek)

        if not e.endpoints:
            msg = f"GraphEdgeSpec {ek!r} must declare at least one GraphEdgeEndpoint"
            raise ValueError(msg)

        for end in e.endpoints:
            if end.from_kind not in node_kinds:
                msg = (
                    f"GraphEdgeSpec {ek!r} references unknown from_kind {end.from_kind!r} "
                    f"(not in GraphModuleSpec.nodes)"
                )
                raise ValueError(msg)
            if end.to_kind not in node_kinds:
                msg = (
                    f"GraphEdgeSpec {ek!r} references unknown to_kind {end.to_kind!r} "
                    f"(not in GraphModuleSpec.nodes)"
                )
                raise ValueError(msg)

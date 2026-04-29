"""Query and command ports for a bounded graph module.

Ports are intentionally free of Cypher, AQL, and other engine-specific query
strings; adapters map these operations to the underlying graph database.
"""

from collections.abc import Sequence
from typing import Awaitable, Protocol, runtime_checkable

from pydantic import BaseModel

from forze.base.primitives import JsonDict

from .specs import GraphModuleSpec
from .types import GraphDirection
from .value_objects import (
    EdgeRef,
    GraphWalkParams,
    GraphWalkStep,
    NeighborRow,
    ShortestPathParams,
    ShortestPathResult,
    VertexRef,
)

# ----------------------- #


@runtime_checkable
class BaseGraphModulePort(Protocol):
    """Shared :attr:`spec` binding for graph module adapters."""

    spec: GraphModuleSpec
    """:class:`GraphModuleSpec` for this port instance."""


# ....................... #


@runtime_checkable
class GraphQueryPort(BaseGraphModulePort, Protocol):
    """Read-only operations for a single :class:`GraphModuleSpec`."""

    def get_vertex(self, ref: VertexRef) -> Awaitable[BaseModel | None]:
        """Load one vertex by ref; return type matches the node kind’s ``read`` model."""
        ...  # pragma: no cover

    def get_vertices(
        self,
        refs: Sequence[VertexRef],  # noqa: F841
    ) -> Awaitable[Sequence[BaseModel]]:
        """Load many vertices; order may follow ``refs`` or be undefined (see adapter)."""
        ...  # pragma: no cover

    def get_edge(self, ref: EdgeRef) -> Awaitable[BaseModel | None]:  # noqa: F841
        """Load one edge by ref."""
        ...  # pragma: no cover

    def get_edges(
        self,
        refs: Sequence[EdgeRef],  # noqa: F841
    ) -> Awaitable[Sequence[BaseModel]]:
        """Load many edges by ref."""
        ...  # pragma: no cover

    def vertex_exists(self, ref: VertexRef) -> Awaitable[bool]: ...  # pragma: no cover

    def edge_exists(self, ref: EdgeRef) -> Awaitable[bool]: ...  # pragma: no cover

    def count_vertices(
        self,
        node_kind: str,  # noqa: F841
        *,
        property_filter: JsonDict | None = None,  # noqa: F841
    ) -> Awaitable[int]:
        """Count vertices of a logical node kind, optionally with equality-style filters.

        Unbounded full scans are discouraged; adapters may require *property_filter* or
        raise when the filter would be too expensive.
        """
        ...  # pragma: no cover

    def count_edges(
        self,
        edge_kind: str,  # noqa: F841
        *,
        property_filter: JsonDict | None = None,  # noqa: F841
    ) -> Awaitable[int]:
        """Count edges of a logical edge kind, with the same performance caveats as :meth:`count_vertices`."""
        ...  # pragma: no cover

    def neighbors(
        self,
        origin: VertexRef,
        direction: GraphDirection,
        edge_kinds: frozenset[str],
        *,
        limit: int,
        to_vertex_kinds: frozenset[str] | None = None,  # noqa: F841
    ) -> Awaitable[Sequence[NeighborRow]]:
        """
        :param to_vertex_kinds: If set, only return neighbors whose ``VertexRef.kind`` is in
            the set; if ``None``, any adjacent vertex kind in the spec is allowed.
        """
        ...  # pragma: no cover

    def incident_edges(
        self,
        origin: VertexRef,
        direction: GraphDirection,
        edge_kinds: frozenset[str],
        *,
        limit: int,
    ) -> Awaitable[Sequence[BaseModel]]:
        """List incident edge read models (no other-vertex join), ordered by adapter."""
        ...  # pragma: no cover

    def expand(
        self,
        start: VertexRef,
        params: GraphWalkParams,
    ) -> Awaitable[Sequence[GraphWalkStep]]:
        """Bounded multi-hop walk (BFS- or adapter-defined semantics) with *params* limits."""
        ...  # pragma: no cover

    def shortest_path(
        self,
        from_ref: VertexRef,  # noqa: F841
        to_ref: VertexRef,  # noqa: F841
        params: ShortestPathParams,
    ) -> Awaitable[ShortestPathResult | None]:
        """Return one of the shortest paths, or ``None`` if no path within *params* exists."""
        ...  # pragma: no cover

    def find_vertices(
        self,
        node_kind: str,  # noqa: F841
        *,
        property_filter: JsonDict | None = None,  # noqa: F841
        limit: int = 100,
        offset: int = 0,
    ) -> Awaitable[Sequence[BaseModel]]:
        """
        List vertices of *node_kind* with a simple property filter and offset pagination.

        *property_filter* is an adapter-defined equality (or best-effort) map; it is
        not a full :class:`forze.application.contracts.query.QueryFilterExpression` tree.
        """
        ...  # pragma: no cover

    def find_edges(
        self,
        edge_kind: str,  # noqa: F841
        *,
        property_filter: JsonDict | None = None,  # noqa: F841
        limit: int = 100,
        offset: int = 0,
    ) -> Awaitable[Sequence[BaseModel]]:
        """List edges of *edge_kind* with simple filtering and offset pagination."""
        ...  # pragma: no cover

    def vertex_degree(
        self,
        ref: VertexRef,
        *,
        direction: GraphDirection = GraphDirection.BOTH,
        edge_kinds: frozenset[str] | None = None,
    ) -> Awaitable[int]:
        """Count incident edges (optionally restricted by direction and kind)."""
        ...  # pragma: no cover

    def count_neighbors(
        self,
        ref: VertexRef,
        *,
        direction: GraphDirection = GraphDirection.BOTH,
        edge_kinds: frozenset[str] | None = None,
    ) -> Awaitable[int]:
        """Count distinct neighbor vertices (not edge multiplicity)."""
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class GraphCommandPort(BaseGraphModulePort, Protocol):
    """Write operations for a single :class:`GraphModuleSpec` (no raw query strings)."""

    def create_vertex(
        self,
        node_kind: str,  # noqa: F841
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> Awaitable[BaseModel | None]:
        """
        :param node_kind: Must match a :class:`GraphNodeSpec` ``name`` in :attr:`spec`.
        :param cmd: Create payload; *node_kind* must match the spec’s create model when
            a ``create`` DTO is declared on that node spec.
        """
        ...  # pragma: no cover

    def update_vertex(
        self,
        ref: VertexRef,
        cmd: BaseModel,
    ) -> Awaitable[BaseModel]: ...  # pragma: no cover

    def delete_vertex(self, ref: VertexRef) -> Awaitable[None]: ...  # pragma: no cover

    def create_edge(
        self,
        edge_kind: str,  # noqa: F841
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> Awaitable[BaseModel | None]:
        """*cmd* is adapter-defined; typically includes from/to and edge properties."""
        ...  # pragma: no cover

    def update_edge(
        self,
        ref: EdgeRef,
        cmd: BaseModel,
    ) -> Awaitable[BaseModel]: ...  # pragma: no cover

    def delete_edge(self, ref: EdgeRef) -> Awaitable[None]: ...  # pragma: no cover

    def create_vertices(
        self,
        items: Sequence[tuple[str, BaseModel]],
        *,
        return_new: bool = True,
    ) -> Awaitable[Sequence[BaseModel] | None]:
        """``(node_kind, cmd)`` pairs, batched; semantics match :meth:`create_vertex`."""
        ...  # pragma: no cover

    def create_edges(
        self,
        items: Sequence[tuple[str, BaseModel]],
        *,
        return_new: bool = True,
    ) -> Awaitable[Sequence[BaseModel] | None]:
        """``(edge_kind, cmd)`` pairs, batched."""
        ...  # pragma: no cover

    def ensure_vertex(
        self,
        node_kind: str,  # noqa: F841
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> Awaitable[BaseModel | None]:
        """Create when missing; if a matching vertex exists, return it unchanged (no in-place update)."""
        ...  # pragma: no cover

    def ensure_edge(
        self,
        edge_kind: str,  # noqa: F841
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> Awaitable[BaseModel | None]:
        """Idempotent create for edges; uniqueness rules are adapter-defined (e.g. at-most-one per pair+kind)."""
        ...  # pragma: no cover

    def delete_vertices(
        self,
        refs: Sequence[VertexRef],  # noqa: F841
    ) -> Awaitable[None]: ...  # pragma: no cover

    def delete_edges(
        self,
        refs: Sequence[EdgeRef],  # noqa: F841
    ) -> Awaitable[None]: ...  # pragma: no cover

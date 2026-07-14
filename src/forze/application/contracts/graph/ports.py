"""Query and command ports for a bounded graph module.

Ports are intentionally free of Cypher, AQL, and other engine-specific query
strings; adapters map these operations to the underlying graph database.
"""

from collections.abc import AsyncGenerator, Awaitable, Sequence
from typing import Protocol, runtime_checkable

from pydantic import BaseModel

from forze.base.primitives import JsonDict

from .specs import GraphModuleSpec
from .types import GraphDirection
from .value_objects import (
    EdgeRef,
    GraphWalkParams,
    GraphWalkStep,
    NeighborRow,
    ScopedWalkParams,
    ShortestPathParams,
    ShortestPathResult,
    VertexRef,
)

# ----------------------- #


@runtime_checkable
class BaseGraphModulePort(Protocol):
    """Shared ``spec`` binding for graph module adapters."""

    spec: GraphModuleSpec
    """``GraphModuleSpec`` for this port instance."""


# ....................... #


@runtime_checkable
class GraphQueryPort(BaseGraphModulePort, Protocol):
    """Read-only operations for a single ``GraphModuleSpec``."""

    def get_vertex(self, ref: VertexRef) -> Awaitable[BaseModel | None]:
        """Load one vertex by ref; return type matches the node kind’s ``read`` model."""
        ...  # pragma: no cover

    def get_vertices(
        self,
        refs: Sequence[VertexRef],
    ) -> Awaitable[Sequence[BaseModel]]:
        """Load many vertices; order may follow ``refs`` or be undefined (see adapter)."""
        ...  # pragma: no cover

    def get_edge(self, ref: EdgeRef) -> Awaitable[BaseModel | None]:
        """Load one edge by ref."""
        ...  # pragma: no cover

    def get_edges(
        self,
        refs: Sequence[EdgeRef],
    ) -> Awaitable[Sequence[BaseModel]]:
        """Load many edges by ref."""
        ...  # pragma: no cover

    def vertex_exists(self, ref: VertexRef) -> Awaitable[bool]: ...  # pragma: no cover

    def edge_exists(self, ref: EdgeRef) -> Awaitable[bool]: ...  # pragma: no cover

    def count_vertices(
        self,
        node_kind: str,
        *,
        property_filter: JsonDict | None = None,
    ) -> Awaitable[int]:
        """Count vertices of a logical node kind, optionally with equality-style filters.

        Unbounded full scans are discouraged; adapters may require *property_filter* or
        raise when the filter would be too expensive.
        """
        ...  # pragma: no cover

    def count_edges(
        self,
        edge_kind: str,
        *,
        property_filter: JsonDict | None = None,
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
        to_vertex_kinds: frozenset[str] | None = None,
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
        from_ref: VertexRef,
        to_ref: VertexRef,
        params: ShortestPathParams,
    ) -> Awaitable[ShortestPathResult | None]:
        """Return one of the shortest paths, or ``None`` if no path within *params* exists."""
        ...  # pragma: no cover

    def scoped_walk(
        self,
        anchor: VertexRef,
        params: ScopedWalkParams,
    ) -> Awaitable[Sequence[BaseModel]]:
        """Tenant-safe multi-segment walk returning the distinct typed target vertices.

        Unlike the raw hatch, the adapter owns the entire query: the anchor and the typed
        target are tenant-scoped and a full-path predicate constrains every intermediate
        node, so the traversal cannot cross tenants. Every caller input is structured (edge
        kinds, direction, hop bounds, target kind) — there is no engine query string, hence
        no injection or escape surface. Use it for multi-segment patterns that
        :meth:`neighbors` / :meth:`expand` / :meth:`shortest_path` cannot express, in place
        of the whole-query raw hatch.
        """
        ...  # pragma: no cover

    def k_shortest_paths(
        self,
        from_ref: VertexRef,
        to_ref: VertexRef,
        params: ShortestPathParams,
        *,
        k: int,
    ) -> Awaitable[Sequence[ShortestPathResult]]:
        """Return up to *k* shortest paths in increasing length (may be empty)."""
        ...  # pragma: no cover

    def find_vertices(
        self,
        node_kind: str,
        *,
        property_filter: JsonDict | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Awaitable[Sequence[BaseModel]]:
        """
        List vertices of *node_kind* with a simple property filter and offset pagination.

        *property_filter* is an adapter-defined equality (or best-effort) map; it is
        not a full ``forze.application.contracts.querying.QueryFilterExpression`` tree.
        Keys must be plain identifiers (see :func:`validate_property_filter_keys`);
        adapters fail closed on anything else.
        """
        ...  # pragma: no cover

    def find_edges(
        self,
        edge_kind: str,
        *,
        property_filter: JsonDict | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Awaitable[Sequence[BaseModel]]:
        """List edges of *edge_kind* with simple filtering and offset pagination."""
        ...  # pragma: no cover

    def find_vertices_stream(
        self,
        node_kind: str,
        *,
        property_filter: JsonDict | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[BaseModel]]:
        """Yield keyset batches of every vertex of *node_kind* — the whole-kind read.

        The streaming counterpart of :meth:`find_vertices`, and **not** the same thing with a
        bigger ``limit``: it seeks by key (``key_field > last-seen``) rather than by offset, so
        a graph being written while it is walked cannot shift rows past the cursor. Offset
        paging over a live graph silently skips and repeats — the failure an export must not
        have, because a page that was skipped and a page that was empty produce the same
        artifact.

        No total count, memory bounded by *chunk_size*. Fails closed on a backend that does not
        report vertex streaming, rather than serving a partial scan that reads like a complete
        one.
        """
        ...  # pragma: no cover

    def find_edges_stream(
        self,
        edge_kind: str,
        *,
        property_filter: JsonDict | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[BaseModel]]:
        """Yield keyset batches of every edge of *edge_kind*.

        As :meth:`find_vertices_stream`. A **keyed** edge (``identity="key"``) bookmarks on its
        own key; an **endpoint-identified** one has no key of its own — that is what the
        declaration means — so it bookmarks on the ``(tail, head)`` node-key pair, which *is*
        the identity the author asserted.

        For an endpoint-identified kind, *chunk_size* therefore bounds **pairs, not edges**, and
        every edge of a pair is yielded with it. The framework does not enforce the
        one-edge-per-pair identity that declaration promises (``create_edge`` will add a second
        parallel edge), so a page cut *within* a pair would leave edges behind the cursor that
        the next seek steps straight over — a walk that looked complete and was not.

        Fails closed on a multi-endpoint kind whose endpoint node kinds key on *different*
        properties: there is no single ordering that covers both, and no partial one is offered.
        """
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
    """Write operations for a single ``GraphModuleSpec`` (no raw query strings)."""

    def create_vertex(
        self,
        node_kind: str,
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> Awaitable[BaseModel | None]:
        """
        :param node_kind: Must match a ``GraphNodeSpec`` ``name`` in ``spec``.
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
        edge_kind: str,
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
        node_kind: str,
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> Awaitable[BaseModel | None]:
        """Create when missing; if a matching vertex exists, return it unchanged (no in-place update)."""
        ...  # pragma: no cover

    def ensure_edge(
        self,
        edge_kind: str,
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> Awaitable[BaseModel | None]:
        """Idempotent create for edges; uniqueness rules are adapter-defined (e.g. at-most-one per pair+kind)."""
        ...  # pragma: no cover

    def delete_vertices(
        self,
        refs: Sequence[VertexRef],
    ) -> Awaitable[None]: ...  # pragma: no cover

    def delete_edges(
        self,
        refs: Sequence[EdgeRef],
    ) -> Awaitable[None]: ...  # pragma: no cover


# ....................... #


@runtime_checkable
class GraphRawQueryPort(BaseGraphModulePort, Protocol):
    """Opt-in, engine-specific raw query escape hatch.

    Use only for power features the neutral ports cannot express (Cypher path
    predicates, GDS, APOC, AQL traversal options). The query string is engine-specific,
    so any code using this is **not** portable across graph backends, and result codec
    materialization is the caller's responsibility. Prefer the structured ports.

    Tenancy: in a **tenant-aware** module the adapter fails closed (raises if no tenant is
    bound, rather than running unscoped) and binds the current tenant as ``$tenant`` — so
    you must scope the query yourself, e.g. ``MATCH (n {tenant_id: $tenant})``. The adapter
    cannot rewrite arbitrary engine queries, so the filter placement is on you; a query that
    must legitimately span tenants belongs in a **non**-tenant-aware module by construction.
    """

    def run(
        self,
        query: str,
        params: JsonDict | None = None,
    ) -> Awaitable[Sequence[JsonDict]]:
        """Execute *query* with *params* and return raw result rows as mappings."""
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class GraphManagementPort(BaseGraphModulePort, Protocol):
    """Control-plane schema provisioning for a single ``GraphModuleSpec``.

    Separate from the data-plane query/command ports (mirrors the search port split): it
    creates the constraints and indexes that back node/edge key identity and tenant lookups —
    node key uniqueness (composite with the tenant property under tagged tenancy), keyed-edge
    key uniqueness (so concurrent ``ensure_edge`` cannot create duplicate keyed edges), and a
    tenant-property index. Not every backend provisions schema; where it does, the operations
    are idempotent.
    """

    def ensure_schema(self) -> Awaitable[None]:
        """Create the module's constraints/indexes if absent (idempotent)."""
        ...  # pragma: no cover

    def drop_schema(self) -> Awaitable[None]:
        """Drop the module's constraints/indexes if present (teardown / tests)."""
        ...  # pragma: no cover

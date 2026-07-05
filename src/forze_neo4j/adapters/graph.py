"""Neo4j graph adapter implementing the graph query/command/raw ports.

**MVP surface — not a full port implementation.** Implemented: vertex/edge CRUD,
``ensure_edge``, ``neighbors``, ``expand``, ``shortest_path``, and the raw escape hatch.
The remaining port methods (roughly the read-introspection and bulk half —
``get_vertices``/``get_edges``/``edge_exists``, ``count_*``, ``find_*``, ``vertex_degree``,
``k_shortest_paths``, ``update_edge``/``delete_edge``, the ``*_many`` bulk writes) raise a
clear ``exc.precondition`` (code ``graph_not_implemented``). The in-memory mock implements a
*different* subset, so no single adapter yet covers the whole ``GraphQueryPort`` contract —
treat the graph plane as community-tier and pin usage to the implemented slice.

**No schema/constraint/index provisioning.** This adapter issues no ``CREATE CONSTRAINT`` /
``CREATE INDEX``: node/edge key uniqueness and lookup performance depend on constraints the
operator must create out of band. In particular keyed-edge identity is enforced only at
``MERGE`` time (in-query), not by a database uniqueness constraint, so a concurrent
``ensure_edge`` race can still create duplicate keyed edges until such a constraint exists.

Tenancy uses property partition: a ``tenant_property`` is stamped on writes and constrains
anchor-node matches.
"""

from forze_neo4j._compat import require_neo4j

require_neo4j()

# ....................... #

from collections.abc import Sequence
from typing import Any, Literal, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.graph import (
    EdgeRef,
    GraphDirection,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
    GraphWalkParams,
    GraphWalkStep,
    NeighborRow,
    ScopedWalkParams,
    ShortestPathParams,
    ShortestPathResult,
    VertexRef,
)
from forze.application.contracts.resolution import (
    NamedResourceSpec,
    is_static_named_resource,
    resolve_scoped_namespace,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.application.integrations.graph import GraphCodecs, GraphKindCipher
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import JsonDict, OnceCell
from forze.base.serialization import default_model_codec

from ..kernel.client import Neo4jClientPort
from ..kernel.cypher import builders
from ..kernel.relation import resolve_neo4j_database

# ----------------------- #


def _nyi(method: str) -> CoreException:
    """Build the standard not-implemented error for a deferred port slice.

    Stays inside the :class:`~forze.base.exceptions.CoreException` taxonomy so
    egress mapping can classify it (a bare ``NotImplementedError`` cannot be).
    """

    return exc.precondition(
        f"{method} is not implemented by the neo4j backend yet",
        code="graph_not_implemented",
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Neo4jGraphAdapter(TenancyMixin):
    """Neo4j-backed adapter for a single :class:`GraphModuleSpec`.

    Inherits ``tenant_aware`` / ``tenant_provider`` from :class:`TenancyMixin`; tenancy
    is enforced by stamping/matching :attr:`tenant_property` on anchor nodes.
    """

    spec: GraphModuleSpec
    client: Neo4jClientPort
    codecs: GraphCodecs | None = None
    """Per-kind property-map ciphers (one :class:`GraphKindCipher` per node/edge kind). The
    factory resolves these from the wired keyring; ``None`` falls back to plaintext codecs (so
    a module with no ``encryption`` policy needs no crypto wiring)."""
    tenant_property: str = "tenant_id"
    database: NamedResourceSpec | None = None
    """Target Neo4j database — a static name, a per-tenant resolver (``namespace`` tier:
    per-tenant database on a shared cluster), or ``None`` (client default)."""

    _database_cell: OnceCell[str] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

    traversal_isolation: Literal["anchor", "full-path"] = "full-path"
    """How far tenant scoping reaches on traversals when ``tenant_aware``.

    ``full-path`` (default) constrains every node on a ``neighbors``/``expand``/
    ``shortest_path`` result, so a cross-tenant edge cannot surface a foreign node.
    ``anchor`` constrains only the start/endpoint nodes — cheaper, but safe only under the
    invariant that no edge ever crosses a tenant boundary.
    """

    allow_raw_query: bool = False
    """Whether the whole-query raw hatch :meth:`run` is permitted.

    The raw hatch is a **trusted-caller** escape: the caller writes the entire Cypher, so a
    buggy or hostile query can read cross-tenant even though ``$tenant`` is bound. It is
    therefore **disabled by default** (fail closed, code ``graph_raw_disabled``); set this
    ``True`` to opt in where trusted raw Cypher is genuinely needed — otherwise use the
    structured ports (full-path scoped) and :meth:`scoped_walk` instead.
    """

    # ....................... #
    # tenancy / database resolution

    async def _resolved_database(self) -> str | None:
        """Target Neo4j database for the current tenant (``None`` = client default).

        A static name (or ``None``) resolves without a tenant; a per-tenant resolver scopes
        each query to the tenant's own database (the ``namespace`` tier).
        """

        spec = self.database

        if spec is None or is_static_named_resource(spec):
            return spec

        return await resolve_scoped_namespace(
            spec,
            tenant_id=self._tenant_id_for_resolve(),
            cell=self._database_cell,
            resolver=resolve_neo4j_database,
        )

    # ....................... #
    # spec / codec helpers

    def _node(self, kind: str) -> GraphNodeSpec[BaseModel]:
        node = self.spec.graph_node_by_kind(kind)

        if node is None:
            raise exc.configuration(
                f"Unknown graph node kind {kind!r} in module {self.spec.name!r}",
                code="graph_unknown_node_kind",
            )

        return node

    # ....................... #

    def _edge(self, kind: str) -> GraphEdgeSpec[BaseModel]:
        edge = self.spec.graph_edge_by_kind(kind)

        if edge is None:
            raise exc.configuration(
                f"Unknown graph edge kind {kind!r} in module {self.spec.name!r}",
                code="graph_unknown_edge_kind",
            )

        return edge

    # ....................... #

    def _node_kind_from_labels(self, labels: Sequence[str]) -> str:
        for label in labels:
            if self.spec.graph_node_by_kind(label) is not None:
                return label

        raise exc.infrastructure(
            f"Returned node has no label matching a known node kind: {list(labels)!r}",
            code="graph_unmapped_node_labels",
        )

    # ....................... #

    def _strip_internal(self, props: JsonDict) -> JsonDict:
        """Drop adapter-internal properties (the tenant tag) not in the read model."""

        if self.tenant_aware and self.tenant_property in props:
            return {k: v for k, v in props.items() if k != self.tenant_property}

        return props

    # ....................... #

    def _node_cipher(self, kind: str) -> GraphKindCipher:
        """Per-kind property-map cipher for a node kind (plaintext when unwired)."""

        node = self._node(kind)

        if self.codecs is not None:
            return self.codecs.node(kind)

        return GraphKindCipher(read_codec=default_model_codec(node.read), cipher=None)

    # ....................... #

    def _edge_cipher(self, kind: str) -> GraphKindCipher:
        """Per-kind property-map cipher for an edge kind (plaintext when unwired)."""

        edge = self._edge(kind)

        if self.codecs is not None:
            return self.codecs.edge(kind)

        return GraphKindCipher(read_codec=default_model_codec(edge.read), cipher=None)

    # ....................... #

    async def _vertex_model(self, kind: str, props: JsonDict) -> BaseModel:
        return await self._node_cipher(kind).open(self._strip_internal(props))

    # ....................... #

    async def _edge_model(self, kind: str, props: JsonDict) -> BaseModel:
        return await self._edge_cipher(kind).open(self._strip_internal(props))

    # ....................... #
    # tenancy helpers

    @property
    def _tenant_field(self) -> str | None:
        return self.tenant_property if self.tenant_aware else None

    # ....................... #

    @property
    def _interior_scope(self) -> bool:
        """Whether traversals also constrain interior/terminal nodes to the tenant."""

        return self.tenant_aware and self.traversal_isolation == "full-path"

    # ....................... #

    def _tenant_str(self) -> str | None:
        tid = self.require_tenant_if_aware()
        return str(tid) if tid is not None else None

    # ....................... #

    def _params(self, **extra: Any) -> JsonDict:
        params: JsonDict = dict(extra)

        if self.tenant_aware:
            params["tenant"] = self._tenant_str()

        return params

    # ....................... #

    async def _encode(
        self,
        cmd: BaseModel,
        cipher: GraphKindCipher,
        *,
        record_id: Any = None,
    ) -> JsonDict:
        data: JsonDict = cmd.model_dump(mode="json", exclude_none=True)
        data = await cipher.seal(data, record_id=record_id)

        if self.tenant_aware:
            data[self.tenant_property] = self._tenant_str()

        return data

    # ....................... #
    # GraphQueryPort

    async def get_vertex(self, ref: VertexRef) -> BaseModel | None:
        node = self._node(ref.kind)
        query = builders.get_vertex(
            ref.kind, node.key_field, tenant_field=self._tenant_field
        )
        rows = await self.client.run(
            query, self._params(key=ref.key), database=await self._resolved_database()
        )

        if not rows:
            return None

        return await self._vertex_model(ref.kind, rows[0]["n"])

    # ....................... #

    async def vertex_exists(self, ref: VertexRef) -> bool:
        node = self._node(ref.kind)
        query = builders.vertex_exists(
            ref.kind, node.key_field, tenant_field=self._tenant_field
        )
        rows = await self.client.run(
            query, self._params(key=ref.key), database=await self._resolved_database()
        )

        return bool(rows and rows[0]["exists"])

    # ....................... #

    async def get_edge(self, ref: EdgeRef) -> BaseModel | None:
        if not ref.is_keyed:
            raise _nyi("get_edge in endpoints mode")

        edge = self._edge(ref.kind)

        if edge.key_field is None:
            raise exc.configuration(
                f"Edge kind {ref.kind!r} has no key_field but a keyed EdgeRef was used",
                code="graph_edge_missing_key_field",
            )

        query = builders.get_edge_by_key(
            ref.kind, edge.key_field, tenant_field=self._tenant_field
        )
        rows = await self.client.run(
            query, self._params(key=ref.key), database=await self._resolved_database()
        )

        if not rows:
            return None

        return await self._edge_model(ref.kind, rows[0]["r"])

    # ....................... #

    async def neighbors(
        self,
        origin: VertexRef,
        direction: GraphDirection,
        edge_kinds: frozenset[str],
        *,
        limit: int,
        to_vertex_kinds: frozenset[str] | None = None,
    ) -> Sequence[NeighborRow]:
        node = self._node(origin.kind)
        query = builders.neighbors(
            label=origin.kind,
            key_field=node.key_field,
            direction=direction,
            edge_types=edge_kinds,
            tenant_field=self._tenant_field,
            interior=self._interior_scope,
        )
        rows = await self.client.run(
            query,
            self._params(key=origin.key, limit=limit),
            database=await self._resolved_database(),
        )

        out: list[NeighborRow] = []

        for row in rows:
            other_kind = self._node_kind_from_labels(row["other_labels"])

            if to_vertex_kinds is not None and other_kind not in to_vertex_kinds:
                continue

            out.append(
                NeighborRow(
                    other=await self._vertex_model(other_kind, row["other"]),
                    via_edge=await self._edge_model(row["via_type"], row["via_edge"]),
                    direction=direction,
                )
            )

        return out

    # ....................... #

    async def expand(
        self,
        start: VertexRef,
        params: GraphWalkParams,
    ) -> Sequence[GraphWalkStep]:
        node = self._node(start.kind)
        query = builders.expand(
            label=start.kind,
            key_field=node.key_field,
            direction=params.direction,
            edge_types=params.edge_kinds,
            max_depth=params.max_depth,
            tenant_field=self._tenant_field,
            interior=self._interior_scope,
        )
        rows = await self.client.run(
            query,
            self._params(key=start.key, max_results=params.max_results),
            database=await self._resolved_database(),
        )

        out: list[GraphWalkStep] = []

        for row in rows:
            vertex_kind = self._node_kind_from_labels(row["vertex_labels"])
            parent_labels: list[str] = row.get("parent_labels") or []
            parent_ref: VertexRef | None = None
            from_parent: BaseModel | None = None

            if parent_labels and row.get("parent"):
                parent_kind = self._node_kind_from_labels(parent_labels)
                parent_props = row["parent"]
                parent_ref = VertexRef(
                    kind=parent_kind,
                    key=str(parent_props[self._node(parent_kind).key_field]),
                )

            if row.get("from_parent") and row.get("from_parent_type"):
                from_parent = await self._edge_model(
                    row["from_parent_type"], row["from_parent"]
                )

            out.append(
                GraphWalkStep(
                    depth=row["depth"],
                    vertex=await self._vertex_model(vertex_kind, row["vertex"]),
                    from_parent=from_parent,
                    parent_ref=parent_ref,
                )
            )

        return out

    # ....................... #

    async def shortest_path(
        self,
        from_ref: VertexRef,
        to_ref: VertexRef,
        params: ShortestPathParams,
    ) -> ShortestPathResult | None:
        from_node = self._node(from_ref.kind)
        to_node = self._node(to_ref.kind)
        query = builders.shortest_path(
            from_label=from_ref.kind,
            from_key_field=from_node.key_field,
            to_label=to_ref.kind,
            to_key_field=to_node.key_field,
            direction=GraphDirection.OUT,
            edge_types=params.edge_kinds,
            max_hops=params.max_hops,
            tenant_field=self._tenant_field,
            interior=self._interior_scope,
        )
        rows = await self.client.run(
            query,
            self._params(from_key=from_ref.key, to_key=to_ref.key),
            database=await self._resolved_database(),
        )

        if not rows:
            return None

        row = rows[0]
        vertices = tuple(
            [
                await self._vertex_model(self._node_kind_from_labels(labels), props)
                for props, labels in zip(
                    row["vertices"], row["vertex_labels"], strict=True
                )
            ]
        )
        edges = tuple(
            [
                await self._edge_model(edge_type, props)
                for props, edge_type in zip(
                    row["edges"], row["edge_types"], strict=True
                )
            ]
        )

        return ShortestPathResult(vertices=vertices, edges=edges)

    # ....................... #

    async def scoped_walk(
        self,
        anchor: VertexRef,
        params: ScopedWalkParams,
    ) -> Sequence[BaseModel]:
        anchor_node = self._node(anchor.kind)
        self._node(params.target_kind)  # validate the target kind is in the spec

        segments = [
            (step.direction, step.edge_kinds, step.min_hops, step.max_hops)
            for step in params.steps
        ]
        query = builders.scoped_walk(
            anchor_label=anchor.kind,
            anchor_key_field=anchor_node.key_field,
            segments=segments,
            target_label=params.target_kind,
            tenant_field=self._tenant_field,
        )
        rows = await self.client.run(
            query,
            self._params(key=anchor.key, limit=params.limit),
            database=await self._resolved_database(),
        )

        return [
            await self._vertex_model(params.target_kind, row["m"]) for row in rows
        ]

    # ....................... #
    # GraphCommandPort

    async def create_vertex(
        self,
        node_kind: str,
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> BaseModel | None:
        query = builders.create_vertex(node_kind)
        props = await self._encode(cmd, self._node_cipher(node_kind))
        rows = await self.client.run(
            query,
            {"props": props, **self._params()},
            database=await self._resolved_database(),
        )

        if not return_new:
            return None

        return await self._vertex_model(node_kind, rows[0]["n"])

    # ....................... #

    async def update_vertex(self, ref: VertexRef, cmd: BaseModel) -> BaseModel:
        node = self._node(ref.kind)
        query = builders.update_vertex(
            ref.kind, node.key_field, tenant_field=self._tenant_field
        )
        props = await self._encode(
            cmd, self._node_cipher(ref.kind), record_id=ref.key
        )
        rows = await self.client.run(
            query,
            {"props": props, **self._params(key=ref.key)},
            database=await self._resolved_database(),
        )

        if not rows:
            raise exc.not_found(
                f"Vertex {ref.kind}:{ref.key} not found",
                code="graph_vertex_not_found",
            )

        return await self._vertex_model(ref.kind, rows[0]["n"])

    # ....................... #

    async def delete_vertex(self, ref: VertexRef) -> None:
        node = self._node(ref.kind)
        query = builders.delete_vertex(
            ref.kind, node.key_field, tenant_field=self._tenant_field
        )
        await self.client.run(query, self._params(key=ref.key), database=await self._resolved_database())

    # ....................... #

    async def create_edge(
        self,
        edge_kind: str,
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> BaseModel | None:
        return await self._write_edge(
            edge_kind, cmd, merge=False, return_new=return_new
        )

    # ....................... #

    async def ensure_edge(
        self,
        edge_kind: str,
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> BaseModel | None:
        return await self._write_edge(edge_kind, cmd, merge=True, return_new=return_new)

    # ....................... #

    async def _write_edge(
        self,
        edge_kind: str,
        cmd: BaseModel,
        *,
        merge: bool,
        return_new: bool,
    ) -> BaseModel | None:
        edge = self._edge(edge_kind)

        if len(edge.endpoints) != 1:
            raise _nyi(f"multi-endpoint edge kind {edge_kind!r}")

        endpoint = edge.endpoints[0]
        from_node = self._node(endpoint.from_kind)
        to_node = self._node(endpoint.to_kind)

        data = await self._encode(cmd, self._edge_cipher(edge_kind))
        from_key = data.pop("from_key", None)
        to_key = data.pop("to_key", None)

        if from_key is None or to_key is None:
            raise exc.validation(
                f"Edge create command for {edge_kind!r} must include 'from_key' and 'to_key'",
                code="graph_edge_endpoints_required",
            )

        # A keyed edge kind is identified by its key property: an ``ensure`` (MERGE)
        # must match on that key so two distinct keyed edges between the same pair
        # stay separate. A keyless MERGE matches any edge of the type and collapses
        # them.
        edge_key = None

        if merge and edge.key_field is not None:
            edge_key = data.get(edge.key_field)

            if edge_key is None:
                raise exc.validation(
                    f"Keyed edge command for {edge_kind!r} must include "
                    f"{edge.key_field!r} to ensure a stable identity",
                    code="graph_edge_key_required",
                )

        query = builders.create_edge(
            from_label=endpoint.from_kind,
            from_key_field=from_node.key_field,
            to_label=endpoint.to_kind,
            to_key_field=to_node.key_field,
            edge_type=edge_kind,
            merge=merge,
            tenant_field=self._tenant_field,
            key_field=edge.key_field if merge else None,
        )
        params = {"props": data, **self._params(from_key=from_key, to_key=to_key)}

        if edge_key is not None:
            params["edge_key"] = edge_key

        rows = await self.client.run(
            query,
            params,
            database=await self._resolved_database(),
        )

        if not rows:
            raise exc.not_found(
                f"Edge endpoints for {edge_kind!r} not found ({from_key} -> {to_key})",
                code="graph_edge_endpoints_not_found",
            )

        if not return_new:
            return None

        return await self._edge_model(edge_kind, rows[0]["r"])

    # ....................... #
    # GraphRawQueryPort

    async def run(
        self, query: str, params: JsonDict | None = None
    ) -> Sequence[JsonDict]:
        # The whole-query raw hatch is trusted-caller by construction; a deployment that
        # requires enforced tenancy disables it (``allow_raw_query=False``) and uses the
        # structured ports / ``scoped_walk`` instead.
        if not self.allow_raw_query:
            raise exc.configuration(
                f"Raw graph queries are disabled for module {self.spec.name!r} "
                "(allow_raw_query=False); use the structured ports or scoped_walk.",
                code="graph_raw_disabled",
            )

        # Tenant-aware raw queries fail closed: ``_tenant_str`` →
        # ``require_tenant_if_aware`` raises if no tenant is bound (was: silent
        # cross-tenant access). The framework tenant is bound as ``$tenant`` (authoritative
        # over any caller-supplied key) so the query can ``MATCH (... {tenant_id: $tenant})``.
        merged = dict(params or {})

        if self.tenant_aware:
            merged["tenant"] = self._tenant_str()

        return await self.client.run(query, merged or None, database=await self._resolved_database())

    # ....................... #
    # Deferred GraphQueryPort methods

    async def get_vertices(self, refs: Sequence[VertexRef]) -> Sequence[BaseModel]:
        raise _nyi("get_vertices")

    async def get_edges(self, refs: Sequence[EdgeRef]) -> Sequence[BaseModel]:
        raise _nyi("get_edges")

    async def edge_exists(self, ref: EdgeRef) -> bool:
        raise _nyi("edge_exists")

    async def count_vertices(
        self,
        node_kind: str,
        *,
        property_filter: JsonDict | None = None,
    ) -> int:
        del property_filter
        raise _nyi("count_vertices")

    async def count_edges(
        self,
        edge_kind: str,
        *,
        property_filter: JsonDict | None = None,
    ) -> int:
        del property_filter
        raise _nyi("count_edges")

    async def incident_edges(
        self,
        origin: VertexRef,
        direction: GraphDirection,
        edge_kinds: frozenset[str],
        *,
        limit: int,
    ) -> Sequence[BaseModel]:
        raise _nyi("incident_edges")

    async def k_shortest_paths(
        self,
        from_ref: VertexRef,
        to_ref: VertexRef,
        params: ShortestPathParams,
        *,
        k: int,
    ) -> Sequence[ShortestPathResult]:
        raise _nyi("k_shortest_paths")

    async def find_vertices(
        self,
        node_kind: str,
        *,
        property_filter: JsonDict | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Sequence[BaseModel]:
        del property_filter
        raise _nyi("find_vertices")

    async def find_edges(
        self,
        edge_kind: str,
        *,
        property_filter: JsonDict | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Sequence[BaseModel]:
        del property_filter
        raise _nyi("find_edges")

    async def vertex_degree(
        self,
        ref: VertexRef,
        *,
        direction: GraphDirection = GraphDirection.BOTH,
        edge_kinds: frozenset[str] | None = None,
    ) -> int:
        raise _nyi("vertex_degree")

    async def count_neighbors(
        self,
        ref: VertexRef,
        *,
        direction: GraphDirection = GraphDirection.BOTH,
        edge_kinds: frozenset[str] | None = None,
    ) -> int:
        raise _nyi("count_neighbors")

    # ....................... #
    # Deferred GraphCommandPort methods

    async def update_edge(self, ref: EdgeRef, cmd: BaseModel) -> BaseModel:
        raise _nyi("update_edge")

    async def delete_edge(self, ref: EdgeRef) -> None:
        raise _nyi("delete_edge")

    async def create_vertices(
        self,
        items: Sequence[tuple[str, BaseModel]],
        *,
        return_new: bool = True,
    ) -> Sequence[BaseModel] | None:
        raise _nyi("create_vertices")

    async def create_edges(
        self,
        items: Sequence[tuple[str, BaseModel]],
        *,
        return_new: bool = True,
    ) -> Sequence[BaseModel] | None:
        raise _nyi("create_edges")

    async def ensure_vertex(
        self,
        node_kind: str,
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> BaseModel | None:
        raise _nyi("ensure_vertex")

    async def delete_vertices(self, refs: Sequence[VertexRef]) -> None:
        raise _nyi("delete_vertices")

    async def delete_edges(self, refs: Sequence[EdgeRef]) -> None:
        raise _nyi("delete_edges")

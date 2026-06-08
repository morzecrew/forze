"""Neo4j graph adapter implementing the graph query/command/raw ports.

Focused vertical slice: vertex/edge CRUD, ``ensure_edge``, ``neighbors``, ``expand``,
``shortest_path``, and the raw escape hatch. The remaining port methods raise a clear
``NotImplementedError`` and are filled in follow-ups. Tenancy uses property partition:
a ``tenant_property`` is stamped on writes and constrains anchor-node matches.
"""

from forze_neo4j._compat import require_neo4j

require_neo4j()

# ....................... #

from collections.abc import Sequence
from typing import Any, final

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
    ShortestPathParams,
    ShortestPathResult,
    VertexRef,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import default_model_codec

from ..kernel.client import Neo4jClientPort
from ..kernel.cypher import builders

# ----------------------- #


def _nyi(method: str) -> NotImplementedError:
    return NotImplementedError(f"forze_neo4j: {method} is not yet implemented")


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
    tenant_property: str = "tenant_id"
    database: str | None = None

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

    def _vertex_model(self, kind: str, props: JsonDict) -> BaseModel:
        codec = default_model_codec(self._node(kind).read)
        return codec.decode_mapping(self._strip_internal(props), trust_source=True)

    # ....................... #

    def _edge_model(self, kind: str, props: JsonDict) -> BaseModel:
        codec = default_model_codec(self._edge(kind).read)
        return codec.decode_mapping(self._strip_internal(props), trust_source=True)

    # ....................... #
    # tenancy helpers

    @property
    def _tenant_field(self) -> str | None:
        return self.tenant_property if self.tenant_aware else None

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

    def _encode(self, cmd: BaseModel) -> JsonDict:
        data: JsonDict = cmd.model_dump(mode="json", exclude_none=True)

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
            query, self._params(key=ref.key), database=self.database
        )

        if not rows:
            return None

        return self._vertex_model(ref.kind, rows[0]["n"])

    # ....................... #

    async def vertex_exists(self, ref: VertexRef) -> bool:
        node = self._node(ref.kind)
        query = builders.vertex_exists(
            ref.kind, node.key_field, tenant_field=self._tenant_field
        )
        rows = await self.client.run(
            query, self._params(key=ref.key), database=self.database
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
            query, self._params(key=ref.key), database=self.database
        )

        if not rows:
            return None

        return self._edge_model(ref.kind, rows[0]["r"])

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
        )
        rows = await self.client.run(
            query,
            self._params(key=origin.key, limit=limit),
            database=self.database,
        )

        out: list[NeighborRow] = []

        for row in rows:
            other_kind = self._node_kind_from_labels(row["other_labels"])

            if to_vertex_kinds is not None and other_kind not in to_vertex_kinds:
                continue

            out.append(
                NeighborRow(
                    other=self._vertex_model(other_kind, row["other"]),
                    via_edge=self._edge_model(row["via_type"], row["via_edge"]),
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
        )
        rows = await self.client.run(
            query,
            self._params(key=start.key, max_results=params.max_results),
            database=self.database,
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
                from_parent = self._edge_model(
                    row["from_parent_type"], row["from_parent"]
                )

            out.append(
                GraphWalkStep(
                    depth=row["depth"],
                    vertex=self._vertex_model(vertex_kind, row["vertex"]),
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
        )
        rows = await self.client.run(
            query,
            self._params(from_key=from_ref.key, to_key=to_ref.key),
            database=self.database,
        )

        if not rows:
            return None

        row = rows[0]
        vertices = tuple(
            self._vertex_model(self._node_kind_from_labels(labels), props)
            for props, labels in zip(row["vertices"], row["vertex_labels"], strict=True)
        )
        edges = tuple(
            self._edge_model(edge_type, props)
            for props, edge_type in zip(row["edges"], row["edge_types"], strict=True)
        )

        return ShortestPathResult(vertices=vertices, edges=edges)

    # ....................... #
    # GraphCommandPort

    async def create_vertex(
        self,
        node_kind: str,
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> BaseModel | None:
        self._node(node_kind)
        query = builders.create_vertex(node_kind)
        rows = await self.client.run(
            query,
            {"props": self._encode(cmd), **self._params()},
            database=self.database,
        )

        if not return_new:
            return None

        return self._vertex_model(node_kind, rows[0]["n"])

    # ....................... #

    async def update_vertex(self, ref: VertexRef, cmd: BaseModel) -> BaseModel:
        node = self._node(ref.kind)
        query = builders.update_vertex(
            ref.kind, node.key_field, tenant_field=self._tenant_field
        )
        rows = await self.client.run(
            query,
            {"props": self._encode(cmd), **self._params(key=ref.key)},
            database=self.database,
        )

        if not rows:
            raise exc.not_found(
                f"Vertex {ref.kind}:{ref.key} not found",
                code="graph_vertex_not_found",
            )

        return self._vertex_model(ref.kind, rows[0]["n"])

    # ....................... #

    async def delete_vertex(self, ref: VertexRef) -> None:
        node = self._node(ref.kind)
        query = builders.delete_vertex(
            ref.kind, node.key_field, tenant_field=self._tenant_field
        )
        await self.client.run(query, self._params(key=ref.key), database=self.database)

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

        data = self._encode(cmd)
        from_key = data.pop("from_key", None)
        to_key = data.pop("to_key", None)

        if from_key is None or to_key is None:
            raise exc.validation(
                f"Edge create command for {edge_kind!r} must include 'from_key' and 'to_key'",
                code="graph_edge_endpoints_required",
            )

        query = builders.create_edge(
            from_label=endpoint.from_kind,
            from_key_field=from_node.key_field,
            to_label=endpoint.to_kind,
            to_key_field=to_node.key_field,
            edge_type=edge_kind,
            merge=merge,
            tenant_field=self._tenant_field,
        )
        rows = await self.client.run(
            query,
            {"props": data, **self._params(from_key=from_key, to_key=to_key)},
            database=self.database,
        )

        if not rows:
            raise exc.not_found(
                f"Edge endpoints for {edge_kind!r} not found ({from_key} -> {to_key})",
                code="graph_edge_endpoints_not_found",
            )

        if not return_new:
            return None

        return self._edge_model(edge_kind, rows[0]["r"])

    # ....................... #
    # GraphRawQueryPort

    async def run(
        self, query: str, params: JsonDict | None = None
    ) -> Sequence[JsonDict]:
        # Tenant-aware raw queries fail closed: ``_tenant_str`` →
        # ``require_tenant_if_aware`` raises if no tenant is bound (was: silent
        # cross-tenant access). The framework tenant is bound as ``$tenant`` (authoritative
        # over any caller-supplied key) so the query can ``MATCH (... {tenant_id: $tenant})``.
        merged = dict(params or {})

        if self.tenant_aware:
            merged["tenant"] = self._tenant_str()

        return await self.client.run(query, merged or None, database=self.database)

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

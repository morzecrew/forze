"""In-memory mock graph adapter.

Exists so handlers/kits can exercise the graph ports without a real engine. Covers the full
``GraphCommandPort`` (vertex/edge CRUD + bulk + ensure/update/delete), the read-introspection
+ ``find_*`` surface, and multi-hop traversal (``expand`` / ``shortest_path`` — unweighted BFS
or weighted Dijkstra — / ``scoped_walk``) via a small in-memory traversal core. Because
Neo4j's traversal ordering/tie-breaking is only partly specified, the differential
conformance test compares invariants (step multisets, path lengths/costs, reachable target
sets), not byte-identical output. ``k_shortest_paths`` and the raw escape hatch raise
``NotImplementedError`` — use ``forze_neo4j`` for those.
"""

from __future__ import annotations

import heapq
from collections import deque
from collections.abc import AsyncGenerator, Sequence
from typing import Any, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.graph import (
    EdgeRef,
    GraphDirection,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
    GraphPathStep,
    GraphReadCapabilities,
    GraphWalkParams,
    GraphWalkStep,
    NeighborRow,
    ScopedWalkParams,
    ShortestPathParams,
    ShortestPathResult,
    VertexRef,
    validate_property_filter_keys,
)
from forze.application.integrations.graph import (
    assert_edge_streamable,
    assert_vertex_streamable,
    resolve_write_endpoint,
    stream_keyset_pages,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import default_model_codec
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin

# ----------------------- #


def _nyi(method: str) -> NotImplementedError:
    return NotImplementedError(f"forze_mock graph: {method} is not implemented")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockGraphAdapter(MockTenancyMixin):
    """In-memory adapter for a single :class:`GraphModuleSpec`."""

    spec: GraphModuleSpec
    state: MockState
    namespace: str

    # ....................... #

    def _verts(self) -> dict[tuple[str, str], JsonDict]:
        # Tenant-partition the store like the other mocks (fails closed when tenant_aware
        # with no bound tenant) so unit tests catch cross-tenant leaks instead of the mock
        # silently sharing one store across tenants.
        ns = self._partitioned_namespace(self.namespace)
        return self.state.graph_vertices.setdefault(ns, {})

    def _edges_store(self) -> list[JsonDict]:
        ns = self._partitioned_namespace(self.namespace)
        return self.state.graph_edges.setdefault(ns, [])

    # ....................... #

    def _node(self, kind: str) -> GraphNodeSpec[BaseModel]:
        node = self.spec.graph_node_by_kind(kind)

        if node is None:
            raise exc.configuration(f"Unknown node kind {kind!r}", code="graph_unknown_node_kind")

        return node

    def _edge(self, kind: str) -> GraphEdgeSpec[BaseModel]:
        edge = self.spec.graph_edge_by_kind(kind)

        if edge is None:
            raise exc.configuration(f"Unknown edge kind {kind!r}", code="graph_unknown_edge_kind")

        return edge

    # ....................... #

    def _vmodel(self, kind: str, props: JsonDict) -> BaseModel:
        return default_model_codec(self._node(kind).read).decode_mapping(props, trust_source=True)

    def _emodel(self, kind: str, props: JsonDict) -> BaseModel:
        return default_model_codec(self._edge(kind).read).decode_mapping(props, trust_source=True)

    # ....................... #
    # GraphQueryPort

    async def get_vertex(self, ref: VertexRef) -> BaseModel | None:
        with self.state.lock:
            props = self._verts().get((ref.kind, ref.key))

        return self._vmodel(ref.kind, props) if props is not None else None

    async def vertex_exists(self, ref: VertexRef) -> bool:
        with self.state.lock:
            return (ref.kind, ref.key) in self._verts()

    async def get_edge(self, ref: EdgeRef) -> BaseModel | None:
        with self.state.lock:
            rec = self._find_edge_rec(ref, self._edges_store())

        return self._emodel(ref.kind, rec["props"]) if rec is not None else None

    def _find_edge_rec(
        self,
        ref: EdgeRef,
        edges: Sequence[JsonDict],
    ) -> JsonDict | None:
        """Locate the edge record for *ref* (keyed by key, else by endpoint pair)."""

        if ref.is_keyed:
            kf = self._edge(ref.kind).key_field

            if kf is None:
                raise exc.configuration(
                    f"Edge kind {ref.kind!r} has no key_field",
                    code="graph_edge_missing_key_field",
                )

            return next(
                (
                    rec
                    for rec in edges
                    if rec["kind"] == ref.kind and str(rec["props"].get(kf)) == ref.key
                ),
                None,
            )

        if ref.from_ref is None or ref.to_ref is None:
            raise exc.configuration(
                f"Endpoints EdgeRef for {ref.kind!r} must carry from_ref and to_ref",
                code="graph_edge_missing_endpoints",
            )

        return next(
            (
                rec
                for rec in edges
                if (
                    rec["kind"] == ref.kind
                    and rec["from_kind"] == ref.from_ref.kind
                    and rec["from_key"] == ref.from_ref.key
                    and rec["to_kind"] == ref.to_ref.kind
                    and rec["to_key"] == ref.to_ref.key
                )
            ),
            None,
        )

    @staticmethod
    def _validate_filter(
        property_filter: JsonDict | None, encryption: FieldEncryption | None
    ) -> None:
        if not property_filter:
            return

        # Same rule/order as the Neo4j adapter's ``_filter_params``: non-identifier keys
        # first, then sealed properties — so the mock rejects exactly what production does.
        validate_property_filter_keys(property_filter)

        sealed: frozenset[str] = (
            encryption.encrypted | encryption.searchable if encryption is not None else frozenset()
        )

        if blocked := sorted(k for k in property_filter if k in sealed):
            raise exc.precondition(
                f"Cannot filter on encrypted graph properties {blocked} (sealed at rest); "
                "filter on a plaintext property instead.",
                code="graph_filter_on_encrypted_field",
            )

    @staticmethod
    def _matches(props: JsonDict, property_filter: JsonDict | None) -> bool:
        if not property_filter:
            return True

        return all(props.get(k) == v for k, v in property_filter.items())

    async def neighbors(
        self,
        origin: VertexRef,
        direction: GraphDirection,
        edge_kinds: frozenset[str],
        *,
        limit: int,
        to_vertex_kinds: frozenset[str] | None = None,
    ) -> Sequence[NeighborRow]:
        out: list[NeighborRow] = []

        with self.state.lock:
            verts = dict(self._verts())
            edges = list(self._edges_store())

        for rec in edges:
            if edge_kinds and rec["kind"] not in edge_kinds:
                continue

            other_kind, other_key = _other_endpoint(rec, origin, direction)

            if other_kind is None or other_key is None:
                continue

            if to_vertex_kinds is not None and other_kind not in to_vertex_kinds:
                continue

            other_props = verts.get((other_kind, other_key))

            if other_props is None:
                continue

            out.append(
                NeighborRow(
                    other=self._vmodel(other_kind, other_props),
                    via_edge=self._emodel(rec["kind"], rec["props"]),
                    direction=direction,
                )
            )

            if len(out) >= limit:
                break

        return out

    async def expand(self, start: VertexRef, params: GraphWalkParams) -> Sequence[GraphWalkStep]:
        with self.state.lock:
            verts = dict(self._verts())
            edges = list(self._edges_store())

        if (start.kind, start.key) not in verts:
            return []

        adj = _adjacency(edges, params.direction, params.edge_kinds)
        steps = _enumerate_expand((start.kind, start.key), adj, verts, params.max_depth)
        # Neo4j orders by depth (intra-depth order unspecified) then LIMIT max_results.
        steps.sort(key=lambda step: step[0])

        return [
            GraphWalkStep(
                depth=depth,
                vertex=self._vmodel(node[0], verts[node]),
                from_parent=self._emodel(rec["kind"], rec["props"]),
                parent_ref=VertexRef(kind=parent[0], key=parent[1]),
            )
            for depth, node, parent, rec in steps[: params.max_results]
        ]

    async def shortest_path(
        self,
        from_ref: VertexRef,
        to_ref: VertexRef,
        params: ShortestPathParams,
    ) -> ShortestPathResult | None:
        with self.state.lock:
            verts = dict(self._verts())
            edges = list(self._edges_store())

        src = (from_ref.kind, from_ref.key)
        dst = (to_ref.kind, to_ref.key)

        if src not in verts or dst not in verts:
            return None

        # Neo4j drives shortest_path in the OUT direction.
        adj = _adjacency(edges, GraphDirection.OUT, params.edge_kinds)

        if params.weight_property is None:
            path = _bfs_shortest(src, dst, adj, verts, params.max_hops)
        else:
            path = _dijkstra_shortest(src, dst, adj, verts, params.weight_property, params.max_hops)

        if path is None:
            return None

        nodes, edge_recs = path
        return ShortestPathResult(
            vertices=tuple(self._vmodel(n[0], verts[n]) for n in nodes),
            edges=tuple(self._emodel(r["kind"], r["props"]) for r in edge_recs),
        )

    async def scoped_walk(
        self,
        anchor: VertexRef,
        params: ScopedWalkParams,
    ) -> Sequence[BaseModel]:
        self._node(anchor.kind)
        self._node(params.target_kind)  # validate kinds are in the spec

        with self.state.lock:
            verts = dict(self._verts())
            edges = list(self._edges_store())

        anchor_node = (anchor.kind, anchor.key)

        if anchor_node not in verts:
            return []

        frontier: set[tuple[tuple[str, str], frozenset[int]]] = {(anchor_node, frozenset())}

        for step in params.steps:
            adj = _adjacency(edges, step.direction, step.edge_kinds)
            frontier = _advance_segment(frontier, step, adj, verts)

        # Distinct target-kind vertices (Neo4j returns DISTINCT m, unordered), capped.
        targets: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()

        for node, _used in frontier:
            if node[0] == params.target_kind and node not in seen:
                seen.add(node)
                targets.append(node)

        return [self._vmodel(params.target_kind, verts[n]) for n in targets[: params.limit]]

    # ....................... #
    # GraphCommandPort

    async def create_vertex(
        self,
        node_kind: str,
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> BaseModel | None:
        node = self._node(node_kind)
        props = cmd.model_dump(mode="json", exclude_none=True)
        key = str(props[node.key_field])

        with self.state.lock:
            self._verts()[(node_kind, key)] = props

        return self._vmodel(node_kind, props) if return_new else None

    async def update_vertex(self, ref: VertexRef, cmd: BaseModel) -> BaseModel:
        patch = cmd.model_dump(mode="json", exclude_none=True)

        with self.state.lock:
            existing = self._verts().get((ref.kind, ref.key))

            if existing is None:
                raise exc.not_found(
                    f"Vertex {ref.kind}:{ref.key} not found",
                    code="graph_vertex_not_found",
                )

            merged = {**existing, **patch}
            self._verts()[(ref.kind, ref.key)] = merged

        return self._vmodel(ref.kind, merged)

    async def delete_vertex(self, ref: VertexRef) -> None:
        with self.state.lock:
            self._verts().pop((ref.kind, ref.key), None)

    async def create_edge(
        self,
        edge_kind: str,
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> BaseModel | None:
        return await self._write_edge(edge_kind, cmd, merge=False, return_new=return_new)

    async def ensure_edge(
        self,
        edge_kind: str,
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> BaseModel | None:
        return await self._write_edge(edge_kind, cmd, merge=True, return_new=return_new)

    async def _write_edge(
        self,
        edge_kind: str,
        cmd: BaseModel,
        *,
        merge: bool,
        return_new: bool,
    ) -> BaseModel | None:
        edge = self._edge(edge_kind)
        data = cmd.model_dump(mode="json", exclude_none=True)
        from_key = data.pop("from_key", None)
        to_key = data.pop("to_key", None)

        if from_key is None or to_key is None:
            raise exc.validation(
                f"Edge create for {edge_kind!r} requires 'from_key' and 'to_key'",
                code="graph_edge_endpoints_required",
            )

        # Single-endpoint kinds are implicit; multi-endpoint kinds name the pair via
        # from_kind/to_kind (popped from ``data``).
        endpoint = resolve_write_endpoint(edge, data)
        rec = {
            "kind": edge_kind,
            "from_kind": endpoint.from_kind,
            "from_key": str(from_key),
            "to_kind": endpoint.to_kind,
            "to_key": str(to_key),
            "props": data,
        }

        with self.state.lock:
            store = self._edges_store()

            if merge:
                key_field = edge.key_field
                edge_key = data.get(key_field) if key_field is not None else None

                if key_field is not None and edge_key is None:
                    raise exc.validation(
                        f"Keyed edge command for {edge_kind!r} must include {key_field!r} "
                        "to ensure a stable identity",
                        code="graph_edge_key_required",
                    )

                for existing in store:
                    # Identity includes the endpoint *kinds* (so two multi-endpoint edges with the
                    # same key values but different kinds stay distinct) and, for a keyed edge kind,
                    # the key value — mirroring Neo4j's ``MERGE (a)-[r:T {<key>}]->(b)`` so two
                    # distinct keys between the same pair are separate edges, not collapsed into one.
                    if (
                        existing["kind"] == edge_kind
                        and existing["from_kind"] == rec["from_kind"]
                        and existing["from_key"] == rec["from_key"]
                        and existing["to_kind"] == rec["to_kind"]
                        and existing["to_key"] == rec["to_key"]
                        and (key_field is None or existing["props"].get(key_field) == edge_key)
                    ):
                        return self._emodel(edge_kind, existing["props"]) if return_new else None

            store.append(rec)

        return self._emodel(edge_kind, data) if return_new else None

    # ....................... #
    # GraphRawQueryPort

    async def run(self, query: str, params: JsonDict | None = None) -> Sequence[JsonDict]:
        raise _nyi("raw run")

    # ....................... #
    # Deferred

    async def get_vertices(self, refs: Sequence[VertexRef]) -> Sequence[BaseModel]:
        if not refs:
            return []

        with self.state.lock:
            verts = dict(self._verts())

        # Input order, found-only (missing refs omitted — batch-get semantics).
        return [
            self._vmodel(ref.kind, verts[(ref.kind, ref.key)])
            for ref in refs
            if (ref.kind, ref.key) in verts
        ]

    async def get_edges(self, refs: Sequence[EdgeRef]) -> Sequence[BaseModel]:
        if not refs:
            return []

        with self.state.lock:
            edges = list(self._edges_store())

        out: list[BaseModel] = []

        for ref in refs:
            rec = self._find_edge_rec(ref, edges)
            if rec is not None:
                out.append(self._emodel(ref.kind, rec["props"]))

        return out

    async def edge_exists(self, ref: EdgeRef) -> bool:
        with self.state.lock:
            return self._find_edge_rec(ref, self._edges_store()) is not None

    async def count_vertices(
        self, node_kind: str, *, property_filter: JsonDict | None = None
    ) -> int:
        node = self._node(node_kind)
        self._validate_filter(property_filter, node.encryption)

        with self.state.lock:
            verts = list(self._verts().items())

        return sum(
            bool(kind == node_kind and self._matches(props, property_filter))
            for (kind, _key), props in verts
        )

    async def count_edges(self, edge_kind: str, *, property_filter: JsonDict | None = None) -> int:
        edge = self._edge(edge_kind)
        self._validate_filter(property_filter, edge.encryption)

        with self.state.lock:
            edges = list(self._edges_store())

        return sum(
            bool(rec["kind"] == edge_kind and self._matches(rec["props"], property_filter))
            for rec in edges
        )

    async def incident_edges(
        self,
        origin: VertexRef,
        direction: GraphDirection,
        edge_kinds: frozenset[str],
        *,
        limit: int,
    ) -> Sequence[BaseModel]:
        with self.state.lock:
            edges = list(self._edges_store())

        out: list[BaseModel] = []

        for rec in edges:
            if edge_kinds and rec["kind"] not in edge_kinds:
                continue

            other_kind, _other_key = _other_endpoint(rec, origin, direction)
            if other_kind is None:
                continue

            out.append(self._emodel(rec["kind"], rec["props"]))

            if len(out) >= limit:
                break

        return out

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
        node = self._node(node_kind)
        self._validate_filter(property_filter, node.encryption)

        with self.state.lock:
            matches = [
                (key, props)
                for (kind, key), props in self._verts().items()
                if kind == node_kind and self._matches(props, property_filter)
            ]

        # Order by key (matches Neo4j's ``ORDER BY n.<key_field>``) for stable pagination.
        matches.sort(key=lambda item: item[0])
        window = matches[offset : offset + limit]
        return [self._vmodel(node_kind, props) for _key, props in window]

    async def find_edges(
        self,
        edge_kind: str,
        *,
        property_filter: JsonDict | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Sequence[BaseModel]:
        edge = self._edge(edge_kind)
        self._validate_filter(property_filter, edge.encryption)

        with self.state.lock:
            matches = [
                rec
                for rec in self._edges_store()
                if rec["kind"] == edge_kind and self._matches(rec["props"], property_filter)
            ]

        if edge.key_field is not None:
            key_field = edge.key_field
            matches.sort(key=lambda rec: str(rec["props"].get(key_field)))

        window = matches[offset : offset + limit]
        return [self._emodel(edge_kind, rec["props"]) for rec in window]

    # ....................... #

    def read_capabilities(self) -> GraphReadCapabilities:
        return GraphReadCapabilities(
            supports_vertex_streaming=True,
            supports_edge_streaming=True,
        )

    # ....................... #

    def find_vertices_stream(
        self,
        node_kind: str,
        *,
        property_filter: JsonDict | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[BaseModel]]:
        node = self._node(node_kind)
        self._validate_filter(property_filter, node.encryption)
        key_field = assert_vertex_streamable(
            node, kind=node_kind, capabilities=self.read_capabilities()
        )

        async def _fetch(after: Any | None, limit: int) -> Sequence[tuple[Any, BaseModel]]:
            with self.state.lock:
                matches = sorted(
                    (
                        (str(props.get(key_field)), props)
                        for (kind, _key), props in self._verts().items()
                        if kind == node_kind and self._matches(props, property_filter)
                    ),
                    key=lambda item: item[0],
                )

            # The keyset seek, in the one line a real backend spends a WHERE clause on:
            # strictly after the bookmark, never at it.
            page = [item for item in matches if after is None or item[0] > after][:limit]

            return [(key, self._vmodel(node_kind, props)) for key, props in page]

        return stream_keyset_pages(_fetch, chunk_size=chunk_size)

    # ....................... #

    def find_edges_stream(
        self,
        edge_kind: str,
        *,
        property_filter: JsonDict | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[BaseModel]]:
        edge = self._edge(edge_kind)
        self._validate_filter(property_filter, edge.encryption)
        key_field = assert_edge_streamable(
            edge, kind=edge_kind, capabilities=self.read_capabilities()
        )

        async def _fetch(after: Any | None, limit: int) -> Sequence[tuple[Any, BaseModel]]:
            with self.state.lock:
                matches = sorted(
                    (
                        (str(rec["props"].get(key_field)), rec["props"])
                        for rec in self._edges_store()
                        if rec["kind"] == edge_kind and self._matches(rec["props"], property_filter)
                    ),
                    key=lambda item: item[0],
                )

            page = [item for item in matches if after is None or item[0] > after][:limit]

            return [(key, self._emodel(edge_kind, props)) for key, props in page]

        return stream_keyset_pages(_fetch, chunk_size=chunk_size)

    async def vertex_degree(
        self,
        ref: VertexRef,
        *,
        direction: GraphDirection = GraphDirection.BOTH,
        edge_kinds: frozenset[str] | None = None,
    ) -> int:
        with self.state.lock:
            edges = list(self._edges_store())

        return sum(
            not (edge_kinds and rec["kind"] not in edge_kinds)
            and _other_endpoint(rec, ref, direction)[0] is not None
            for rec in edges
        )

    async def count_neighbors(
        self,
        ref: VertexRef,
        *,
        direction: GraphDirection = GraphDirection.BOTH,
        edge_kinds: frozenset[str] | None = None,
    ) -> int:
        with self.state.lock:
            edges = list(self._edges_store())

        neighbors: set[tuple[str, str]] = set()

        for rec in edges:
            if edge_kinds and rec["kind"] not in edge_kinds:
                continue

            other_kind, other_key = _other_endpoint(rec, ref, direction)
            if other_kind is not None and other_key is not None:
                neighbors.add((other_kind, other_key))

        return len(neighbors)

    async def update_edge(self, ref: EdgeRef, cmd: BaseModel) -> BaseModel:
        patch = cmd.model_dump(mode="json", exclude_none=True)
        patch.pop("from_key", None)
        patch.pop("to_key", None)

        with self.state.lock:
            rec = self._find_edge_rec(ref, self._edges_store())

            if rec is None:
                raise exc.not_found(f"Edge {ref.kind!r} not found", code="graph_edge_not_found")

            rec["props"] = {**rec["props"], **patch}
            merged = rec["props"]

        return self._emodel(ref.kind, merged)

    async def delete_edge(self, ref: EdgeRef) -> None:
        with self.state.lock:
            store = self._edges_store()
            rec = self._find_edge_rec(ref, store)

            if rec is not None:
                store.remove(rec)

    async def create_vertices(
        self, items: Sequence[tuple[str, BaseModel]], *, return_new: bool = True
    ) -> Sequence[BaseModel] | None:
        if not items:
            return [] if return_new else None

        stored: list[tuple[str, JsonDict]] = []

        with self.state.lock:
            verts = self._verts()

            for kind, cmd in items:
                node = self._node(kind)
                props = cmd.model_dump(mode="json", exclude_none=True)
                verts[(kind, str(props[node.key_field]))] = props
                stored.append((kind, props))

        if not return_new:
            return None

        return [self._vmodel(kind, props) for kind, props in stored]

    async def create_edges(
        self, items: Sequence[tuple[str, BaseModel]], *, return_new: bool = True
    ) -> Sequence[BaseModel] | None:
        if not items:
            return [] if return_new else None

        created: list[BaseModel] = []

        for kind, cmd in items:
            edge = await self.create_edge(kind, cmd, return_new=return_new)
            if return_new and edge is not None:
                created.append(edge)

        return created if return_new else None

    async def ensure_vertex(
        self, node_kind: str, cmd: BaseModel, *, return_new: bool = True
    ) -> BaseModel | None:
        node = self._node(node_kind)
        props = cmd.model_dump(mode="json", exclude_none=True)
        key = str(props[node.key_field])

        with self.state.lock:
            verts = self._verts()
            existing = verts.get((node_kind, key))

            if existing is None:
                verts[(node_kind, key)] = props
                stored = props
            else:
                stored = existing  # create-if-missing: existing returned unchanged

        return self._vmodel(node_kind, stored) if return_new else None

    async def delete_vertices(self, refs: Sequence[VertexRef]) -> None:
        if not refs:
            return

        with self.state.lock:
            verts = self._verts()
            for ref in refs:
                verts.pop((ref.kind, ref.key), None)

    async def delete_edges(self, refs: Sequence[EdgeRef]) -> None:
        if not refs:
            return

        with self.state.lock:
            store = self._edges_store()
            for ref in refs:
                rec = self._find_edge_rec(ref, store)
                if rec is not None:
                    store.remove(rec)


# ----------------------- #


def _other_endpoint(
    rec: JsonDict,
    origin: VertexRef,
    direction: GraphDirection,
) -> tuple[str | None, str | None]:
    """Return the (kind, key) of the neighbor reached from *origin* under *direction*."""

    out_match = rec["from_kind"] == origin.kind and rec["from_key"] == origin.key
    in_match = rec["to_kind"] == origin.kind and rec["to_key"] == origin.key

    if direction in (GraphDirection.OUT, GraphDirection.BOTH) and out_match:
        return rec["to_kind"], rec["to_key"]

    return (
        (rec["from_kind"], rec["from_key"])
        if direction in (GraphDirection.IN, GraphDirection.BOTH) and in_match
        else (None, None)
    )


# ....................... #
# In-memory traversal core (expand / shortest_path / scoped_walk)

_Node = tuple[str, str]
_Adjacency = dict[_Node, list[tuple[int, JsonDict, _Node]]]


def _adjacency(
    edges: Sequence[JsonDict],
    direction: GraphDirection,
    edge_kinds: frozenset[str],
) -> _Adjacency:
    """Node -> outgoing ``(edge_index, edge_rec, neighbor_node)`` under *direction*/kinds."""

    adj: _Adjacency = {}

    for idx, rec in enumerate(edges):
        if edge_kinds and rec["kind"] not in edge_kinds:
            continue

        frm: _Node = (rec["from_kind"], rec["from_key"])
        to: _Node = (rec["to_kind"], rec["to_key"])

        if direction in (GraphDirection.OUT, GraphDirection.BOTH):
            adj.setdefault(frm, []).append((idx, rec, to))
        if direction in (GraphDirection.IN, GraphDirection.BOTH):
            adj.setdefault(to, []).append((idx, rec, frm))

    return adj


def _enumerate_expand(
    start: _Node,
    adj: _Adjacency,
    verts: dict[_Node, JsonDict],
    max_depth: int,
) -> list[tuple[int, _Node, _Node, JsonDict]]:
    """All relationship-simple paths of length 1..max_depth as ``(depth, node, parent, edge)``.

    Relationship-simple mirrors Neo4j's ``*1..d`` (no edge repeated within a path; nodes may
    repeat). Only steps onto existing vertices are emitted.
    """

    steps: list[tuple[int, _Node, _Node, JsonDict]] = []
    frontier: list[tuple[_Node, frozenset[int]]] = [(start, frozenset())]

    for depth in range(1, max_depth + 1):
        nxt: list[tuple[_Node, frozenset[int]]] = []

        for term, used in frontier:
            for idx, rec, neighbor in adj.get(term, ()):
                if idx in used or neighbor not in verts:
                    continue
                steps.append((depth, neighbor, term, rec))
                nxt.append((neighbor, used | {idx}))

        frontier = nxt

    return steps


def _reconstruct(
    dst: _Node,
    prev: dict[_Node, tuple[_Node, JsonDict] | None],
    max_hops: int,
) -> tuple[list[_Node], list[JsonDict]] | None:
    nodes: list[_Node] = []
    edges: list[JsonDict] = []
    cur: _Node | None = dst

    while cur is not None:  # pyright: ignore[reportUnnecessaryComparison]
        nodes.append(cur)
        link = prev[cur]

        if link is None:
            break

        parent, rec = link
        edges.append(rec)
        cur = parent

    nodes.reverse()
    edges.reverse()

    # shortest path exceeds the hop bound -> none qualifies
    return None if len(edges) > max_hops else (nodes, edges)


def _bfs_shortest(
    src: _Node,
    dst: _Node,
    adj: _Adjacency,
    verts: dict[_Node, JsonDict],
    max_hops: int,
) -> tuple[list[_Node], list[JsonDict]] | None:
    """Fewest-hops path src -> dst (breadth-first)."""

    prev: dict[_Node, tuple[_Node, JsonDict] | None] = {src: None}
    queue: deque[_Node] = deque([src])

    while queue:
        node = queue.popleft()
        if node == dst:
            break
        for _idx, rec, neighbor in adj.get(node, ()):
            if neighbor in prev or neighbor not in verts:
                continue
            prev[neighbor] = (node, rec)
            queue.append(neighbor)

    return None if dst not in prev else _reconstruct(dst, prev, max_hops)


def _dijkstra_shortest(
    src: _Node,
    dst: _Node,
    adj: _Adjacency,
    verts: dict[_Node, JsonDict],
    weight_property: str,
    max_hops: int,
) -> tuple[list[_Node], list[JsonDict]] | None:
    """Least-cost path src -> dst (summed edge ``weight_property``) using at most ``max_hops`` edges.

    Dijkstra over ``(node, hops)`` states so ``max_hops`` bounds the *search*: the cheapest path
    *within* the bound is returned even when a cheaper path exists only beyond it. A plain
    cheapest-then-reject (global cheapest, drop if too long) would instead return nothing when a
    cheaper over-long path pre-empts a valid bounded one.
    """

    # State is (node, hops-so-far); popping in cost order means the first settled state on ``dst``
    # is the cheapest path reaching it within the bound.
    dist: dict[tuple[_Node, int], float] = {(src, 0): 0.0}
    prev: dict[tuple[_Node, int], tuple[_Node, int, JsonDict] | None] = {(src, 0): None}
    heap: list[tuple[float, int, tuple[_Node, int]]] = [(0.0, 0, (src, 0))]
    counter = 1
    settled: set[tuple[_Node, int]] = set()

    while heap:
        cost, _, state = heapq.heappop(heap)
        if state in settled:
            continue
        settled.add(state)
        node, hops = state

        if node == dst:
            return _reconstruct_hopped(state, prev)

        if hops >= max_hops:
            continue

        for _idx, rec, neighbor in adj.get(node, ()):
            if neighbor not in verts:
                continue
            weight = float(rec["props"].get(weight_property, 0.0) or 0.0)
            nxt = (neighbor, hops + 1)
            new_cost = cost + weight
            if nxt not in dist or new_cost < dist[nxt]:
                dist[nxt] = new_cost
                prev[nxt] = (node, hops, rec)
                heapq.heappush(heap, (new_cost, counter, nxt))
                counter += 1

    return None


def _reconstruct_hopped(
    dst_state: tuple[_Node, int],
    prev: dict[tuple[_Node, int], tuple[_Node, int, JsonDict] | None],
) -> tuple[list[_Node], list[JsonDict]]:
    """Walk the ``(node, hops)`` predecessor chain back to the source."""

    nodes: list[_Node] = []
    edges: list[JsonDict] = []
    cur: tuple[_Node, int] | None = dst_state

    while cur is not None:  # pyright: ignore[reportUnnecessaryComparison]
        nodes.append(cur[0])
        link = prev[cur]

        if link is None:
            break

        parent, parent_hops, rec = link
        edges.append(rec)
        cur = (parent, parent_hops)

    nodes.reverse()
    edges.reverse()

    return nodes, edges


def _advance_segment(
    frontier: set[tuple[_Node, frozenset[int]]],
    step: GraphPathStep,
    adj: _Adjacency,
    verts: dict[_Node, JsonDict],
) -> set[tuple[_Node, frozenset[int]]]:
    """Advance each ``(node, used_edges)`` by one ``*min..max`` segment (edge-simple)."""

    min_hops: int = step.min_hops
    max_hops: int = step.max_hops
    results: set[tuple[_Node, frozenset[int]]] = set()

    for start_node, used0 in frontier:
        current: set[tuple[_Node, frozenset[int]]] = {(start_node, used0)}

        if min_hops == 0:
            results |= current

        for hop in range(1, max_hops + 1):
            nxt: set[tuple[_Node, frozenset[int]]] = set()

            for node, used in current:
                for idx, _rec, neighbor in adj.get(node, ()):
                    if idx in used or neighbor not in verts:
                        continue

                    nxt.add((neighbor, used | frozenset({idx})))

            current = nxt
            if hop >= min_hops:
                results |= current

    return results

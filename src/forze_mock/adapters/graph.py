"""In-memory mock graph adapter (vertex/edge CRUD + neighbors).

Exists so handlers/kits can exercise the graph ports without a real engine. Traversal
beyond one hop (expand / shortest_path) and the raw escape hatch raise
``NotImplementedError`` — use ``forze_neo4j`` for those.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import final

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
        return self.state.graph_vertices.setdefault(self.namespace, {})

    def _edges_store(self) -> list[JsonDict]:
        return self.state.graph_edges.setdefault(self.namespace, [])

    # ....................... #

    def _node(self, kind: str) -> GraphNodeSpec[BaseModel]:
        node = self.spec.graph_node_by_kind(kind)

        if node is None:
            raise exc.configuration(
                f"Unknown node kind {kind!r}", code="graph_unknown_node_kind"
            )

        return node

    def _edge(self, kind: str) -> GraphEdgeSpec[BaseModel]:
        edge = self.spec.graph_edge_by_kind(kind)

        if edge is None:
            raise exc.configuration(
                f"Unknown edge kind {kind!r}", code="graph_unknown_edge_kind"
            )

        return edge

    # ....................... #

    def _vmodel(self, kind: str, props: JsonDict) -> BaseModel:
        return default_model_codec(self._node(kind).read).decode_mapping(
            props, trust_source=True
        )

    def _emodel(self, kind: str, props: JsonDict) -> BaseModel:
        return default_model_codec(self._edge(kind).read).decode_mapping(
            props, trust_source=True
        )

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
        if not ref.is_keyed:
            raise _nyi("get_edge in endpoints mode")

        edge = self._edge(ref.kind)
        kf = edge.key_field

        if kf is None:
            raise exc.configuration(
                f"Edge kind {ref.kind!r} has no key_field",
                code="graph_edge_missing_key_field",
            )

        with self.state.lock:
            for rec in self._edges_store():
                if rec["kind"] == ref.kind and str(rec["props"].get(kf)) == ref.key:
                    return self._emodel(ref.kind, rec["props"])

        return None

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

    async def expand(
        self, start: VertexRef, params: GraphWalkParams
    ) -> Sequence[GraphWalkStep]:
        raise _nyi("expand")

    async def shortest_path(
        self,
        from_ref: VertexRef,
        to_ref: VertexRef,
        params: ShortestPathParams,
    ) -> ShortestPathResult | None:
        raise _nyi("shortest_path")

    async def scoped_walk(
        self,
        anchor: VertexRef,
        params: ScopedWalkParams,
    ) -> Sequence[BaseModel]:
        raise _nyi("scoped_walk")

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
        return await self._write_edge(
            edge_kind, cmd, merge=False, return_new=return_new
        )

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
        endpoint = edge.endpoints[0]
        data = cmd.model_dump(mode="json", exclude_none=True)
        from_key = data.pop("from_key", None)
        to_key = data.pop("to_key", None)

        if from_key is None or to_key is None:
            raise exc.validation(
                f"Edge create for {edge_kind!r} requires 'from_key' and 'to_key'",
                code="graph_edge_endpoints_required",
            )

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
                for existing in store:
                    if (
                        existing["kind"] == edge_kind
                        and existing["from_key"] == rec["from_key"]
                        and existing["to_key"] == rec["to_key"]
                    ):
                        return (
                            self._emodel(edge_kind, existing["props"])
                            if return_new
                            else None
                        )

            store.append(rec)

        return self._emodel(edge_kind, data) if return_new else None

    # ....................... #
    # GraphRawQueryPort

    async def run(
        self, query: str, params: JsonDict | None = None
    ) -> Sequence[JsonDict]:
        raise _nyi("raw run")

    # ....................... #
    # Deferred

    async def get_vertices(self, refs: Sequence[VertexRef]) -> Sequence[BaseModel]:
        raise _nyi("get_vertices")

    async def get_edges(self, refs: Sequence[EdgeRef]) -> Sequence[BaseModel]:
        raise _nyi("get_edges")

    async def edge_exists(self, ref: EdgeRef) -> bool:
        raise _nyi("edge_exists")

    async def count_vertices(
        self, node_kind: str, *, property_filter: JsonDict | None = None
    ) -> int:
        del property_filter
        raise _nyi("count_vertices")

    async def count_edges(
        self, edge_kind: str, *, property_filter: JsonDict | None = None
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

    async def update_edge(self, ref: EdgeRef, cmd: BaseModel) -> BaseModel:
        raise _nyi("update_edge")

    async def delete_edge(self, ref: EdgeRef) -> None:
        raise _nyi("delete_edge")

    async def create_vertices(
        self, items: Sequence[tuple[str, BaseModel]], *, return_new: bool = True
    ) -> Sequence[BaseModel] | None:
        raise _nyi("create_vertices")

    async def create_edges(
        self, items: Sequence[tuple[str, BaseModel]], *, return_new: bool = True
    ) -> Sequence[BaseModel] | None:
        raise _nyi("create_edges")

    async def ensure_vertex(
        self, node_kind: str, cmd: BaseModel, *, return_new: bool = True
    ) -> BaseModel | None:
        raise _nyi("ensure_vertex")

    async def delete_vertices(self, refs: Sequence[VertexRef]) -> None:
        raise _nyi("delete_vertices")

    async def delete_edges(self, refs: Sequence[EdgeRef]) -> None:
        raise _nyi("delete_edges")


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

    if direction in (GraphDirection.IN, GraphDirection.BOTH) and in_match:
        return rec["from_kind"], rec["from_key"]

    return None, None

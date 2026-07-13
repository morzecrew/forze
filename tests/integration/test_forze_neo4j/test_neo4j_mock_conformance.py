"""Differential conformance: the mock read-introspection ≡ Neo4j on the same graph.

Seeds an identical graph in the in-memory mock and in Neo4j, runs each WS2 read method on
both, and asserts identical results — so the mock is a faithful reference for the read
surface (not merely "some subset works").
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.graph import (
    EdgeRef,
    GraphDirection,
    GraphEdgeDirectionality,
    GraphEdgeEndpoint,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
    GraphPathStep,
    GraphWalkParams,
    ScopedWalkParams,
    ShortestPathParams,
    VertexRef,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze_mock import MockDepsModule, MockState
from forze_neo4j.adapters import Neo4jGraphAdapter
from forze_neo4j.kernel.client import Neo4jClient
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class UserRead(BaseModel):
    id: str
    name: str | None = None


class UserCreate(BaseModel):
    id: str
    name: str | None = None


class FollowsRead(BaseModel):
    weight: int | None = None


class FollowsCreate(BaseModel):
    from_key: str
    to_key: str
    weight: int | None = None


class RatedRead(BaseModel):
    id: str
    score: int


class RatedCreate(BaseModel):
    id: str
    from_key: str
    to_key: str
    score: int


def _spec() -> GraphModuleSpec:
    return GraphModuleSpec(
        name="conf",
        nodes=(GraphNodeSpec(name="User", read=UserRead, create=UserCreate),),
        edges=(
            GraphEdgeSpec(
                name="FOLLOWS",
                read=FollowsRead,
                identity="endpoints",
                endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="User"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
            GraphEdgeSpec(
                name="RATED",
                read=RatedRead,
                identity="key",
                key_field="id",
                endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="User"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
        ),
    )


async def _seed(cmd: Any) -> None:
    await cmd.create_vertex("User", UserCreate(id="a", name="Ana"))
    await cmd.create_vertex("User", UserCreate(id="b", name="Bo"))
    await cmd.create_vertex("User", UserCreate(id="c", name="Ana"))
    await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b"))
    await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="c"))
    await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="b", to_key="c"))
    await cmd.create_edge("RATED", RatedCreate(id="r1", from_key="a", to_key="b", score=5))


def _u(k: str) -> VertexRef:
    return VertexRef(kind="User", key=k)


def _dump(models: Any) -> list[dict]:
    return sorted((m.model_dump() for m in models), key=lambda d: sorted(d.items()))


async def _read_snapshot(port: Any) -> dict[str, Any]:
    """Run every WS2 read method and capture comparable results."""

    incident = await port.incident_edges(
        _u("a"), GraphDirection.OUT, frozenset({"FOLLOWS"}), limit=10
    )
    return {
        "get_vertices": [
            m.model_dump() for m in await port.get_vertices([_u("c"), _u("missing"), _u("a")])
        ],
        "get_edges": _dump(
            await port.get_edges(
                [EdgeRef.by_key("RATED", "r1"), EdgeRef.by_endpoints("FOLLOWS", _u("a"), _u("b"))]
            )
        ),
        "edge_exists_keyed": await port.edge_exists(EdgeRef.by_key("RATED", "r1")),
        "edge_exists_missing": await port.edge_exists(EdgeRef.by_key("RATED", "no")),
        "edge_exists_endpoints": await port.edge_exists(
            EdgeRef.by_endpoints("FOLLOWS", _u("a"), _u("b"))
        ),
        "edge_exists_reversed": await port.edge_exists(
            EdgeRef.by_endpoints("FOLLOWS", _u("b"), _u("a"))
        ),
        "count_vertices": await port.count_vertices("User"),
        "count_vertices_filtered": await port.count_vertices(
            "User", property_filter={"name": "Ana"}
        ),
        "count_edges_follows": await port.count_edges("FOLLOWS"),
        "count_edges_rated_filtered": await port.count_edges("RATED", property_filter={"score": 5}),
        "degree_out": await port.vertex_degree(_u("a"), direction=GraphDirection.OUT),
        "degree_follows": await port.vertex_degree(
            _u("a"), direction=GraphDirection.OUT, edge_kinds=frozenset({"FOLLOWS"})
        ),
        "neighbors_out": await port.count_neighbors(
            _u("a"), direction=GraphDirection.OUT, edge_kinds=frozenset({"FOLLOWS"})
        ),
        "incident_count": len(incident),
        # find_* (deterministic order: vertices by id, keyed edges by key)
        "find_vertices_all": [m.model_dump() for m in await port.find_vertices("User")],
        "find_vertices_paged": [
            m.model_dump() for m in await port.find_vertices("User", limit=1, offset=1)
        ],
        "find_vertices_filtered": [
            m.model_dump()
            for m in await port.find_vertices("User", property_filter={"name": "Ana"})
        ],
        "find_edges_rated": [m.model_dump() for m in await port.find_edges("RATED")],
    }


class RatedUpdate(BaseModel):
    score: int


async def _apply_writes(cmd: Any) -> None:
    # bulk create
    await cmd.create_vertices(
        [
            ("User", UserCreate(id="a", name="A")),
            ("User", UserCreate(id="b", name="B")),
            ("User", UserCreate(id="c", name="C")),
        ]
    )
    # ensure: existing 'a' stays unchanged, 'd' is created
    await cmd.ensure_vertex("User", UserCreate(id="a", name="CHANGED"))
    await cmd.ensure_vertex("User", UserCreate(id="d", name="D"))
    # bulk edges, then update one, delete another
    await cmd.create_edges(
        [
            ("RATED", RatedCreate(id="r1", from_key="a", to_key="b", score=5)),
            ("RATED", RatedCreate(id="r2", from_key="a", to_key="c", score=3)),
        ]
    )
    await cmd.update_edge(EdgeRef.by_key("RATED", "r1"), RatedUpdate(score=9))
    await cmd.delete_edge(EdgeRef.by_key("RATED", "r2"))
    # bulk delete a vertex
    await cmd.delete_vertices([_u("c")])


async def _write_snapshot(port: Any) -> dict[str, Any]:
    return {
        "users": [m.model_dump() for m in await port.find_vertices("User")],
        "rated": [m.model_dump() for m in await port.find_edges("RATED")],
        "count_users": await port.count_vertices("User"),
        "count_rated": await port.count_edges("RATED"),
        "r1_exists": await port.edge_exists(EdgeRef.by_key("RATED", "r1")),
        "r2_exists": await port.edge_exists(EdgeRef.by_key("RATED", "r2")),
    }


async def test_mock_matches_neo4j_write_effects(neo4j_client: Neo4jClient) -> None:
    spec = _spec()

    mock_ctx: ExecutionContext = context_from_deps(MockDepsModule(state=MockState())())
    await _apply_writes(mock_ctx.graph.command(spec))

    neo = Neo4jGraphAdapter(spec=spec, client=neo4j_client)
    await _apply_writes(neo)

    assert await _write_snapshot(mock_ctx.graph.query(spec)) == await _write_snapshot(neo)


class TagRead(BaseModel):
    id: str


class TagCreate(BaseModel):
    id: str


class TaggedCreate(BaseModel):
    from_key: str
    to_key: str
    from_kind: str
    to_kind: str


class TaggedRead(BaseModel):
    weight: int | None = None


def _multi_spec() -> GraphModuleSpec:
    return GraphModuleSpec(
        name="multi",
        nodes=(
            GraphNodeSpec(name="Post", read=TagRead, create=TagCreate),
            GraphNodeSpec(name="Note", read=TagRead, create=TagCreate),
            GraphNodeSpec(name="Tag", read=TagRead, create=TagCreate),
        ),
        edges=(
            GraphEdgeSpec(
                name="TAGGED",
                read=TaggedRead,
                identity="endpoints",
                endpoints=(
                    GraphEdgeEndpoint(from_kind="Post", to_kind="Tag"),
                    GraphEdgeEndpoint(from_kind="Note", to_kind="Tag"),
                ),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
        ),
    )


async def _multi_apply(cmd: Any) -> None:
    await cmd.create_vertex("Post", TagCreate(id="1"))
    await cmd.create_vertex("Note", TagCreate(id="1"))  # same key, different kind
    await cmd.create_vertex("Tag", TagCreate(id="x"))
    # Two edges with identical key VALUES ("1"→"x") but different endpoint kinds — distinct.
    await cmd.create_edge(
        "TAGGED", TaggedCreate(from_key="1", to_key="x", from_kind="Post", to_kind="Tag")
    )
    await cmd.create_edge(
        "TAGGED", TaggedCreate(from_key="1", to_key="x", from_kind="Note", to_kind="Tag")
    )


async def _multi_snapshot(port: Any) -> dict[str, Any]:
    def _ref(kind: str) -> EdgeRef:
        return EdgeRef.by_endpoints(
            "TAGGED", VertexRef(kind=kind, key="1"), VertexRef(kind="Tag", key="x")
        )

    return {
        "count": await port.count_edges("TAGGED"),
        "post_edge_exists": await port.edge_exists(_ref("Post")),
        "note_edge_exists": await port.edge_exists(_ref("Note")),
    }


async def test_mock_matches_neo4j_multi_endpoint_edges(neo4j_client: Neo4jClient) -> None:
    spec = _multi_spec()

    mock_ctx: ExecutionContext = context_from_deps(MockDepsModule(state=MockState())())
    await _multi_apply(mock_ctx.graph.command(spec))

    neo = Neo4jGraphAdapter(spec=spec, client=neo4j_client)
    await _multi_apply(neo)

    mock_snap = await _multi_snapshot(mock_ctx.graph.query(spec))
    neo_snap = await _multi_snapshot(neo)

    assert mock_snap == neo_snap
    assert neo_snap == {"count": 2, "post_edge_exists": True, "note_edge_exists": True}


async def test_mock_matches_neo4j_multi_endpoint_kind_sensitive(
    neo4j_client: Neo4jClient,
) -> None:
    """With only the Post→Tag edge present, an endpoints lookup for the same key values under a
    different kind pair (Note→Tag) must miss on both planes — proving kind-sensitive matching
    (same keys, different kinds → distinct identity)."""

    spec = _multi_spec()

    async def _apply_one(cmd: Any) -> None:
        await cmd.create_vertex("Post", TagCreate(id="1"))
        await cmd.create_vertex("Note", TagCreate(id="1"))  # same key, different kind
        await cmd.create_vertex("Tag", TagCreate(id="x"))
        # Only Post→Tag — no Note→Tag edge is created.
        await cmd.create_edge(
            "TAGGED",
            TaggedCreate(from_key="1", to_key="x", from_kind="Post", to_kind="Tag"),
        )

    post_ref = EdgeRef.by_endpoints(
        "TAGGED", VertexRef(kind="Post", key="1"), VertexRef(kind="Tag", key="x")
    )
    note_ref = EdgeRef.by_endpoints(
        "TAGGED", VertexRef(kind="Note", key="1"), VertexRef(kind="Tag", key="x")
    )

    mock_ctx: ExecutionContext = context_from_deps(MockDepsModule(state=MockState())())
    await _apply_one(mock_ctx.graph.command(spec))
    neo = Neo4jGraphAdapter(spec=spec, client=neo4j_client)
    await _apply_one(neo)

    for port in (mock_ctx.graph.query(spec), neo):
        assert await port.edge_exists(post_ref) is True
        assert await port.edge_exists(note_ref) is False  # same keys, wrong kinds → miss


async def _traversal_apply(cmd: Any) -> None:
    for k in ("a", "b", "c", "d"):
        await cmd.create_vertex("User", UserCreate(id=k))
    # weighted diamond: direct a->c (5) vs detour a->b->c (1+1); plus a->d
    await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b", weight=1))
    await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="b", to_key="c", weight=1))
    await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="c", weight=5))
    await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="d", weight=1))


async def _traversal_snapshot(port: Any) -> dict[str, Any]:
    steps = await port.expand(
        _u("a"),
        GraphWalkParams(max_depth=2, max_results=100, direction=GraphDirection.OUT),
    )
    sp = await port.shortest_path(
        _u("a"), _u("c"), ShortestPathParams(max_hops=5, edge_kinds=frozenset({"FOLLOWS"}))
    )
    one = GraphPathStep(edge_kinds=frozenset({"FOLLOWS"}), direction=GraphDirection.OUT)
    walk1 = await port.scoped_walk(_u("a"), ScopedWalkParams(steps=(one,), target_kind="User"))
    walk2 = await port.scoped_walk(_u("a"), ScopedWalkParams(steps=(one, one), target_kind="User"))

    return {
        # invariant multiset (intra-depth order is unspecified in Neo4j)
        "expand": sorted(
            (s.depth, s.vertex.id, s.parent_ref.key, s.from_parent.weight) for s in steps
        ),
        "sp_len": None if sp is None else len(sp.edges),
        "sp_ends": None if sp is None else (sp.vertices[0].id, sp.vertices[-1].id),
        "walk1": sorted(v.id for v in walk1),
        "walk2": sorted(v.id for v in walk2),
    }


async def test_mock_matches_neo4j_traversal_invariants(neo4j_client: Neo4jClient) -> None:
    spec = _spec()

    mock_ctx: ExecutionContext = context_from_deps(MockDepsModule(state=MockState())())
    await _traversal_apply(mock_ctx.graph.command(spec))

    neo = Neo4jGraphAdapter(spec=spec, client=neo4j_client)
    await _traversal_apply(neo)

    assert await _traversal_snapshot(mock_ctx.graph.query(spec)) == await _traversal_snapshot(neo)


async def test_mock_matches_neo4j_weighted_shortest_path(
    gds_neo4j_client: Neo4jClient,
) -> None:
    """Weighted shortest_path (``weight_property``) conformance on the diamond: both planes take
    the cheaper 2-hop detour a→b→c over the costly direct a→c edge (needs Neo4j GDS)."""

    spec = _spec()

    mock_ctx: ExecutionContext = context_from_deps(MockDepsModule(state=MockState())())
    await _traversal_apply(mock_ctx.graph.command(spec))

    neo = Neo4jGraphAdapter(spec=spec, client=gds_neo4j_client, graph_algorithms=True)
    await _traversal_apply(neo)

    params = ShortestPathParams(
        max_hops=5, weight_property="weight", edge_kinds=frozenset({"FOLLOWS"})
    )
    mock_sp = await mock_ctx.graph.query(spec).shortest_path(_u("a"), _u("c"), params)
    neo_sp = await neo.shortest_path(_u("a"), _u("c"), params)

    def _route(sp: Any) -> list[str] | None:
        return None if sp is None else [v.id for v in sp.vertices]

    assert _route(mock_sp) == _route(neo_sp) == ["a", "b", "c"]


async def _filter_rejection_snapshot(port: Any) -> list[tuple[Any, str, str]]:
    """(kind, code, summary) raised by each property-filter entry point for a bad key."""

    bad = {"name = $tenant OR true //": "x"}
    observed: list[tuple[Any, str, str]] = []

    for call in (
        lambda: port.count_vertices("User", property_filter=bad),
        lambda: port.count_edges("RATED", property_filter=bad),
        lambda: port.find_vertices("User", property_filter=bad),
        lambda: port.find_edges("RATED", property_filter=bad),
    ):
        with pytest.raises(CoreException) as ei:
            await call()

        observed.append((ei.value.kind, ei.value.code, ei.value.summary))

    return observed


async def test_mock_matches_neo4j_filter_key_rejection(neo4j_client: Neo4jClient) -> None:
    """A non-identifier property-filter key fails closed identically on both planes —
    same exception kind, code, and message on every filter-taking entry point — so a
    test that passes against the mock cannot smuggle a key Neo4j would reject."""

    spec = _spec()

    mock_ctx: ExecutionContext = context_from_deps(MockDepsModule(state=MockState())())
    await _seed(mock_ctx.graph.command(spec))

    neo = Neo4jGraphAdapter(spec=spec, client=neo4j_client)
    await _seed(neo)

    mock_snap = await _filter_rejection_snapshot(mock_ctx.graph.query(spec))
    neo_snap = await _filter_rejection_snapshot(neo)

    assert mock_snap == neo_snap
    assert all(code == "graph_filter_key_invalid" for _kind, code, _summary in neo_snap)


async def test_mock_matches_neo4j_read_surface(neo4j_client: Neo4jClient) -> None:
    spec = _spec()

    # Mock plane.
    mock_ctx: ExecutionContext = context_from_deps(MockDepsModule(state=MockState())())
    mock_cmd = mock_ctx.graph.command(spec)
    mock_qry = mock_ctx.graph.query(spec)
    await _seed(mock_cmd)

    # Neo4j plane.
    neo = Neo4jGraphAdapter(spec=spec, client=neo4j_client)
    await _seed(neo)

    mock_snap = await _read_snapshot(mock_qry)
    neo_snap = await _read_snapshot(neo)

    assert mock_snap == neo_snap

"""Coverage for :mod:`forze_mock.adapters.graph` error/branch paths."""

from __future__ import annotations

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
    GraphWalkParams,
    ScopedWalkParams,
    ShortestPathParams,
    VertexRef,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from tests.support.execution_context import context_from_deps

from forze_mock import MockDepsModule, MockState

# ----------------------- #


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
    edge_id: str
    score: int | None = None


class RatedCreate(BaseModel):
    from_key: str
    to_key: str
    edge_id: str
    score: int | None = None


def _spec() -> GraphModuleSpec:
    return GraphModuleSpec(
        name="social",
        nodes=(
            GraphNodeSpec(name="User", read=UserRead, create=UserCreate),
            GraphNodeSpec(name="Movie", read=UserRead, create=UserCreate),
        ),
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
                key_field="edge_id",
                endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="Movie"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
        ),
    )


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule(state=MockState())())


# ....................... #


class TestUnknownKinds:
    @pytest.mark.asyncio
    async def test_unknown_node_kind_raises(self, ctx: ExecutionContext) -> None:
        cmd = ctx.graph.command(_spec())
        with pytest.raises(CoreException, match="graph_unknown_node_kind"):
            await cmd.create_vertex("Ghost", UserCreate(id="a"))

    @pytest.mark.asyncio
    async def test_unknown_edge_kind_raises(self, ctx: ExecutionContext) -> None:
        cmd = ctx.graph.command(_spec())
        await cmd.create_vertex("User", UserCreate(id="a"))
        await cmd.create_vertex("User", UserCreate(id="b"))
        with pytest.raises(CoreException, match="graph_unknown_edge_kind"):
            await cmd.create_edge("GHOST", FollowsCreate(from_key="a", to_key="b"))


# ....................... #


class TestGetEdge:
    @pytest.mark.asyncio
    async def test_get_edge_endpoints_mode_returns_none_when_absent(
        self, ctx: ExecutionContext
    ) -> None:
        # Endpoints mode is now supported: a missing edge returns None (no longer NYI).
        qry = ctx.graph.query(_spec())
        ref = EdgeRef.by_endpoints(
            "FOLLOWS",
            VertexRef(kind="User", key="a"),
            VertexRef(kind="User", key="b"),
        )
        assert await qry.get_edge(ref) is None

    @pytest.mark.asyncio
    async def test_get_edge_without_key_field_raises(
        self, ctx: ExecutionContext
    ) -> None:
        # FOLLOWS is an endpoints edge (key_field is None); a keyed ref against it
        # trips the "edge has no key_field" configuration error.
        qry = ctx.graph.query(_spec())
        with pytest.raises(CoreException, match="graph_edge_missing_key_field"):
            await qry.get_edge(EdgeRef.by_key("FOLLOWS", "k"))

    @pytest.mark.asyncio
    async def test_get_edge_keyed_hit_and_miss(self, ctx: ExecutionContext) -> None:
        cmd = ctx.graph.command(_spec())
        qry = ctx.graph.query(_spec())
        await cmd.create_vertex("User", UserCreate(id="u"))
        await cmd.create_vertex("Movie", UserCreate(id="m"))
        await cmd.create_edge(
            "RATED", RatedCreate(from_key="u", to_key="m", edge_id="e1", score=5)
        )

        found = await qry.get_edge(EdgeRef.by_key("RATED", "e1"))
        assert found is not None and found.score == 5

        assert await qry.get_edge(EdgeRef.by_key("RATED", "missing")) is None


# ....................... #


class TestNeighborsFilters:
    @pytest.mark.asyncio
    async def test_edge_kinds_filter_excludes_other_kinds(
        self, ctx: ExecutionContext
    ) -> None:
        cmd = ctx.graph.command(_spec())
        qry = ctx.graph.query(_spec())
        await cmd.create_vertex("User", UserCreate(id="a"))
        await cmd.create_vertex("User", UserCreate(id="b"))
        await cmd.create_vertex("Movie", UserCreate(id="m"))
        await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b"))
        await cmd.create_edge(
            "RATED", RatedCreate(from_key="a", to_key="m", edge_id="e1")
        )

        # Only RATED requested: the FOLLOWS edge is filtered out by kind.
        out = await qry.neighbors(
            VertexRef(kind="User", key="a"),
            GraphDirection.OUT,
            frozenset({"RATED"}),
            limit=10,
        )
        assert [n.other.id for n in out] == ["m"]

    @pytest.mark.asyncio
    async def test_to_vertex_kinds_filter(self, ctx: ExecutionContext) -> None:
        cmd = ctx.graph.command(_spec())
        qry = ctx.graph.query(_spec())
        await cmd.create_vertex("User", UserCreate(id="a"))
        await cmd.create_vertex("User", UserCreate(id="b"))
        await cmd.create_vertex("Movie", UserCreate(id="m"))
        await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b"))
        await cmd.create_edge(
            "RATED", RatedCreate(from_key="a", to_key="m", edge_id="e1")
        )

        # Both edge kinds allowed, but only Movie endpoints are kept.
        out = await qry.neighbors(
            VertexRef(kind="User", key="a"),
            GraphDirection.OUT,
            frozenset({"FOLLOWS", "RATED"}),
            limit=10,
            to_vertex_kinds=frozenset({"Movie"}),
        )
        assert [n.other.id for n in out] == ["m"]

    @pytest.mark.asyncio
    async def test_edge_not_touching_origin_in_direction_is_skipped(
        self, ctx: ExecutionContext
    ) -> None:
        # An edge a->b, queried for OUT neighbors of b: the edge does not start at
        # b, so _other_endpoint returns (None, None) and the row is skipped.
        cmd = ctx.graph.command(_spec())
        qry = ctx.graph.query(_spec())
        await cmd.create_vertex("User", UserCreate(id="a"))
        await cmd.create_vertex("User", UserCreate(id="b"))
        await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b"))

        out = await qry.neighbors(
            VertexRef(kind="User", key="b"),
            GraphDirection.OUT,
            frozenset({"FOLLOWS"}),
            limit=10,
        )
        assert out == []

    @pytest.mark.asyncio
    async def test_limit_truncates_neighbors(self, ctx: ExecutionContext) -> None:
        cmd = ctx.graph.command(_spec())
        qry = ctx.graph.query(_spec())
        await cmd.create_vertex("User", UserCreate(id="a"))
        for k in ("b", "c", "d"):
            await cmd.create_vertex("User", UserCreate(id=k))
            await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key=k))

        out = await qry.neighbors(
            VertexRef(kind="User", key="a"),
            GraphDirection.OUT,
            frozenset({"FOLLOWS"}),
            limit=2,
        )
        assert len(out) == 2

    @pytest.mark.asyncio
    async def test_both_direction_reaches_in_and_out(
        self, ctx: ExecutionContext
    ) -> None:
        cmd = ctx.graph.command(_spec())
        qry = ctx.graph.query(_spec())
        await cmd.create_vertex("User", UserCreate(id="a"))
        await cmd.create_vertex("User", UserCreate(id="b"))
        await cmd.create_vertex("User", UserCreate(id="c"))
        await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b"))
        await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="c", to_key="a"))

        out = await qry.neighbors(
            VertexRef(kind="User", key="a"),
            GraphDirection.BOTH,
            frozenset({"FOLLOWS"}),
            limit=10,
        )
        assert {n.other.id for n in out} == {"b", "c"}

    @pytest.mark.asyncio
    async def test_missing_neighbor_vertex_is_skipped(
        self, ctx: ExecutionContext
    ) -> None:
        cmd = ctx.graph.command(_spec())
        qry = ctx.graph.query(_spec())
        await cmd.create_vertex("User", UserCreate(id="a"))
        await cmd.create_vertex("User", UserCreate(id="b"))
        await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b"))

        # Delete the far endpoint: the dangling edge yields no neighbor row.
        await cmd.delete_vertex(VertexRef(kind="User", key="b"))
        out = await qry.neighbors(
            VertexRef(kind="User", key="a"),
            GraphDirection.OUT,
            frozenset({"FOLLOWS"}),
            limit=10,
        )
        assert out == []


# ....................... #


class TestTraversalStubs:
    @pytest.mark.asyncio
    async def test_expand_not_implemented(self, ctx: ExecutionContext) -> None:
        qry = ctx.graph.query(_spec())
        with pytest.raises(NotImplementedError, match="expand"):
            await qry.expand(
                VertexRef(kind="User", key="a"),
                GraphWalkParams(max_depth=2, max_results=10),
            )

    @pytest.mark.asyncio
    async def test_shortest_path_not_implemented(self, ctx: ExecutionContext) -> None:
        qry = ctx.graph.query(_spec())
        with pytest.raises(NotImplementedError, match="shortest_path"):
            await qry.shortest_path(
                VertexRef(kind="User", key="a"),
                VertexRef(kind="User", key="b"),
                ShortestPathParams(max_hops=3),
            )

    @pytest.mark.asyncio
    async def test_scoped_walk_not_implemented(self, ctx: ExecutionContext) -> None:
        from forze.application.contracts.graph import GraphPathStep

        qry = ctx.graph.query(_spec())
        params = ScopedWalkParams(
            steps=(
                GraphPathStep(
                    edge_kinds=frozenset({"FOLLOWS"}),
                    direction=GraphDirection.OUT,
                ),
            ),
            target_kind="User",
        )
        with pytest.raises(NotImplementedError, match="scoped_walk"):
            await qry.scoped_walk(VertexRef(kind="User", key="a"), params)


# ....................... #


class TestDeferredStubs:
    """The deferred (not-yet-implemented) port methods all raise NotImplementedError."""

    @pytest.mark.asyncio
    async def test_raw_run(self, ctx: ExecutionContext) -> None:
        qry = ctx.graph.query(_spec())
        with pytest.raises(NotImplementedError, match="raw run"):
            await qry.run("MATCH (n) RETURN n")

    @pytest.mark.asyncio
    async def test_deferred_query_methods(self, ctx: ExecutionContext) -> None:
        # WS2 read-introspection is implemented; these remain deferred on the mock.
        qry = ctx.graph.query(_spec())
        u = VertexRef(kind="User", key="a")
        v = VertexRef(kind="User", key="b")
        sp = ShortestPathParams(max_hops=2)

        for coro in (
            qry.k_shortest_paths(u, v, sp, k=2),
            qry.find_vertices("User"),
            qry.find_edges("RATED"),
        ):
            with pytest.raises(NotImplementedError):
                await coro

    @pytest.mark.asyncio
    async def test_deferred_command_methods(self, ctx: ExecutionContext) -> None:
        cmd = ctx.graph.command(_spec())
        u = VertexRef(kind="User", key="a")
        ekind = EdgeRef.by_key("RATED", "e1")

        for coro in (
            cmd.update_edge(ekind, RatedCreate(from_key="a", to_key="m", edge_id="e1")),
            cmd.delete_edge(ekind),
            cmd.create_vertices([("User", UserCreate(id="a"))]),
            cmd.create_edges([("RATED", RatedCreate(from_key="a", to_key="m", edge_id="e1"))]),
            cmd.ensure_vertex("User", UserCreate(id="a")),
            cmd.delete_vertices([u]),
            cmd.delete_edges([ekind]),
        ):
            with pytest.raises(NotImplementedError):
                await coro


# ....................... #


class TestWriteEdgeValidation:
    @pytest.mark.asyncio
    async def test_create_edge_missing_endpoints_raises(
        self, ctx: ExecutionContext
    ) -> None:
        cmd = ctx.graph.command(_spec())

        class _NoEndpoints(BaseModel):
            weight: int | None = None

        with pytest.raises(CoreException, match="graph_edge_endpoints_required"):
            await cmd.create_edge("FOLLOWS", _NoEndpoints(weight=1))

    @pytest.mark.asyncio
    async def test_create_edge_return_new_false(self, ctx: ExecutionContext) -> None:
        cmd = ctx.graph.command(_spec())
        await cmd.create_vertex("User", UserCreate(id="a"))
        await cmd.create_vertex("User", UserCreate(id="b"))
        out = await cmd.create_edge(
            "FOLLOWS", FollowsCreate(from_key="a", to_key="b"), return_new=False
        )
        assert out is None

    @pytest.mark.asyncio
    async def test_ensure_edge_merge_return_new_false(
        self, ctx: ExecutionContext
    ) -> None:
        cmd = ctx.graph.command(_spec())
        await cmd.create_vertex("User", UserCreate(id="a"))
        await cmd.create_vertex("User", UserCreate(id="b"))
        await cmd.ensure_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b"))
        # Second ensure hits the merge branch; return_new=False yields None.
        out = await cmd.ensure_edge(
            "FOLLOWS", FollowsCreate(from_key="a", to_key="b"), return_new=False
        )
        assert out is None

    @pytest.mark.asyncio
    async def test_update_vertex_missing_raises(self, ctx: ExecutionContext) -> None:
        cmd = ctx.graph.command(_spec())
        with pytest.raises(CoreException, match="graph_vertex_not_found"):
            await cmd.update_vertex(
                VertexRef(kind="User", key="ghost"), UserCreate(id="ghost")
            )

    @pytest.mark.asyncio
    async def test_create_vertex_return_new_false(
        self, ctx: ExecutionContext
    ) -> None:
        cmd = ctx.graph.command(_spec())
        out = await cmd.create_vertex("User", UserCreate(id="a"), return_new=False)
        assert out is None

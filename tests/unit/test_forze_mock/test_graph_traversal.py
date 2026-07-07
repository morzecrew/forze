"""Unit tests for the mock multi-hop traversal core (expand / shortest_path / scoped_walk)."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.graph import (
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
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.unit


class UserRead(BaseModel):
    id: str


class UserCreate(BaseModel):
    id: str


class LinkRead(BaseModel):
    weight: int | None = None


class LinkCreate(BaseModel):
    from_key: str
    to_key: str
    weight: int | None = None


def _spec() -> GraphModuleSpec:
    return GraphModuleSpec(
        name="t",
        nodes=(GraphNodeSpec(name="User", read=UserRead, create=UserCreate),),
        edges=(
            GraphEdgeSpec(
                name="LINK",
                read=LinkRead,
                identity="endpoints",
                endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="User"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
        ),
    )


def _u(k: str) -> VertexRef:
    return VertexRef(kind="User", key=k)


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule(state=MockState())())


async def _seed(cmd: object, links: list[tuple[str, str, int]]) -> None:
    keys = {k for pair in links for k in pair[:2]}
    for k in sorted(keys):
        await cmd.create_vertex("User", UserCreate(id=k))
    for a, b, w in links:
        await cmd.create_edge("LINK", LinkCreate(from_key=a, to_key=b, weight=w))


@pytest.mark.asyncio
async def test_expand_by_depth(ctx: ExecutionContext) -> None:
    spec = _spec()
    await _seed(ctx.graph.command(spec), [("a", "b", 1), ("b", "c", 1), ("a", "c", 1), ("a", "d", 1)])
    q = ctx.graph.query(spec)

    steps = await q.expand(
        _u("a"), GraphWalkParams(max_depth=2, max_results=100, direction=GraphDirection.OUT)
    )
    # depth 1: a->b, a->c, a->d ; depth 2: a->b->c
    by_depth = sorted((s.depth, s.vertex.id, s.parent_ref.key) for s in steps)
    assert by_depth == [
        (1, "b", "a"),
        (1, "c", "a"),
        (1, "d", "a"),
        (2, "c", "b"),
    ]


@pytest.mark.asyncio
async def test_expand_respects_direction_and_edge_kinds(ctx: ExecutionContext) -> None:
    spec = _spec()
    await _seed(ctx.graph.command(spec), [("a", "b", 1)])
    q = ctx.graph.query(spec)

    # IN from b reaches a; OUT from b reaches nothing
    into_b = await q.expand(_u("b"), GraphWalkParams(max_depth=1, max_results=10, direction=GraphDirection.IN))
    assert [s.vertex.id for s in into_b] == ["a"]
    out_b = await q.expand(_u("b"), GraphWalkParams(max_depth=1, max_results=10, direction=GraphDirection.OUT))
    assert out_b == []


@pytest.mark.asyncio
async def test_shortest_path_unweighted_fewest_hops(ctx: ExecutionContext) -> None:
    spec = _spec()
    await _seed(ctx.graph.command(spec), [("a", "b", 1), ("b", "c", 1), ("a", "c", 1)])
    q = ctx.graph.query(spec)

    path = await q.shortest_path(_u("a"), _u("c"), ShortestPathParams(max_hops=5))
    assert path is not None
    assert [v.id for v in path.vertices] == ["a", "c"]  # direct, 1 hop
    assert len(path.edges) == 1

    # unreachable
    assert await q.shortest_path(_u("c"), _u("a"), ShortestPathParams(max_hops=5)) is None
    # exceeds hop bound
    assert await q.shortest_path(_u("a"), _u("c"), ShortestPathParams(max_hops=0)) is None


@pytest.mark.asyncio
async def test_shortest_path_weighted_prefers_low_cost(ctx: ExecutionContext) -> None:
    spec = _spec()
    # direct a->c costs 5; detour a->b->c costs 2
    await _seed(ctx.graph.command(spec), [("a", "c", 5), ("a", "b", 1), ("b", "c", 1)])
    q = ctx.graph.query(spec)

    path = await q.shortest_path(
        _u("a"), _u("c"), ShortestPathParams(max_hops=5, weight_property="weight")
    )
    assert path is not None
    assert [v.id for v in path.vertices] == ["a", "b", "c"]  # cheaper 2-hop route

    # max_hops bounds the search: with only 1 hop allowed the cheaper 2-hop detour is out of
    # reach, so the direct (costlier but bounded) a->c route is returned — not dropped.
    capped = await q.shortest_path(
        _u("a"), _u("c"), ShortestPathParams(max_hops=1, weight_property="weight")
    )
    assert capped is not None
    assert [v.id for v in capped.vertices] == ["a", "c"]


@pytest.mark.asyncio
async def test_scoped_walk_segments(ctx: ExecutionContext) -> None:
    spec = _spec()
    await _seed(ctx.graph.command(spec), [("a", "b", 1), ("b", "c", 1), ("a", "d", 1)])
    q = ctx.graph.query(spec)

    one = GraphPathStep(edge_kinds=frozenset({"LINK"}), direction=GraphDirection.OUT)

    hop1 = await q.scoped_walk(_u("a"), ScopedWalkParams(steps=(one,), target_kind="User"))
    assert sorted(v.id for v in hop1) == ["b", "d"]

    hop2 = await q.scoped_walk(
        _u("a"), ScopedWalkParams(steps=(one, one), target_kind="User")
    )
    assert sorted(v.id for v in hop2) == ["c"]  # a->b->c

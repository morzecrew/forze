"""Edge-case coverage for the mock graph adapter: traversal branches (missing endpoints,
cycles, edge-kind filters, unreachable targets, zero-hop segments) and the bulk / deferred
read-write surface (empty inputs, ``return_new=False``, keyed-ensure validation)."""

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
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.unit


class UserRead(BaseModel):
    id: str


class UserCreate(BaseModel):
    id: str


class EdgeRead(BaseModel):
    weight: int | None = None


class LinkCreate(BaseModel):
    from_key: str
    to_key: str
    weight: int | None = None


class LikeCreate(BaseModel):
    from_key: str
    to_key: str


class KeyedCreate(BaseModel):
    from_key: str
    to_key: str
    edge_id: str | None = None  # optional so a keyed ensure can omit it


def _spec() -> GraphModuleSpec:
    """Two edge kinds so ``edge_kinds`` filtering has something to skip; plus a keyed kind."""

    return GraphModuleSpec(
        name="cov",
        nodes=(GraphNodeSpec(name="User", read=UserRead, create=UserCreate),),
        edges=(
            GraphEdgeSpec(
                name="LINK",
                read=EdgeRead,
                identity="endpoints",
                endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="User"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
            GraphEdgeSpec(
                name="LIKE",
                read=EdgeRead,
                identity="endpoints",
                endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="User"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
            GraphEdgeSpec(
                name="KEYED",
                read=EdgeRead,
                identity="key",
                key_field="edge_id",
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


# ----------------------- #
# traversal branches


@pytest.mark.asyncio
async def test_expand_edge_kinds_filter_skips_other_kinds(ctx: ExecutionContext) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    for k in ("a", "b", "c"):
        await cmd.create_vertex("User", UserCreate(id=k))
    await cmd.create_edge("LINK", LinkCreate(from_key="a", to_key="b"))
    await cmd.create_edge("LIKE", LikeCreate(from_key="a", to_key="c"))

    steps = await ctx.graph.query(spec).expand(
        _u("a"),
        GraphWalkParams(
            max_depth=1,
            max_results=10,
            direction=GraphDirection.OUT,
            edge_kinds=frozenset({"LINK"}),  # LIKE edge is skipped in _adjacency
        ),
    )
    assert [s.vertex.id for s in steps] == ["b"]


@pytest.mark.asyncio
async def test_expand_skips_edges_to_missing_vertices(ctx: ExecutionContext) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    await cmd.create_vertex("User", UserCreate(id="a"))
    # Edge to a vertex that was never created — the traversal core must skip the dangling target.
    await cmd.create_edge("LINK", LinkCreate(from_key="a", to_key="ghost"))

    steps = await ctx.graph.query(spec).expand(
        _u("a"), GraphWalkParams(max_depth=2, max_results=10, direction=GraphDirection.OUT)
    )
    assert steps == []


@pytest.mark.asyncio
async def test_expand_edge_simple_over_a_cycle(ctx: ExecutionContext) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    for k in ("a", "b"):
        await cmd.create_vertex("User", UserCreate(id=k))
    await cmd.create_edge("LINK", LinkCreate(from_key="a", to_key="b"))
    await cmd.create_edge("LINK", LinkCreate(from_key="b", to_key="a"))  # cycle back

    steps = await ctx.graph.query(spec).expand(
        _u("a"), GraphWalkParams(max_depth=3, max_results=50, direction=GraphDirection.OUT)
    )
    # Edge-simple: a->b (d1), b->a (d2); a->b again reuses the first edge idx so it stops.
    assert sorted((s.depth, s.vertex.id) for s in steps) == [(1, "b"), (2, "a")]


@pytest.mark.asyncio
async def test_weighted_shortest_path_settles_diamond(ctx: ExecutionContext) -> None:
    # Two routes reach d; Dijkstra pops the ``d`` state once and skips the costlier re-entry.
    spec = _spec()
    cmd = ctx.graph.command(spec)
    for k in ("a", "b", "c", "d"):
        await cmd.create_vertex("User", UserCreate(id=k))
    await cmd.create_edge("LINK", LinkCreate(from_key="a", to_key="b", weight=1))
    await cmd.create_edge("LINK", LinkCreate(from_key="a", to_key="c", weight=1))
    await cmd.create_edge("LINK", LinkCreate(from_key="b", to_key="d", weight=1))
    await cmd.create_edge("LINK", LinkCreate(from_key="c", to_key="d", weight=5))

    path = await ctx.graph.query(spec).shortest_path(
        _u("a"), _u("d"), ShortestPathParams(max_hops=5, weight_property="weight")
    )
    assert path is not None
    assert [v.id for v in path.vertices] == ["a", "b", "d"]


@pytest.mark.asyncio
async def test_weighted_shortest_path_unreachable_is_none(ctx: ExecutionContext) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    for k in ("a", "b"):
        await cmd.create_vertex("User", UserCreate(id=k))
    # No edge a->b, so the weighted search exhausts its heap without reaching the target.
    path = await ctx.graph.query(spec).shortest_path(
        _u("a"), _u("b"), ShortestPathParams(max_hops=5, weight_property="weight")
    )
    assert path is None


@pytest.mark.asyncio
async def test_scoped_walk_zero_hop_segment_includes_anchor(ctx: ExecutionContext) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    for k in ("a", "b"):
        await cmd.create_vertex("User", UserCreate(id=k))
    await cmd.create_edge("LINK", LinkCreate(from_key="a", to_key="b"))

    # min_hops=0 makes the anchor itself a valid terminal of the segment.
    step = GraphPathStep(
        edge_kinds=frozenset({"LINK"}),
        direction=GraphDirection.OUT,
        min_hops=0,
        max_hops=1,
    )
    out = await ctx.graph.query(spec).scoped_walk(
        _u("a"), ScopedWalkParams(steps=(step,), target_kind="User")
    )
    assert sorted(v.id for v in out) == ["a", "b"]


# ----------------------- #
# bulk / deferred surface


@pytest.mark.asyncio
async def test_bulk_and_batch_empty_inputs(ctx: ExecutionContext) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    qry = ctx.graph.query(spec)

    assert await qry.get_vertices([]) == []
    assert await qry.get_edges([]) == []
    assert await cmd.create_vertices([]) == []
    assert await cmd.create_vertices([], return_new=False) is None
    assert await cmd.create_edges([]) == []
    assert await cmd.create_edges([], return_new=False) is None
    # Deletes on empty input are no-ops (early return).
    await cmd.delete_vertices([])
    await cmd.delete_edges([])


@pytest.mark.asyncio
async def test_bulk_create_return_new_false_returns_none(ctx: ExecutionContext) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)

    verts = await cmd.create_vertices(
        [("User", UserCreate(id="a")), ("User", UserCreate(id="b"))], return_new=False
    )
    assert verts is None

    edges = await cmd.create_edges(
        [("LINK", LinkCreate(from_key="a", to_key="b"))], return_new=False
    )
    assert edges is None


@pytest.mark.asyncio
async def test_delete_edges_removes_matching_record(ctx: ExecutionContext) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    qry = ctx.graph.query(spec)
    for k in ("a", "b"):
        await cmd.create_vertex("User", UserCreate(id=k))
    await cmd.create_edge("LINK", LinkCreate(from_key="a", to_key="b"))

    ref = EdgeRef.by_endpoints("LINK", _u("a"), _u("b"))
    assert await qry.edge_exists(ref) is True

    await cmd.delete_edges([ref])
    assert await qry.edge_exists(ref) is False


@pytest.mark.asyncio
async def test_incident_edges_skips_non_touching_and_honors_limit(
    ctx: ExecutionContext,
) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    for k in ("a", "b", "c"):
        await cmd.create_vertex("User", UserCreate(id=k))
    await cmd.create_edge("LINK", LinkCreate(from_key="a", to_key="b"))
    await cmd.create_edge("LINK", LinkCreate(from_key="a", to_key="c"))
    await cmd.create_edge("LINK", LinkCreate(from_key="b", to_key="c"))  # not incident to a (OUT)

    qry = ctx.graph.query(spec)

    # b->c does not touch a in the OUT direction, so it is skipped.
    out = await qry.incident_edges(
        _u("a"), GraphDirection.OUT, frozenset({"LINK"}), limit=10
    )
    assert len(out) == 2

    # limit stops the scan early once reached.
    capped = await qry.incident_edges(
        _u("a"), GraphDirection.OUT, frozenset({"LINK"}), limit=1
    )
    assert len(capped) == 1


@pytest.mark.asyncio
async def test_delete_edge_single_removes_record(ctx: ExecutionContext) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    for k in ("a", "b"):
        await cmd.create_vertex("User", UserCreate(id=k))
    await cmd.create_edge("LINK", LinkCreate(from_key="a", to_key="b"))

    ref = EdgeRef.by_endpoints("LINK", _u("a"), _u("b"))
    await cmd.delete_edge(ref)
    assert await ctx.graph.query(spec).edge_exists(ref) is False

    # Deleting an already-absent edge is a no-op (the record lookup misses).
    await cmd.delete_edge(ref)


@pytest.mark.asyncio
async def test_weighted_shortest_path_revisits_and_skips_dangling(
    ctx: ExecutionContext,
) -> None:
    # A stale, costlier ``(x, 2)`` heap entry is popped after the cheaper one settles ``x`` (the
    # settled-state guard), and ``b -> ghost`` exercises the dangling-target skip inside Dijkstra.
    spec = _spec()
    cmd = ctx.graph.command(spec)
    for k in ("a", "b", "c", "x", "d"):
        await cmd.create_vertex("User", UserCreate(id=k))
    # b->x (5) settles a costlier (x,2)@6 before c->x (1) pushes the cheaper (x,2)@2; the expensive
    # x->d (10) makes d cost 12 > 6, so the stale (x,2)@6 is popped (and skipped) before d.
    await cmd.create_edge("LINK", LinkCreate(from_key="a", to_key="b", weight=1))
    await cmd.create_edge("LINK", LinkCreate(from_key="a", to_key="c", weight=1))
    await cmd.create_edge("LINK", LinkCreate(from_key="b", to_key="x", weight=5))
    await cmd.create_edge("LINK", LinkCreate(from_key="c", to_key="x", weight=1))
    await cmd.create_edge("LINK", LinkCreate(from_key="x", to_key="d", weight=10))
    await cmd.create_edge("LINK", LinkCreate(from_key="b", to_key="ghost", weight=1))

    path = await ctx.graph.query(spec).shortest_path(
        _u("a"), _u("d"), ShortestPathParams(max_hops=4, weight_property="weight")
    )
    assert path is not None
    assert [v.id for v in path.vertices] == ["a", "c", "x", "d"]


@pytest.mark.asyncio
async def test_scoped_walk_segment_skips_dangling_target(ctx: ExecutionContext) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    for k in ("a", "b"):
        await cmd.create_vertex("User", UserCreate(id=k))
    await cmd.create_edge("LINK", LinkCreate(from_key="a", to_key="b"))
    await cmd.create_edge("LINK", LinkCreate(from_key="b", to_key="ghost"))  # dangling

    step = GraphPathStep(
        edge_kinds=frozenset({"LINK"}),
        direction=GraphDirection.OUT,
        min_hops=1,
        max_hops=2,
    )
    out = await ctx.graph.query(spec).scoped_walk(
        _u("a"), ScopedWalkParams(steps=(step,), target_kind="User")
    )
    # b is reachable; the second hop onto the missing ``ghost`` vertex is skipped.
    assert sorted(v.id for v in out) == ["b"]


@pytest.mark.asyncio
async def test_keyed_ensure_without_key_fails_closed(ctx: ExecutionContext) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    for k in ("a", "b"):
        await cmd.create_vertex("User", UserCreate(id=k))

    # A keyed ensure with the key omitted cannot form a stable identity.
    with pytest.raises(CoreException, match="graph_edge_key_required"):
        await cmd.ensure_edge("KEYED", KeyedCreate(from_key="a", to_key="b"))

"""Unit tests for the in-memory mock graph adapter + ctx.graph wiring."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.graph import (
    GraphCommandPort,
    GraphDirection,
    GraphEdgeDirectionality,
    GraphEdgeEndpoint,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
    GraphQueryPort,
    VertexRef,
)
from forze.application.execution import ExecutionContext
from tests.support.execution_context import context_from_deps

from forze_mock import MockDepsModule, MockState


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


def _spec() -> GraphModuleSpec:
    return GraphModuleSpec(
        name="social",
        nodes=(GraphNodeSpec(name="User", read=UserRead, create=UserCreate),),
        edges=(
            GraphEdgeSpec(
                name="FOLLOWS",
                read=FollowsRead,
                identity="endpoints",
                endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="User"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
        ),
    )


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule(state=MockState())())


def test_ctx_graph_resolves_ports(ctx: ExecutionContext) -> None:
    spec = _spec()
    assert isinstance(ctx.graph.query(spec), GraphQueryPort)
    assert isinstance(ctx.graph.command(spec), GraphCommandPort)


@pytest.mark.asyncio
async def test_vertex_crud(ctx: ExecutionContext) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    qry = ctx.graph.query(spec)

    created = await cmd.create_vertex("User", UserCreate(id="a", name="Alice"))
    assert isinstance(created, UserRead) and created.name == "Alice"

    assert await qry.vertex_exists(VertexRef(kind="User", key="a")) is True

    updated = await cmd.update_vertex(VertexRef(kind="User", key="a"), UserCreate(id="a", name="Al"))
    assert updated.name == "Al"

    await cmd.delete_vertex(VertexRef(kind="User", key="a"))
    assert await qry.get_vertex(VertexRef(kind="User", key="a")) is None


@pytest.mark.asyncio
async def test_edges_and_neighbors(ctx: ExecutionContext) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    qry = ctx.graph.query(spec)

    await cmd.create_vertex("User", UserCreate(id="a"))
    await cmd.create_vertex("User", UserCreate(id="b"))
    await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b", weight=3))

    out = await qry.neighbors(
        VertexRef(kind="User", key="a"), GraphDirection.OUT, frozenset({"FOLLOWS"}), limit=10
    )
    assert [n.other.id for n in out] == ["b"]
    assert out[0].via_edge.weight == 3

    # IN direction from b reaches a.
    back = await qry.neighbors(
        VertexRef(kind="User", key="b"), GraphDirection.IN, frozenset({"FOLLOWS"}), limit=10
    )
    assert [n.other.id for n in back] == ["a"]


@pytest.mark.asyncio
async def test_ensure_edge_idempotent(ctx: ExecutionContext) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    qry = ctx.graph.query(spec)

    await cmd.create_vertex("User", UserCreate(id="a"))
    await cmd.create_vertex("User", UserCreate(id="b"))
    await cmd.ensure_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b"))
    await cmd.ensure_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b"))

    out = await qry.neighbors(
        VertexRef(kind="User", key="a"), GraphDirection.OUT, frozenset({"FOLLOWS"}), limit=10
    )
    assert len(out) == 1


@pytest.mark.asyncio
async def test_expand_not_implemented(ctx: ExecutionContext) -> None:
    from forze.application.contracts.graph import GraphWalkParams

    qry = ctx.graph.query(_spec())
    with pytest.raises(NotImplementedError):
        await qry.expand(VertexRef(kind="User", key="a"), GraphWalkParams(max_depth=2, max_results=10))

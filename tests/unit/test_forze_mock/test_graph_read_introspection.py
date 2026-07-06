"""Unit tests for the mock graph read-introspection methods (WS2)."""

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
    VertexRef,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.unit


class UserRead(BaseModel):
    id: str
    name: str | None = None


class UserCreate(BaseModel):
    id: str
    name: str | None = None


class RatedRead(BaseModel):
    id: str
    score: int


class RatedCreate(BaseModel):
    id: str
    from_key: str
    to_key: str
    score: int


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


def _u(k: str) -> VertexRef:
    return VertexRef(kind="User", key=k)


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule(state=MockState())())


async def _seed(cmd: object) -> None:
    await cmd.create_vertex("User", UserCreate(id="a", name="Ana"))
    await cmd.create_vertex("User", UserCreate(id="b", name="Bo"))
    await cmd.create_vertex("User", UserCreate(id="c", name="Ana"))
    await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b"))
    await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="c"))
    await cmd.create_edge("RATED", RatedCreate(id="r1", from_key="a", to_key="b", score=5))


@pytest.mark.asyncio
async def test_get_vertices_found_only_in_order(ctx: ExecutionContext) -> None:
    spec = _spec()
    await _seed(ctx.graph.command(spec))
    q = ctx.graph.query(spec)

    out = await q.get_vertices([_u("c"), _u("missing"), _u("a")])
    assert [v.id for v in out] == ["c", "a"]


@pytest.mark.asyncio
async def test_get_edges_and_edge_exists(ctx: ExecutionContext) -> None:
    spec = _spec()
    await _seed(ctx.graph.command(spec))
    q = ctx.graph.query(spec)

    out = await q.get_edges(
        [EdgeRef.by_key("RATED", "r1"), EdgeRef.by_endpoints("FOLLOWS", _u("a"), _u("b"))]
    )
    assert len(out) == 2

    assert await q.edge_exists(EdgeRef.by_key("RATED", "r1")) is True
    assert await q.edge_exists(EdgeRef.by_endpoints("FOLLOWS", _u("b"), _u("a"))) is False


@pytest.mark.asyncio
async def test_counts_and_filter(ctx: ExecutionContext) -> None:
    spec = _spec()
    await _seed(ctx.graph.command(spec))
    q = ctx.graph.query(spec)

    assert await q.count_vertices("User") == 3
    assert await q.count_vertices("User", property_filter={"name": "Ana"}) == 2
    assert await q.count_edges("FOLLOWS") == 2
    assert await q.count_edges("RATED", property_filter={"score": 5}) == 1


@pytest.mark.asyncio
async def test_degree_neighbors_incident(ctx: ExecutionContext) -> None:
    spec = _spec()
    await _seed(ctx.graph.command(spec))
    q = ctx.graph.query(spec)

    assert await q.vertex_degree(_u("a"), direction=GraphDirection.OUT) == 3
    assert (
        await q.vertex_degree(
            _u("a"), direction=GraphDirection.OUT, edge_kinds=frozenset({"FOLLOWS"})
        )
        == 2
    )
    assert (
        await q.count_neighbors(
            _u("a"), direction=GraphDirection.OUT, edge_kinds=frozenset({"FOLLOWS"})
        )
        == 2
    )
    incident = await q.incident_edges(
        _u("a"), GraphDirection.OUT, frozenset({"FOLLOWS"}), limit=10
    )
    assert len(incident) == 2


@pytest.mark.asyncio
async def test_filter_on_encrypted_field_rejected() -> None:
    """A filter on a sealed property fails closed (can't match ciphertext)."""

    from forze.application.contracts.crypto import FieldEncryption

    class SecretRead(BaseModel):
        id: str
        ssn: str

    class SecretCreate(BaseModel):
        id: str
        ssn: str

    spec = GraphModuleSpec(
        name="sec",
        nodes=(
            GraphNodeSpec(
                name="Secret",
                read=SecretRead,
                create=SecretCreate,
                encryption=FieldEncryption(encrypted=frozenset({"ssn"})),
            ),
        ),
        edges=(),
    )
    ctx = context_from_deps(MockDepsModule(state=MockState())())
    q = ctx.graph.query(spec)

    with pytest.raises(CoreException) as ei:
        await q.count_vertices("Secret", property_filter={"ssn": "123"})
    assert ei.value.code == "graph_filter_on_encrypted_field"

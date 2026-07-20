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
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_deps


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
async def test_expand_on_missing_start_is_empty(ctx: ExecutionContext) -> None:
    from forze.application.contracts.graph import GraphWalkParams

    qry = ctx.graph.query(_spec())
    # expand is implemented now; a missing start returns [] rather than raising.
    assert (
        await qry.expand(
            VertexRef(kind="User", key="a"), GraphWalkParams(max_depth=2, max_results=10)
        )
        == []
    )


# ----------------------- #
# tenant isolation (parity with the real adapters)


def _tenant_adapter(state: MockState, tenant_provider: object) -> object:
    from forze_mock.adapters import MockGraphAdapter

    return MockGraphAdapter(
        spec=_spec(),
        state=state,
        namespace="social",
        tenant_aware=True,
        tenant_provider=tenant_provider,  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_mock_graph_partitions_store_by_tenant() -> None:
    from uuid import uuid4

    from forze.application.contracts.tenancy import TenantIdentity

    t1, t2 = uuid4(), uuid4()
    current: dict[str, TenantIdentity] = {"id": TenantIdentity(tenant_id=t1)}
    adapter = _tenant_adapter(MockState(), lambda: current["id"])

    await adapter.create_vertex("User", UserCreate(id="shared", name="T1"))
    await adapter.create_vertex("User", UserCreate(id="b"))
    await adapter.ensure_edge("FOLLOWS", FollowsCreate(from_key="shared", to_key="b"))

    # Tenant 2 shares the key namespace but must not see tenant 1's data.
    current["id"] = TenantIdentity(tenant_id=t2)
    assert await adapter.get_vertex(VertexRef(kind="User", key="shared")) is None
    assert (
        await adapter.neighbors(
            VertexRef(kind="User", key="shared"),
            GraphDirection.OUT,
            frozenset({"FOLLOWS"}),
            limit=10,
        )
        == []
    )

    # Back to tenant 1 — visible again.
    current["id"] = TenantIdentity(tenant_id=t1)
    got = await adapter.get_vertex(VertexRef(kind="User", key="shared"))
    assert got is not None and got.name == "T1"


@pytest.mark.asyncio
async def test_mock_graph_fails_closed_without_tenant() -> None:
    from forze.base.exceptions import CoreException

    adapter = _tenant_adapter(MockState(), lambda: None)

    with pytest.raises(CoreException, match="tenant_required"):
        await adapter.create_vertex("User", UserCreate(id="x"))

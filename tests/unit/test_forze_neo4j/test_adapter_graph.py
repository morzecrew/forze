"""Unit tests for ``Neo4jGraphAdapter`` with a mocked client."""

from typing import Any
from uuid import uuid4

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
    GraphRawQueryPort,
    VertexRef,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, ExceptionKind
from forze_neo4j.adapters import Neo4jGraphAdapter

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


class _FakeClient:
    """Records the last run() call and returns canned rows."""

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows if rows is not None else []
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def run(self, query, params=None, *, database=None):  # noqa: ANN001, ANN202
        self.calls.append((query, dict(params or {})))
        return self.rows

    async def close(self) -> None: ...
    async def health(self) -> tuple[str, bool]:
        return "neo4j", True

    def is_in_transaction(self) -> bool:
        return False

    def transaction(self, *, database=None):  # noqa: ANN001, ANN202
        raise NotImplementedError


def _adapter(rows=None, **kw) -> tuple[Neo4jGraphAdapter, _FakeClient]:
    client = _FakeClient(rows)
    return Neo4jGraphAdapter(spec=_spec(), client=client, **kw), client


# ----------------------- #


def test_adapter_satisfies_ports() -> None:
    adapter, _ = _adapter()
    assert isinstance(adapter, GraphQueryPort)
    assert isinstance(adapter, GraphCommandPort)
    assert isinstance(adapter, GraphRawQueryPort)


@pytest.mark.asyncio
async def test_get_vertex_materializes() -> None:
    adapter, client = _adapter(rows=[{"n": {"id": "a", "name": "Alice"}}])
    out = await adapter.get_vertex(VertexRef(kind="User", key="a"))
    assert isinstance(out, UserRead)
    assert out.id == "a" and out.name == "Alice"
    query, params = client.calls[-1]
    assert "MATCH (n:`User` {id: $key})" in query
    assert params == {"key": "a"}


@pytest.mark.asyncio
async def test_get_vertex_none_when_missing() -> None:
    adapter, _ = _adapter(rows=[])
    assert await adapter.get_vertex(VertexRef(kind="User", key="x")) is None


@pytest.mark.asyncio
async def test_create_vertex_returns_model() -> None:
    adapter, client = _adapter(rows=[{"n": {"id": "a", "name": "Alice"}}])
    out = await adapter.create_vertex("User", UserCreate(id="a", name="Alice"))
    assert isinstance(out, UserRead)
    _, params = client.calls[-1]
    assert params["props"]["id"] == "a"


@pytest.mark.asyncio
async def test_create_edge_endpoints_extracts_keys() -> None:
    adapter, client = _adapter(rows=[{"r": {"weight": 5}}])
    out = await adapter.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b", weight=5))
    assert isinstance(out, FollowsRead)
    assert out.weight == 5
    query, params = client.calls[-1]
    assert "CREATE (a)-[r:`FOLLOWS`]->(b)" in query
    assert params["from_key"] == "a" and params["to_key"] == "b"
    assert "from_key" not in params["props"] and "to_key" not in params["props"]


@pytest.mark.asyncio
async def test_ensure_edge_uses_merge() -> None:
    adapter, client = _adapter(rows=[{"r": {}}])
    await adapter.ensure_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b"))
    query, _ = client.calls[-1]
    assert "MERGE (a)-[r:`FOLLOWS`]->(b)" in query


@pytest.mark.asyncio
async def test_create_edge_requires_endpoint_keys() -> None:
    adapter, _ = _adapter(rows=[{"r": {}}])

    class BadCmd(BaseModel):
        weight: int = 1

    with pytest.raises(CoreException, match="from_key"):
        await adapter.create_edge("FOLLOWS", BadCmd())


@pytest.mark.asyncio
async def test_neighbors_maps_rows() -> None:
    rows = [
        {
            "other": {"id": "b", "name": "Bob"},
            "other_labels": ["User"],
            "via_edge": {"weight": 1},
            "via_type": "FOLLOWS",
        }
    ]
    adapter, _ = _adapter(rows=rows)
    out = await adapter.neighbors(
        VertexRef(kind="User", key="a"), GraphDirection.OUT, frozenset({"FOLLOWS"}), limit=10
    )
    assert len(out) == 1
    assert isinstance(out[0].other, UserRead) and out[0].other.id == "b"
    assert isinstance(out[0].via_edge, FollowsRead)
    assert out[0].direction is GraphDirection.OUT


@pytest.mark.asyncio
async def test_update_vertex_not_found() -> None:
    adapter, _ = _adapter(rows=[])
    with pytest.raises(CoreException, match="not found"):
        await adapter.update_vertex(VertexRef(kind="User", key="ghost"), UserCreate(id="ghost"))


@pytest.mark.asyncio
async def test_tenant_aware_stamps_and_filters() -> None:
    tid = uuid4()
    adapter, client = _adapter(
        rows=[{"n": {"id": "a"}}],
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )
    await adapter.get_vertex(VertexRef(kind="User", key="a"))
    query, params = client.calls[-1]
    assert "tenant_id: $tenant" in query
    assert params["tenant"] == str(tid)

    await adapter.create_vertex("User", UserCreate(id="a"))
    _, params = client.calls[-1]
    assert params["props"]["tenant_id"] == str(tid)


@pytest.mark.asyncio
async def test_full_path_isolation_constrains_traversal_interior() -> None:
    # Default traversal_isolation="full-path": neighbors terminal node and expand path
    # nodes are tenant-constrained, not just the anchor.
    tid = uuid4()
    adapter, client = _adapter(
        rows=[],
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )

    await adapter.neighbors(
        VertexRef(kind="User", key="a"), GraphDirection.OUT, frozenset({"FOLLOWS"}), limit=10
    )
    query, _ = client.calls[-1]
    assert "(m {tenant_id: $tenant})" in query


@pytest.mark.asyncio
async def test_anchor_isolation_leaves_traversal_interior_open() -> None:
    tid = uuid4()
    adapter, client = _adapter(
        rows=[],
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
        traversal_isolation="anchor",
    )

    await adapter.neighbors(
        VertexRef(kind="User", key="a"), GraphDirection.OUT, frozenset({"FOLLOWS"}), limit=10
    )
    query, _ = client.calls[-1]
    assert "(m {tenant_id: $tenant})" not in query
    assert "{id: $key, tenant_id: $tenant}" in query  # anchor still scoped


@pytest.mark.asyncio
async def test_tenant_required_when_aware_without_provider_value() -> None:
    adapter, _ = _adapter(
        rows=[], tenant_aware=True, tenant_provider=lambda: None
    )
    with pytest.raises(CoreException):
        await adapter.get_vertex(VertexRef(kind="User", key="a"))


@pytest.mark.asyncio
async def test_raw_query_binds_tenant_when_aware() -> None:
    tid = uuid4()
    adapter, client = _adapter(
        rows=[], tenant_aware=True, tenant_provider=lambda: TenantIdentity(tenant_id=tid)
    )

    await adapter.run("MATCH (n {tenant_id: $tenant}) RETURN n", {"x": 1})

    _, params = client.calls[-1]
    assert params == {"x": 1, "tenant": str(tid)}


@pytest.mark.asyncio
async def test_raw_query_fails_closed_without_tenant() -> None:
    # A tenant-aware raw query with no bound tenant raises instead of running unscoped.
    adapter, client = _adapter(rows=[], tenant_aware=True, tenant_provider=lambda: None)

    with pytest.raises(CoreException):
        await adapter.run("MATCH (n) RETURN n")

    assert client.calls == []  # never reached the client


@pytest.mark.asyncio
async def test_raw_query_passthrough_when_not_tenant_aware() -> None:
    adapter, client = _adapter(rows=[])

    await adapter.run("MATCH (n) RETURN n", {"x": 1})

    _, params = client.calls[-1]
    assert params == {"x": 1}  # unchanged — no tenant injected


@pytest.mark.asyncio
async def test_raw_query_disabled_fails_closed() -> None:
    # allow_raw_query=False refuses the whole-query hatch before touching the client.
    adapter, client = _adapter(rows=[], allow_raw_query=False)

    with pytest.raises(CoreException, match="graph_raw_disabled"):
        await adapter.run("MATCH (n) RETURN n")

    assert client.calls == []


@pytest.mark.asyncio
async def test_scoped_walk_builds_tenant_scoped_query_and_materializes() -> None:
    from forze.application.contracts.graph import GraphPathStep, ScopedWalkParams
    from forze.application.contracts.tenancy import TenantIdentity

    tid = uuid4()
    adapter, client = _adapter(
        rows=[{"m": {"id": "b"}}, {"m": {"id": "c"}}],
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )

    out = await adapter.scoped_walk(
        VertexRef(kind="User", key="a"),
        ScopedWalkParams(
            steps=[GraphPathStep(edge_kinds=frozenset({"FOLLOWS"}), max_hops=3)],
            target_kind="User",
            limit=10,
        ),
    )

    query, params = client.calls[-1]
    # anchor + target + full-path tenant constraint, all adapter-owned
    assert "(n0:`User` {id: $key, tenant_id: $tenant})" in query
    assert "(m:`User` {tenant_id: $tenant})" in query
    assert "WHERE all(_n IN nodes(path) WHERE _n.`tenant_id` = $tenant)" in query
    assert params == {"key": "a", "limit": 10, "tenant": str(tid)}
    assert [u.id for u in out] == ["b", "c"]


@pytest.mark.asyncio
async def test_scoped_walk_fails_closed_without_tenant() -> None:
    from forze.application.contracts.graph import GraphPathStep, ScopedWalkParams

    adapter, client = _adapter(
        rows=[], tenant_aware=True, tenant_provider=lambda: None
    )

    with pytest.raises(CoreException, match="tenant_required"):
        await adapter.scoped_walk(
            VertexRef(kind="User", key="a"),
            ScopedWalkParams(
                steps=[GraphPathStep(edge_kinds=frozenset({"FOLLOWS"}))],
                target_kind="User",
            ),
        )

    assert client.calls == []


@pytest.mark.asyncio
async def test_deferred_method_raises() -> None:
    adapter, _ = _adapter()
    with pytest.raises(CoreException, match="not implemented by forze_neo4j yet") as ei:
        await adapter.count_vertices("User")

    assert ei.value.kind is ExceptionKind.INTERNAL
    assert ei.value.code == "graph_not_implemented"

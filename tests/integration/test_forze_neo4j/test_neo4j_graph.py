"""Integration tests validating the Neo4j graph adapter against a live engine.

These exercise the load-bearing contract assumptions: both edge-identity modes
(endpoints + business key), traversal (neighbors/expand/shortest_path), and
tenant-property isolation.
"""

from uuid import uuid4

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
    ShortestPathParams,
    VertexRef,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze_neo4j.adapters import Neo4jGraphAdapter
from forze_neo4j.kernel.client import Neo4jClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


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
    id: str
    score: int


class RatedCreate(BaseModel):
    id: str
    from_key: str
    to_key: str
    score: int


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


def _adapter(client: Neo4jClient, **kw) -> Neo4jGraphAdapter:
    return Neo4jGraphAdapter(spec=_spec(), client=client, **kw)


# ----------------------- #


async def test_vertex_crud(neo4j_client: Neo4jClient) -> None:
    a = _adapter(neo4j_client)

    created = await a.create_vertex("User", UserCreate(id="u1", name="Alice"))
    assert isinstance(created, UserRead) and created.name == "Alice"

    assert await a.vertex_exists(VertexRef(kind="User", key="u1")) is True
    got = await a.get_vertex(VertexRef(kind="User", key="u1"))
    assert got is not None and got.id == "u1"

    updated = await a.update_vertex(VertexRef(kind="User", key="u1"), UserCreate(id="u1", name="Al"))
    assert updated.name == "Al"

    await a.delete_vertex(VertexRef(kind="User", key="u1"))
    assert await a.get_vertex(VertexRef(kind="User", key="u1")) is None


async def test_endpoints_edge_and_neighbors(neo4j_client: Neo4jClient) -> None:
    a = _adapter(neo4j_client)
    await a.create_vertex("User", UserCreate(id="a"))
    await a.create_vertex("User", UserCreate(id="b"))

    await a.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b", weight=7))

    out = await a.neighbors(
        VertexRef(kind="User", key="a"), GraphDirection.OUT, frozenset({"FOLLOWS"}), limit=10
    )
    assert [n.other.id for n in out] == ["b"]
    assert out[0].via_edge.weight == 7

    # ensure_edge is idempotent on endpoints.
    await a.ensure_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b"))
    out2 = await a.neighbors(
        VertexRef(kind="User", key="a"), GraphDirection.OUT, frozenset({"FOLLOWS"}), limit=10
    )
    assert len(out2) == 1


async def test_keyed_edge_get_by_key(neo4j_client: Neo4jClient) -> None:
    a = _adapter(neo4j_client)
    await a.create_vertex("User", UserCreate(id="a"))
    await a.create_vertex("User", UserCreate(id="b"))

    await a.create_edge("RATED", RatedCreate(id="r1", from_key="a", to_key="b", score=5))

    got = await a.get_edge(EdgeRef.by_key("RATED", "r1"))
    assert isinstance(got, RatedRead)
    assert got.id == "r1" and got.score == 5


async def test_expand_and_shortest_path(neo4j_client: Neo4jClient) -> None:
    a = _adapter(neo4j_client)
    for key in ("a", "b", "c"):
        await a.create_vertex("User", UserCreate(id=key))
    await a.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b"))
    await a.create_edge("FOLLOWS", FollowsCreate(from_key="b", to_key="c"))

    steps = await a.expand(
        VertexRef(kind="User", key="a"),
        GraphWalkParams(max_depth=2, max_results=50, direction=GraphDirection.OUT,
                        edge_kinds=frozenset({"FOLLOWS"})),
    )
    reached = {s.vertex.id for s in steps}
    assert {"b", "c"} <= reached

    path = await a.shortest_path(
        VertexRef(kind="User", key="a"),
        VertexRef(kind="User", key="c"),
        ShortestPathParams(max_hops=5, edge_kinds=frozenset({"FOLLOWS"})),
    )
    assert path is not None
    assert [v.id for v in path.vertices] == ["a", "b", "c"]
    assert len(path.edges) == 2


async def test_tenant_property_isolation(neo4j_client: Neo4jClient) -> None:
    t1, t2 = uuid4(), uuid4()
    current: dict[str, TenantIdentity] = {"id": TenantIdentity(tenant_id=t1)}

    a = _adapter(
        neo4j_client,
        tenant_aware=True,
        tenant_provider=lambda: current["id"],
    )

    await a.create_vertex("User", UserCreate(id="shared", name="T1"))

    # Same key, different tenant — must not be visible.
    current["id"] = TenantIdentity(tenant_id=t2)
    assert await a.get_vertex(VertexRef(kind="User", key="shared")) is None

    # Back to tenant 1 — visible again.
    current["id"] = TenantIdentity(tenant_id=t1)
    got = await a.get_vertex(VertexRef(kind="User", key="shared"))
    assert got is not None and got.name == "T1"


async def test_raw_escape_hatch(neo4j_client: Neo4jClient) -> None:
    a = _adapter(neo4j_client)
    await a.create_vertex("User", UserCreate(id="a"))
    await a.create_vertex("User", UserCreate(id="b"))

    rows = await a.run("MATCH (n:User) RETURN count(n) AS total")
    assert rows[0]["total"] == 2


async def test_raw_query_is_tenant_scoped(neo4j_client: Neo4jClient) -> None:
    t1, t2 = uuid4(), uuid4()
    current: dict[str, TenantIdentity | None] = {"id": TenantIdentity(tenant_id=t1)}
    a = _adapter(
        neo4j_client, tenant_aware=True, tenant_provider=lambda: current["id"]
    )

    await a.create_vertex("User", UserCreate(id="t1u"))
    current["id"] = TenantIdentity(tenant_id=t2)
    await a.create_vertex("User", UserCreate(id="t2u"))

    # A raw query scoped on $tenant sees only the current tenant's nodes.
    rows = await a.run("MATCH (n:User {tenant_id: $tenant}) RETURN n.id AS id")
    assert {r["id"] for r in rows} == {"t2u"}

    # Fail-closed: no bound tenant raises rather than running unscoped across tenants.
    current["id"] = None
    with pytest.raises(CoreException):
        await a.run("MATCH (n:User) RETURN n")

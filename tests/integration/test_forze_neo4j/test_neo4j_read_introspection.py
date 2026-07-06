"""Integration coverage for WS2 read-introspection methods on Neo4j."""

from __future__ import annotations

from uuid import UUID

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
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze_neo4j.adapters import Neo4jGraphAdapter
from forze_neo4j.kernel.client import Neo4jClient

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
        name="ri",
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


async def _seed(adapter: Neo4jGraphAdapter) -> None:
    await adapter.create_vertex("User", UserCreate(id="a", name="Ana"))
    await adapter.create_vertex("User", UserCreate(id="b", name="Bo"))
    await adapter.create_vertex("User", UserCreate(id="c", name="Ana"))
    await adapter.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b"))
    await adapter.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="c"))
    await adapter.create_edge("FOLLOWS", FollowsCreate(from_key="b", to_key="c"))
    await adapter.create_edge("RATED", RatedCreate(id="r1", from_key="a", to_key="b", score=5))


def _u(key: str) -> VertexRef:
    return VertexRef(kind="User", key=key)


async def test_get_vertices_found_only_in_input_order(neo4j_client: Neo4jClient) -> None:
    adapter = _adapter(neo4j_client)
    await _seed(adapter)

    out = await adapter.get_vertices([_u("c"), _u("missing"), _u("a")])
    assert [v.id for v in out] == ["c", "a"]  # input order, missing omitted


async def test_get_edges_keyed_and_endpoints(neo4j_client: Neo4jClient) -> None:
    adapter = _adapter(neo4j_client)
    await _seed(adapter)

    keyed = EdgeRef.by_key("RATED", "r1")
    endpoints = EdgeRef.by_endpoints("FOLLOWS", _u("a"), _u("b"))

    out = await adapter.get_edges([keyed, endpoints])
    assert len(out) == 2
    assert any(getattr(e, "score", None) == 5 for e in out)


async def test_edge_exists_keyed_and_endpoints(neo4j_client: Neo4jClient) -> None:
    adapter = _adapter(neo4j_client)
    await _seed(adapter)

    assert await adapter.edge_exists(EdgeRef.by_key("RATED", "r1")) is True
    assert await adapter.edge_exists(EdgeRef.by_key("RATED", "nope")) is False
    assert await adapter.edge_exists(EdgeRef.by_endpoints("FOLLOWS", _u("a"), _u("b"))) is True
    assert await adapter.edge_exists(EdgeRef.by_endpoints("FOLLOWS", _u("b"), _u("a"))) is False


async def test_count_vertices_with_and_without_filter(neo4j_client: Neo4jClient) -> None:
    adapter = _adapter(neo4j_client)
    await _seed(adapter)

    assert await adapter.count_vertices("User") == 3
    assert await adapter.count_vertices("User", property_filter={"name": "Ana"}) == 2


async def test_count_edges(neo4j_client: Neo4jClient) -> None:
    adapter = _adapter(neo4j_client)
    await _seed(adapter)

    assert await adapter.count_edges("FOLLOWS") == 3
    assert await adapter.count_edges("RATED") == 1
    assert await adapter.count_edges("RATED", property_filter={"score": 5}) == 1


async def test_degree_neighbors_incident(neo4j_client: Neo4jClient) -> None:
    adapter = _adapter(neo4j_client)
    await _seed(adapter)

    assert await adapter.vertex_degree(_u("a"), direction=GraphDirection.OUT) == 3  # 2 FOLLOWS + 1 RATED
    assert await adapter.vertex_degree(
        _u("a"), direction=GraphDirection.OUT, edge_kinds=frozenset({"FOLLOWS"})
    ) == 2
    assert await adapter.count_neighbors(
        _u("a"), direction=GraphDirection.OUT, edge_kinds=frozenset({"FOLLOWS"})
    ) == 2

    incident = await adapter.incident_edges(
        _u("a"), GraphDirection.OUT, frozenset({"FOLLOWS"}), limit=10
    )
    assert len(incident) == 2


async def test_read_introspection_is_tenant_scoped(neo4j_client: Neo4jClient) -> None:
    ta = _adapter(
        neo4j_client, tenant_aware=True, tenant_provider=lambda: TenantIdentity(tenant_id=UUID(int=1))
    )
    tb = _adapter(
        neo4j_client, tenant_aware=True, tenant_provider=lambda: TenantIdentity(tenant_id=UUID(int=2))
    )

    await ta.create_vertex("User", UserCreate(id="x", name="mine"))
    await tb.create_vertex("User", UserCreate(id="y", name="theirs"))

    assert await ta.count_vertices("User") == 1  # only tenant A's node
    assert [v.id for v in await ta.get_vertices([_u("x"), _u("y")])] == ["x"]  # y not visible

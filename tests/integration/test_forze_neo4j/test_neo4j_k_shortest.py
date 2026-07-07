"""Integration coverage for k_shortest_paths (native Neo4j 5 ``SHORTEST k``)."""

from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.graph import (
    GraphEdgeDirectionality,
    GraphEdgeEndpoint,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
    ShortestPathParams,
    VertexRef,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze_neo4j.adapters import Neo4jGraphAdapter
from forze_neo4j.kernel.client import Neo4jClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class UserRead(BaseModel):
    id: str


class UserCreate(BaseModel):
    id: str


class FollowsRead(BaseModel):
    weight: int | None = None


class FollowsCreate(BaseModel):
    from_key: str
    to_key: str


def _spec() -> GraphModuleSpec:
    return GraphModuleSpec(
        name="ksp",
        nodes=(GraphNodeSpec(name="KUser", read=UserRead, create=UserCreate),),
        edges=(
            GraphEdgeSpec(
                name="KFOLLOWS",
                read=FollowsRead,
                identity="endpoints",
                endpoints=(GraphEdgeEndpoint(from_kind="KUser", to_kind="KUser"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
        ),
    )


def _adapter(client: Neo4jClient, **kw) -> Neo4jGraphAdapter:
    return Neo4jGraphAdapter(spec=_spec(), client=client, **kw)


async def _diamond(adapter: Neo4jGraphAdapter) -> None:
    # a→d (1 hop); a→b→d and a→c→d (2 hops).
    for key in ("a", "b", "c", "d"):
        await adapter.create_vertex("KUser", UserCreate(id=key))
    for src, dst in (("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"), ("a", "d")):
        await adapter.create_edge("KFOLLOWS", FollowsCreate(from_key=src, to_key=dst))


async def test_k_shortest_returns_paths_in_increasing_length(
    neo4j_client: Neo4jClient,
) -> None:
    adapter = _adapter(neo4j_client)
    await _diamond(adapter)

    paths = await adapter.k_shortest_paths(
        VertexRef(kind="KUser", key="a"),
        VertexRef(kind="KUser", key="d"),
        ShortestPathParams(max_hops=5),
        k=3,
    )

    assert len(paths) == 3
    lengths = [len(p.edges) for p in paths]
    assert lengths == sorted(lengths)  # increasing length
    assert lengths[0] == 1  # the direct a→d edge is shortest
    # every path starts at a and ends at d
    for p in paths:
        assert p.vertices[0].id == "a"
        assert p.vertices[-1].id == "d"


async def test_k_capped_by_availability_and_k_value(neo4j_client: Neo4jClient) -> None:
    adapter = _adapter(neo4j_client)
    await _diamond(adapter)

    one = await adapter.k_shortest_paths(
        VertexRef(kind="KUser", key="a"),
        VertexRef(kind="KUser", key="d"),
        ShortestPathParams(max_hops=5),
        k=1,
    )
    assert len(one) == 1 and len(one[0].edges) == 1

    assert (
        await adapter.k_shortest_paths(
            VertexRef(kind="KUser", key="a"),
            VertexRef(kind="KUser", key="d"),
            ShortestPathParams(max_hops=5),
            k=0,
        )
        == []
    )


async def test_k_shortest_no_path_returns_empty(neo4j_client: Neo4jClient) -> None:
    adapter = _adapter(neo4j_client)
    for key in ("a", "z"):
        await adapter.create_vertex("KUser", UserCreate(id=key))

    paths = await adapter.k_shortest_paths(
        VertexRef(kind="KUser", key="a"),
        VertexRef(kind="KUser", key="z"),
        ShortestPathParams(max_hops=5),
        k=3,
    )
    assert paths == []


async def test_k_shortest_is_tenant_scoped(neo4j_client: Neo4jClient) -> None:
    """A cross-tenant path is not returned under full-path tenant isolation."""

    tenant_a = TenantIdentity(tenant_id=UUID(int=1))
    tenant_b = TenantIdentity(tenant_id=UUID(int=2))

    a_adapter = _adapter(neo4j_client, tenant_aware=True, tenant_provider=lambda: tenant_a)
    b_adapter = _adapter(neo4j_client, tenant_aware=True, tenant_provider=lambda: tenant_b)

    # Tenant A: a→b. Tenant B: b→d (b exists in both tenants as distinct nodes).
    await a_adapter.create_vertex("KUser", UserCreate(id="a"))
    await a_adapter.create_vertex("KUser", UserCreate(id="b"))
    await a_adapter.create_edge("KFOLLOWS", FollowsCreate(from_key="a", to_key="b"))

    await b_adapter.create_vertex("KUser", UserCreate(id="b"))
    await b_adapter.create_vertex("KUser", UserCreate(id="d"))
    await b_adapter.create_edge("KFOLLOWS", FollowsCreate(from_key="b", to_key="d"))

    # No same-tenant a→d path exists for tenant A.
    paths = await a_adapter.k_shortest_paths(
        VertexRef(kind="KUser", key="a"),
        VertexRef(kind="KUser", key="d"),
        ShortestPathParams(max_hops=5),
        k=3,
    )
    assert paths == []

"""Integration coverage for weighted shortest / k-shortest paths (Neo4j GDS)."""

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
from forze.base.exceptions import CoreException
from forze_neo4j.adapters import Neo4jGraphAdapter
from forze_neo4j.kernel.client import Neo4jClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class WUserRead(BaseModel):
    id: str


class WUserCreate(BaseModel):
    id: str


class WEdgeRead(BaseModel):
    w: float


class WEdgeCreate(BaseModel):
    from_key: str
    to_key: str
    w: float


def _spec() -> GraphModuleSpec:
    return GraphModuleSpec(
        name="wpaths",
        nodes=(GraphNodeSpec(name="WUser", read=WUserRead, create=WUserCreate),),
        edges=(
            GraphEdgeSpec(
                name="WLINK",
                read=WEdgeRead,
                identity="endpoints",
                endpoints=(GraphEdgeEndpoint(from_kind="WUser", to_kind="WUser"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
        ),
    )


def _adapter(client: Neo4jClient, **kw) -> Neo4jGraphAdapter:
    return Neo4jGraphAdapter(spec=_spec(), client=client, **kw)


async def _triangle(adapter: Neo4jGraphAdapter, *, direct_w: float) -> None:
    # direct a→b at weight ``direct_w``; detour a→c→b at total weight 0.2 (0.1 + 0.1).
    for key in ("a", "b", "c"):
        await adapter.create_vertex("WUser", WUserCreate(id=key))
    await adapter.create_edge("WLINK", WEdgeCreate(from_key="a", to_key="b", w=direct_w))
    await adapter.create_edge("WLINK", WEdgeCreate(from_key="a", to_key="c", w=0.1))
    await adapter.create_edge("WLINK", WEdgeCreate(from_key="c", to_key="b", w=0.1))


# --- capability gate (no GDS needed — fails before any GDS call) ---


async def test_weighted_requires_graph_algorithms_flag(
    neo4j_client: Neo4jClient,
) -> None:
    """A weighted request on a module without graph_algorithms fails closed."""

    adapter = _adapter(neo4j_client)  # graph_algorithms=False by default
    await adapter.create_vertex("WUser", WUserCreate(id="a"))
    await adapter.create_vertex("WUser", WUserCreate(id="b"))

    with pytest.raises(CoreException) as ei:
        await adapter.shortest_path(
            VertexRef(kind="WUser", key="a"),
            VertexRef(kind="WUser", key="b"),
            ShortestPathParams(max_hops=5, weight_property="w"),
        )
    assert ei.value.code == "graph_algorithm_unavailable"


# --- weighted behavior (requires the GDS container) ---


async def test_weighted_shortest_prefers_low_cost_route(
    gds_neo4j_client: Neo4jClient,
) -> None:
    """The weighted shortest path takes the cheaper 2-hop detour over the costly direct edge."""

    adapter = _adapter(gds_neo4j_client, graph_algorithms=True)
    await _triangle(adapter, direct_w=1.0)

    path = await adapter.shortest_path(
        VertexRef(kind="WUser", key="a"),
        VertexRef(kind="WUser", key="b"),
        ShortestPathParams(max_hops=5, weight_property="w"),
    )

    assert path is not None
    assert [v.id for v in path.vertices] == ["a", "c", "b"]  # low-cost detour, not direct
    assert len(path.edges) == 2


async def test_weighted_k_shortest_in_increasing_cost(
    gds_neo4j_client: Neo4jClient,
) -> None:
    adapter = _adapter(gds_neo4j_client, graph_algorithms=True)
    await _triangle(adapter, direct_w=1.0)

    paths = await adapter.k_shortest_paths(
        VertexRef(kind="WUser", key="a"),
        VertexRef(kind="WUser", key="b"),
        ShortestPathParams(max_hops=5, weight_property="w"),
        k=2,
    )

    assert len(paths) == 2
    assert [v.id for v in paths[0].vertices] == ["a", "c", "b"]  # cost 0.2 first
    assert [v.id for v in paths[1].vertices] == ["a", "b"]  # cost 1.0 second


async def test_weighted_max_hops_post_filter(gds_neo4j_client: Neo4jClient) -> None:
    """max_hops drops a cheaper-but-longer weighted path (applied after cost selection)."""

    adapter = _adapter(gds_neo4j_client, graph_algorithms=True)
    # direct a→b is expensive (5.0) but 1 hop; detour is cheap (0.2) but 2 hops.
    await _triangle(adapter, direct_w=5.0)

    paths = await adapter.k_shortest_paths(
        VertexRef(kind="WUser", key="a"),
        VertexRef(kind="WUser", key="b"),
        ShortestPathParams(max_hops=1, weight_property="w"),  # only 1-hop paths survive
        k=3,
    )

    assert len(paths) == 1
    assert [v.id for v in paths[0].vertices] == ["a", "b"]


async def test_weighted_is_tenant_scoped(gds_neo4j_client: Neo4jClient) -> None:
    """A cross-tenant weighted route is excluded — the projection is tenant-filtered."""

    tenant_a = TenantIdentity(tenant_id=UUID(int=1))
    tenant_b = TenantIdentity(tenant_id=UUID(int=2))
    a_adapter = _adapter(
        gds_neo4j_client, graph_algorithms=True, tenant_aware=True,
        tenant_provider=lambda: tenant_a,
    )
    b_adapter = _adapter(
        gds_neo4j_client, graph_algorithms=True, tenant_aware=True,
        tenant_provider=lambda: tenant_b,
    )

    # Tenant A: a→c only. Tenant B: c→b only. No same-tenant a→b route exists.
    await a_adapter.create_vertex("WUser", WUserCreate(id="a"))
    await a_adapter.create_vertex("WUser", WUserCreate(id="c"))
    await a_adapter.create_edge("WLINK", WEdgeCreate(from_key="a", to_key="c", w=0.1))

    await b_adapter.create_vertex("WUser", WUserCreate(id="c"))
    await b_adapter.create_vertex("WUser", WUserCreate(id="b"))
    await b_adapter.create_edge("WLINK", WEdgeCreate(from_key="c", to_key="b", w=0.1))

    path = await a_adapter.shortest_path(
        VertexRef(kind="WUser", key="a"),
        VertexRef(kind="WUser", key="b"),
        ShortestPathParams(max_hops=5, weight_property="w"),
    )
    assert path is None

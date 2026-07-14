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
    id: str
    w: float


class WEdgeCreate(BaseModel):
    id: str
    from_key: str
    to_key: str
    w: float


def _spec() -> GraphModuleSpec:
    return GraphModuleSpec(
        name="wpaths",
        nodes=(GraphNodeSpec(name="WUser", read=WUserRead, create=WUserCreate),),
        edges=(
            # **Keyed**, not endpoint-identified — and the distinction is the point of this
            # file. These tests lay *parallel* edges between the same pair (different weights),
            # which is a perfectly good graph model: two flights between two cities, two roads
            # between two towns. But such an edge cannot be addressed *by its endpoints*, so it
            # is not an ``identity="endpoints"`` kind — that declaration means "at most one edge
            # of this kind per (from, to) pair", and an edge kind that breaks it has no identity
            # at all: ``get_edge`` would return an arbitrary one of the parallel edges, and
            # ``update_edge`` / ``delete_edge`` would hit every one of them. An edge that is a
            # distinct entity needs a key of its own to be one.
            GraphEdgeSpec(
                name="WLINK",
                read=WEdgeRead,
                identity="key",
                key_field="id",
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
    await adapter.create_edge("WLINK", WEdgeCreate(id="ab", from_key="a", to_key="b", w=direct_w))
    await adapter.create_edge("WLINK", WEdgeCreate(id="ac", from_key="a", to_key="c", w=0.1))
    await adapter.create_edge("WLINK", WEdgeCreate(id="cb", from_key="c", to_key="b", w=0.1))


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


async def test_weighted_max_hops_bounds_search(gds_neo4j_client: Neo4jClient) -> None:
    """max_hops bounds the search: the cheapest path *within* the bound is returned, not the
    global cheapest with an over-long result silently dropped."""

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


async def test_weighted_shortest_returns_bounded_path_over_cheaper_long_one(
    gds_neo4j_client: Neo4jClient,
) -> None:
    """Single weighted shortest path with a tight hop bound: the cheaper 2-hop detour is out of
    reach, so the direct 1-hop route is returned rather than nothing (regression for the old
    top-k post-filter, which dropped the cheapest path and returned None)."""

    adapter = _adapter(gds_neo4j_client, graph_algorithms=True)
    # direct a→b expensive (5.0) 1 hop; detour a→c→b cheap (0.2) 2 hops.
    await _triangle(adapter, direct_w=5.0)

    path = await adapter.shortest_path(
        VertexRef(kind="WUser", key="a"),
        VertexRef(kind="WUser", key="b"),
        ShortestPathParams(max_hops=1, weight_property="w"),
    )

    assert path is not None
    assert [v.id for v in path.vertices] == ["a", "b"]


async def test_weighted_shortest_grows_window_past_candidate_buffer(
    gds_neo4j_client: Neo4jClient,
) -> None:
    """More cheaper-but-over-long paths than the fixed candidate buffer must not hide a valid
    bounded path: the adaptive window grows until the bounded route is found (regression for the
    fixed ``k + buffer`` over-fetch, which filled the window with over-long paths and returned
    nothing)."""

    adapter = _adapter(gds_neo4j_client, graph_algorithms=True)
    await adapter.create_vertex("WUser", WUserCreate(id="a"))
    await adapter.create_vertex("WUser", WUserCreate(id="b"))
    # Expensive direct a→b (1 hop) — the only route within max_hops=1.
    await adapter.create_edge("WLINK", WEdgeCreate(id="ab", from_key="a", to_key="b", w=100.0))
    # 40 cheap 2-hop detours a→mᵢ→b, well past the 32-candidate buffer and each far cheaper than
    # the direct edge — so Yen's fills a fixed window with these over-long paths before the direct.
    for i in range(40):
        mid = f"m{i}"
        await adapter.create_vertex("WUser", WUserCreate(id=mid))
        await adapter.create_edge("WLINK", WEdgeCreate(id=f"a_{mid}", from_key="a", to_key=mid, w=1.0))
        await adapter.create_edge("WLINK", WEdgeCreate(id=f"{mid}_b", from_key=mid, to_key="b", w=1.0))

    path = await adapter.shortest_path(
        VertexRef(kind="WUser", key="a"),
        VertexRef(kind="WUser", key="b"),
        ShortestPathParams(max_hops=1, weight_property="w"),
    )

    assert path is not None
    assert [v.id for v in path.vertices] == ["a", "b"]


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
    await a_adapter.create_edge("WLINK", WEdgeCreate(id="ac", from_key="a", to_key="c", w=0.1))

    await b_adapter.create_vertex("WUser", WUserCreate(id="c"))
    await b_adapter.create_vertex("WUser", WUserCreate(id="b"))
    await b_adapter.create_edge("WLINK", WEdgeCreate(id="cb", from_key="c", to_key="b", w=0.1))

    path = await a_adapter.shortest_path(
        VertexRef(kind="WUser", key="a"),
        VertexRef(kind="WUser", key="b"),
        ShortestPathParams(max_hops=5, weight_property="w"),
    )
    assert path is None


async def test_weighted_k_shortest_rebuilds_correct_parallel_edge(
    gds_neo4j_client: Neo4jClient,
) -> None:
    """Parallel edges of different weights between the same pair: each ranked path is rebuilt from
    the relationship GDS actually charged (matched by per-hop cost), not always the cheapest — so a
    later, costlier path does not surface the cheaper edge's properties."""

    adapter = _adapter(gds_neo4j_client, graph_algorithms=True)
    await adapter.create_vertex("WUser", WUserCreate(id="a"))
    await adapter.create_vertex("WUser", WUserCreate(id="b"))
    # Two parallel a→b relationships with distinct weights.
    await adapter.create_edge("WLINK", WEdgeCreate(id="ab1", from_key="a", to_key="b", w=1.0))
    await adapter.create_edge("WLINK", WEdgeCreate(id="ab3", from_key="a", to_key="b", w=3.0))

    paths = await adapter.k_shortest_paths(
        VertexRef(kind="WUser", key="a"),
        VertexRef(kind="WUser", key="b"),
        ShortestPathParams(max_hops=1, weight_property="w"),
        k=2,
    )

    # The cheaper path carries the w=1 edge; the second carries the w=3 edge (not a second w=1).
    assert [round(p.edges[0].w, 3) for p in paths] == [1.0, 3.0]

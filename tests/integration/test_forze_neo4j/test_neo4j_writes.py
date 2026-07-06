"""Integration coverage for WS4 write methods on Neo4j (beyond the conformance snapshot)."""

from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.graph import (
    EdgeRef,
    GraphEdgeDirectionality,
    GraphEdgeEndpoint,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
    VertexRef,
)
from forze.application.contracts.tenancy import TenantIdentity
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


class FollowsUpdate(BaseModel):
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
        name="w",
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


def _u(k: str) -> VertexRef:
    return VertexRef(kind="User", key=k)


async def test_ensure_vertex_creates_then_leaves_existing_unchanged(
    neo4j_client: Neo4jClient,
) -> None:
    adapter = _adapter(neo4j_client)

    created = await adapter.ensure_vertex("User", UserCreate(id="a", name="first"))
    assert created is not None and created.name == "first"

    again = await adapter.ensure_vertex("User", UserCreate(id="a", name="second"))
    assert again is not None and again.name == "first"  # unchanged
    loaded = await adapter.get_vertex(_u("a"))
    assert loaded is not None and loaded.name == "first"


async def test_update_and_delete_edge_endpoints_mode(neo4j_client: Neo4jClient) -> None:
    adapter = _adapter(neo4j_client)
    await adapter.create_vertex("User", UserCreate(id="a"))
    await adapter.create_vertex("User", UserCreate(id="b"))
    await adapter.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b", weight=1))

    ref = EdgeRef.by_endpoints("FOLLOWS", _u("a"), _u("b"))
    updated = await adapter.update_edge(ref, FollowsUpdate(weight=7))
    assert updated.weight == 7

    await adapter.delete_edge(ref)
    assert await adapter.edge_exists(ref) is False


async def test_bulk_create_and_delete(neo4j_client: Neo4jClient) -> None:
    adapter = _adapter(neo4j_client)

    out = await adapter.create_vertices(
        [("User", UserCreate(id=k)) for k in ("a", "b", "c")]
    )
    assert out is not None and [v.id for v in out] == ["a", "b", "c"]  # input order

    await adapter.create_edges(
        [
            ("RATED", RatedCreate(id="r1", from_key="a", to_key="b", score=1)),
            ("RATED", RatedCreate(id="r2", from_key="a", to_key="c", score=2)),
        ]
    )
    assert await adapter.count_edges("RATED") == 2

    await adapter.delete_edges([EdgeRef.by_key("RATED", "r1"), EdgeRef.by_key("RATED", "r2")])
    assert await adapter.count_edges("RATED") == 0

    await adapter.delete_vertices([_u("a"), _u("b")])
    assert await adapter.count_vertices("User") == 1  # only c remains


class TaggedCreate(BaseModel):
    from_key: str
    to_key: str
    from_kind: str
    to_kind: str


class TaggedRead(BaseModel):
    weight: int | None = None


def _multi_adapter(client: Neo4jClient) -> Neo4jGraphAdapter:
    spec = GraphModuleSpec(
        name="wm",
        nodes=(
            GraphNodeSpec(name="Post", read=UserRead, create=UserCreate),
            GraphNodeSpec(name="Note", read=UserRead, create=UserCreate),
            GraphNodeSpec(name="Tag", read=UserRead, create=UserCreate),
        ),
        edges=(
            GraphEdgeSpec(
                name="TAGGED",
                read=TaggedRead,
                identity="endpoints",
                endpoints=(
                    GraphEdgeEndpoint(from_kind="Post", to_kind="Tag"),
                    GraphEdgeEndpoint(from_kind="Note", to_kind="Tag"),
                ),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
        ),
    )
    return Neo4jGraphAdapter(spec=spec, client=client)


async def test_multi_endpoint_edges_routed_by_kind(neo4j_client: Neo4jClient) -> None:
    adapter = _multi_adapter(neo4j_client)
    await adapter.create_vertex("Post", UserCreate(id="1"))
    await adapter.create_vertex("Note", UserCreate(id="1"))
    await adapter.create_vertex("Tag", UserCreate(id="x"))

    await adapter.create_edge(
        "TAGGED", TaggedCreate(from_key="1", to_key="x", from_kind="Post", to_kind="Tag")
    )
    await adapter.create_edge(
        "TAGGED", TaggedCreate(from_key="1", to_key="x", from_kind="Note", to_kind="Tag")
    )

    assert await adapter.count_edges("TAGGED") == 2  # distinct despite identical keys
    post_ref = EdgeRef.by_endpoints(
        "TAGGED", VertexRef(kind="Post", key="1"), VertexRef(kind="Tag", key="x")
    )
    assert await adapter.edge_exists(post_ref) is True


async def test_delete_vertices_is_tenant_scoped(neo4j_client: Neo4jClient) -> None:
    ta = _adapter(
        neo4j_client, tenant_aware=True, tenant_provider=lambda: TenantIdentity(tenant_id=UUID(int=1))
    )
    tb = _adapter(
        neo4j_client, tenant_aware=True, tenant_provider=lambda: TenantIdentity(tenant_id=UUID(int=2))
    )
    await ta.create_vertex("User", UserCreate(id="x"))
    await tb.create_vertex("User", UserCreate(id="x"))  # same key, other tenant

    await ta.delete_vertices([_u("x")])

    assert await ta.count_vertices("User") == 0
    assert await tb.count_vertices("User") == 1  # tenant B's node untouched

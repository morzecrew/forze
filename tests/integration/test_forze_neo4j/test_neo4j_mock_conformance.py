"""Differential conformance: the mock read-introspection ≡ Neo4j on the same graph.

Seeds an identical graph in the in-memory mock and in Neo4j, runs each WS2 read method on
both, and asserts identical results — so the mock is a faithful reference for the read
surface (not merely "some subset works").
"""

from __future__ import annotations

from typing import Any

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
from forze_mock import MockDepsModule, MockState
from forze_neo4j.adapters import Neo4jGraphAdapter
from forze_neo4j.kernel.client import Neo4jClient
from tests.support.execution_context import context_from_deps

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
        name="conf",
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


async def _seed(cmd: Any) -> None:
    await cmd.create_vertex("User", UserCreate(id="a", name="Ana"))
    await cmd.create_vertex("User", UserCreate(id="b", name="Bo"))
    await cmd.create_vertex("User", UserCreate(id="c", name="Ana"))
    await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b"))
    await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="c"))
    await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="b", to_key="c"))
    await cmd.create_edge("RATED", RatedCreate(id="r1", from_key="a", to_key="b", score=5))


def _u(k: str) -> VertexRef:
    return VertexRef(kind="User", key=k)


def _dump(models: Any) -> list[dict]:
    return sorted((m.model_dump() for m in models), key=lambda d: sorted(d.items()))


async def _read_snapshot(port: Any) -> dict[str, Any]:
    """Run every WS2 read method and capture comparable results."""

    incident = await port.incident_edges(
        _u("a"), GraphDirection.OUT, frozenset({"FOLLOWS"}), limit=10
    )
    return {
        "get_vertices": [
            m.model_dump()
            for m in await port.get_vertices([_u("c"), _u("missing"), _u("a")])
        ],
        "get_edges": _dump(
            await port.get_edges(
                [EdgeRef.by_key("RATED", "r1"), EdgeRef.by_endpoints("FOLLOWS", _u("a"), _u("b"))]
            )
        ),
        "edge_exists_keyed": await port.edge_exists(EdgeRef.by_key("RATED", "r1")),
        "edge_exists_missing": await port.edge_exists(EdgeRef.by_key("RATED", "no")),
        "edge_exists_endpoints": await port.edge_exists(
            EdgeRef.by_endpoints("FOLLOWS", _u("a"), _u("b"))
        ),
        "edge_exists_reversed": await port.edge_exists(
            EdgeRef.by_endpoints("FOLLOWS", _u("b"), _u("a"))
        ),
        "count_vertices": await port.count_vertices("User"),
        "count_vertices_filtered": await port.count_vertices(
            "User", property_filter={"name": "Ana"}
        ),
        "count_edges_follows": await port.count_edges("FOLLOWS"),
        "count_edges_rated_filtered": await port.count_edges(
            "RATED", property_filter={"score": 5}
        ),
        "degree_out": await port.vertex_degree(_u("a"), direction=GraphDirection.OUT),
        "degree_follows": await port.vertex_degree(
            _u("a"), direction=GraphDirection.OUT, edge_kinds=frozenset({"FOLLOWS"})
        ),
        "neighbors_out": await port.count_neighbors(
            _u("a"), direction=GraphDirection.OUT, edge_kinds=frozenset({"FOLLOWS"})
        ),
        "incident_count": len(incident),
    }


async def test_mock_matches_neo4j_read_surface(neo4j_client: Neo4jClient) -> None:
    spec = _spec()

    # Mock plane.
    mock_ctx: ExecutionContext = context_from_deps(MockDepsModule(state=MockState())())
    mock_cmd = mock_ctx.graph.command(spec)
    mock_qry = mock_ctx.graph.query(spec)
    await _seed(mock_cmd)

    # Neo4j plane.
    neo = Neo4jGraphAdapter(spec=spec, client=neo4j_client)
    await _seed(neo)

    mock_snap = await _read_snapshot(mock_qry)
    neo_snap = await _read_snapshot(neo)

    assert mock_snap == neo_snap

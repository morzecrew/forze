"""Differential conformance: the mock's keyset walk ≡ Neo4j's, on the same graph.

The mock is the oracle every other graph test is written against, so a stream it serves from a
sorted Python list and a stream Neo4j serves from `ORDER BY … LIMIT` with a `>` seek predicate
have to agree — otherwise every mock-backed proof about streaming is a proof about the mock.
The two facts that only real Cypher can settle are here: that the seek is **strictly** greater
(an inclusive one re-emits the bookmark forever) and that a keyset walk, unlike the
`SKIP`-paged `find_vertices` beside it, does not lose a row to a concurrent insert.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.graph import (
    GraphEdgeDirectionality,
    GraphEdgeEndpoint,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
    GraphReadCapabilities,
)
from forze.application.execution import ExecutionContext
from forze.application.integrations.graph import graph_read_capabilities
from forze.base.exceptions import CoreException
from forze_mock import MockDepsModule, MockState
from forze_neo4j.adapters import Neo4jGraphAdapter
from forze_neo4j.kernel.client import Neo4jClient
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #


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
        name="stream_conf",
        nodes=(GraphNodeSpec(name="User", read=UserRead, create=UserCreate),),
        edges=(
            GraphEdgeSpec(
                name="RATED",
                read=RatedRead,
                identity="key",
                key_field="id",
                endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="User"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
            # No key of its own — the refusal has to hold on the real backend too.
            GraphEdgeSpec(
                name="FOLLOWS",
                read=FollowsRead,
                identity="endpoints",
                endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="User"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
        ),
    )


_USERS = 25


async def _seed(cmd: Any) -> None:
    for i in range(_USERS):
        await cmd.create_vertex("User", UserCreate(id=f"u{i:02d}", name=f"n{i % 3}"))

    for i in range(_USERS - 1):
        await cmd.create_edge(
            "RATED",
            RatedCreate(id=f"r{i:02d}", from_key=f"u{i:02d}", to_key=f"u{i + 1:02d}", score=i),
        )

    await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="u00", to_key="u01"))


async def _collect(stream: Any) -> list[Any]:
    return [row async for batch in stream for row in batch]


async def _batch_sizes(stream: Any) -> list[int]:
    return [len(batch) async for batch in stream]


async def _stream_snapshot(port: Any) -> dict[str, Any]:
    """Everything a streaming caller can observe, on both backends."""

    return {
        "vertices": [
            row.id for row in await _collect(port.find_vertices_stream("User", chunk_size=7))
        ],
        "vertex_batches": await _batch_sizes(port.find_vertices_stream("User", chunk_size=7)),
        "vertices_filtered": sorted(
            row.id
            for row in await _collect(
                port.find_vertices_stream("User", property_filter={"name": "n1"}, chunk_size=4)
            )
        ),
        "edges": [row.id for row in await _collect(port.find_edges_stream("RATED", chunk_size=6))],
        "edge_batches": await _batch_sizes(port.find_edges_stream("RATED", chunk_size=6)),
        "capabilities": graph_read_capabilities(port),
    }


# ....................... #


async def test_mock_matches_neo4j_streaming_surface(neo4j_client: Neo4jClient) -> None:
    spec = _spec()

    mock_ctx: ExecutionContext = context_from_deps(MockDepsModule(state=MockState())())
    await _seed(mock_ctx.graph.command(spec))

    neo = Neo4jGraphAdapter(spec=spec, client=neo4j_client)
    await _seed(neo)

    mock_snap = await _stream_snapshot(mock_ctx.graph.query(spec))
    neo_snap = await _stream_snapshot(neo)

    assert mock_snap == neo_snap

    # …and the walk is actually complete and actually paged, rather than both backends
    # agreeing on the same wrong answer.
    assert mock_snap["vertices"] == [f"u{i:02d}" for i in range(_USERS)]
    assert mock_snap["vertex_batches"] == [7, 7, 7, 4]
    assert mock_snap["edges"] == [f"r{i:02d}" for i in range(_USERS - 1)]
    assert mock_snap["capabilities"] == GraphReadCapabilities(
        supports_vertex_streaming=True,
        supports_edge_streaming=True,
    )


async def test_the_seek_is_strictly_past_the_bookmark(neo4j_client: Neo4jClient) -> None:
    # An inclusive seek (``>=``) re-emits the bookmark row on every page: the walk yields
    # duplicates and never terminates. Cypher is the only place this can be checked — the
    # predicate is in the query text.
    spec = _spec()
    neo = Neo4jGraphAdapter(spec=spec, client=neo4j_client)
    await _seed(neo)

    ids = [row.id for row in await _collect(neo.find_vertices_stream("User", chunk_size=1))]

    assert ids == sorted(ids)
    assert len(ids) == len(set(ids)) == _USERS  # no row repeated at a page boundary


async def test_a_vertex_inserted_behind_the_cursor_is_not_skipped(
    neo4j_client: Neo4jClient,
) -> None:
    # The failure keyset paging exists to prevent, against the real engine: with ``SKIP $offset``
    # an insert before the cursor shifts every later row one place along, and the next page
    # steps over one. A key bookmark does not move.
    spec = _spec()
    neo = Neo4jGraphAdapter(spec=spec, client=neo4j_client)
    await _seed(neo)

    seen: list[str] = []

    async for batch in neo.find_vertices_stream("User", chunk_size=5):
        seen.extend(row.id for row in batch)

        if len(seen) == 5:
            # Sorts before every row still to come — i.e. behind the cursor.
            await neo.create_vertex("User", UserCreate(id="u00a", name="late"))

    assert seen == [f"u{i:02d}" for i in range(_USERS)]


async def test_an_endpoint_identified_edge_is_refused_on_neo4j_too(
    neo4j_client: Neo4jClient,
) -> None:
    spec = _spec()
    neo = Neo4jGraphAdapter(spec=spec, client=neo4j_client)

    with pytest.raises(CoreException) as exc_info:
        await _collect(neo.find_edges_stream("FOLLOWS"))

    assert exc_info.value.code == "graph_streaming_unsupported"
    assert "FOLLOWS" in str(exc_info.value)

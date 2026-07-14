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

from forze.application.contracts.crypto import (
    AesGcmAead,
    FieldEncryption,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.graph import (
    EdgeRef,
    GraphEdgeDirectionality,
    GraphEdgeEndpoint,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
    GraphReadCapabilities,
    VertexRef,
)
from forze.application.execution import ExecutionContext
from forze.application.integrations.crypto import Keyring
from forze.application.integrations.graph import (
    graph_read_capabilities,
    resolve_graph_codecs,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockDepsModule, MockKeyManagement, MockState
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

    # A keyed edge kind (bookmarked on its own key) and an endpoint-identified one (bookmarked
    # on its endpoint pair) over the same chain of vertices — one edge of each per pair.
    for i in range(_USERS - 1):
        await cmd.create_edge(
            "RATED",
            RatedCreate(id=f"r{i:02d}", from_key=f"u{i:02d}", to_key=f"u{i + 1:02d}", score=i),
        )
        await cmd.create_edge(
            "FOLLOWS",
            FollowsCreate(from_key=f"u{i:02d}", to_key=f"u{i + 1:02d}", weight=i),
        )


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


async def test_an_endpoint_identified_edge_walks_on_its_endpoint_pair(
    neo4j_client: Neo4jClient,
) -> None:
    """An edge with no key of its own is bookmarked on the pair that *is* its identity.

    It used to be refused outright — there was nothing to bookmark, so an app that modelled a
    FOLLOWS-style edge by its endpoints could not export its graph at all.
    """

    spec = _spec()
    neo = Neo4jGraphAdapter(spec=spec, client=neo4j_client)
    await _seed(neo)

    mock_ctx: ExecutionContext = context_from_deps(MockDepsModule(state=MockState())())
    await _seed(mock_ctx.graph.command(spec))

    neo_rows = await _collect(neo.find_edges_stream("FOLLOWS", chunk_size=2))
    mock_rows = await _collect(
        mock_ctx.graph.query(spec).find_edges_stream("FOLLOWS", chunk_size=2)
    )

    # The seed lays one FOLLOWS edge per consecutive pair.
    assert sorted(r.weight for r in neo_rows) == sorted(r.weight for r in mock_rows)
    assert len(neo_rows) == _USERS - 1


async def test_duplicate_edges_on_one_pair_are_all_walked(neo4j_client: Neo4jClient) -> None:
    """No edge falls through a page boundary, even when a pair carries several.

    The hazard the pair cursor is built around, and the reason its Cypher groups by pair rather
    than limiting rows: a row-bounded page could cut between two edges of a pair, after which
    the next seek steps strictly past that pair and the leftover is never seen again — silent,
    and indistinguishable from an edge that was never there.

    ``create_edge`` refuses to make such a duplicate now (the pair *is* the kind's identity),
    but that does not retire the hazard: a graph written before it did not refuse, the raw query
    hatch still can, and nothing outside the framework is bound by the declaration at all. So
    the duplicate is planted the way it actually arises — raw Cypher, straight past the port —
    because an export has to carry the graph it finds, not the graph the spec hoped for.

    ``chunk_size=1`` puts a page boundary on *every* pair, including the duplicated one.
    """

    spec = _spec()
    neo = Neo4jGraphAdapter(spec=spec, client=neo4j_client)
    await _seed(neo)

    await neo4j_client.run(
        "MATCH (a:User {id: 'u00'}), (b:User {id: 'u01'}) CREATE (a)-[:FOLLOWS {weight: 999}]->(b)"
    )

    rows = await _collect(neo.find_edges_stream("FOLLOWS", chunk_size=1))

    assert len(rows) == _USERS  # the (_USERS - 1) seeded edges, plus the duplicate
    assert 999 in {r.weight for r in rows}


async def test_create_edge_enforces_the_endpoint_identity(neo4j_client: Neo4jClient) -> None:
    """``identity="endpoints"`` promises at most one edge per pair — and now keeps it.

    It did not. ``create_edge`` compiled to a bare Cypher ``CREATE``, so a second call on the
    same pair laid a parallel relationship: the pair then addressed two edges, ``get_edge``
    returned an arbitrary one of them, and ``update_edge`` / ``delete_edge`` hit both. No
    constraint could catch it — Neo4j constrains *properties*, not graph shape — so the create
    goes through a ``MERGE`` (which also locks both anchor nodes, serializing concurrent creates
    on the same pair) and reports whether it created.

    A keyed edge keeps the plain ``CREATE``: its identity is a property, and ``ensure_schema``
    provisions ``REQUIRE r.<key> IS UNIQUE`` for exactly this.
    """

    spec = _spec()
    neo = Neo4jGraphAdapter(spec=spec, client=neo4j_client)

    await neo.create_vertex("User", UserCreate(id="ea"))
    await neo.create_vertex("User", UserCreate(id="eb"))
    await neo.create_edge("FOLLOWS", FollowsCreate(from_key="ea", to_key="eb", weight=1))

    with pytest.raises(CoreException) as exc_info:
        await neo.create_edge("FOLLOWS", FollowsCreate(from_key="ea", to_key="eb", weight=2))

    assert exc_info.value.kind is ExceptionKind.CONFLICT
    assert exc_info.value.code == "graph_edge_endpoints_conflict"

    # One edge, and the create that lost did not touch it.
    edge = await neo.get_edge(
        EdgeRef.by_endpoints(
            "FOLLOWS", VertexRef(kind="User", key="ea"), VertexRef(kind="User", key="eb")
        )
    )
    assert edge.weight == 1

    # …and the transient marker the MERGE uses to report "created" never reaches disk. The read
    # model would silently drop an unknown field, so ask Neo4j for the raw stored properties.
    raw = await neo4j_client.run(
        "MATCH (:User {id: 'ea'})-[r:FOLLOWS]->(:User {id: 'eb'}) RETURN properties(r) AS r"
    )

    assert len(raw) == 1
    assert raw[0]["r"] == {"weight": 1}


async def test_a_vertex_kind_cannot_seal_its_own_key_field(neo4j_client: Neo4jClient) -> None:
    """A sealed key is not a key — and the real backend is the only place that showed it.

    ``create_vertex`` seals every property the encryption policy names, and a lookup by key
    matches the caller's **plaintext** against what was stored. Name the key field in that
    policy and the two never meet: the vertex was written, and could never be fetched, updated
    or deleted by its own key again. The mock hid it entirely — it stores properties unsealed
    and keys its store by the plaintext, so the round-trip worked there and only there.

    Refused at spec construction now, so the state is unreachable rather than silent. This test
    is against Neo4j because that is where the damage actually happened.
    """

    with pytest.raises(CoreException) as exc_info:
        GraphNodeSpec(
            name="User",
            read=UserRead,
            create=UserCreate,
            encryption=FieldEncryption(encrypted=frozenset({"id"})),
        )

    assert exc_info.value.code == "graph_sealed_key_field"

    # …and a kind that seals an ordinary property is untouched: it writes, reads back, and
    # streams, against the real backend.
    spec = GraphModuleSpec(
        name="sealed_prop",
        nodes=(
            GraphNodeSpec(
                name="SPUser",
                read=UserRead,
                create=UserCreate,
                encryption=FieldEncryption(encrypted=frozenset({"name"})),
            ),
        ),
        edges=(),
    )
    codecs = resolve_graph_codecs(
        spec,
        keyring=Keyring(
            kms=MockKeyManagement(),
            aead=AesGcmAead(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        ),
        deterministic=None,
        tenant_provider=lambda: None,
    )
    neo = Neo4jGraphAdapter(spec=spec, client=neo4j_client, codecs=codecs)

    await neo.create_vertex("SPUser", UserCreate(id="sp1", name="Ana"))

    found = await neo.get_vertex(VertexRef(kind="SPUser", key="sp1"))

    assert found is not None
    assert found.name == "Ana"  # sealed at rest, decrypted on read

    streamed = await _collect(neo.find_vertices_stream("SPUser"))
    assert [row.id for row in streamed] == ["sp1"]

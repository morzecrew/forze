"""RFC 0017 P4: the graph plane round-trips through a *real* Neo4j backend.

# covers: forze_kits.integrations.portability (graph plane on real Neo4j)
# covers: forze_neo4j.adapters.graph.find_edges_export_stream

``find_edges_stream`` yields read models with no endpoint keys, so P4 added
``find_edges_export_stream`` to carry them; Neo4j's implementation surfaces the endpoint labels and
keys it already reads for the cursor. This is the check the mock cannot give: import a portable graph
archive into a live Neo4j over Cypher, re-export it, and the two archives' rows are identical — the
export format is its own equality observable. The mock stands in for the other backend (a
Neo4j→Neo4j leg needs two databases; Community has one), and the round-trip exercises **both** real
legs: the import writes through ``ensure_vertex`` / ``ensure_edge``, the re-export reads through
``find_vertices_stream`` / ``find_edges_export_stream``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import orjson
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
from forze.application.contracts.inventory import FrozenSpecRegistry, SpecRegistry
from forze.application.execution import ExecutionContext
from forze.base.primitives import JsonDict
from forze_kits.integrations.portability import (
    ArchiveExporter,
    ArchiveImporter,
    FullScope,
)
from forze_kits.integrations.portability.format import read_rows
from forze_kits.integrations.quiesce import QuiesceReport
from forze_mock import MockDepsModule, MockState
from forze_neo4j.execution.deps import ConfigurableNeo4jGraph  # noqa: F401 - ensures deps import
from forze_neo4j.execution.deps.configs import Neo4jGraphConfig
from forze_neo4j.execution.deps.module import Neo4jDepsModule
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


class KnowsRead(BaseModel):
    id: str
    weight: int | None = None


class KnowsCreate(BaseModel):
    id: str
    from_key: str
    to_key: str
    weight: int | None = None


class FollowsRead(BaseModel):
    since: int | None = None


class FollowsCreate(BaseModel):
    from_key: str
    to_key: str
    since: int | None = None


SOCIAL = GraphModuleSpec(
    name="social",
    nodes=(GraphNodeSpec(name="User", read=UserRead, create=UserCreate),),
    edges=(
        GraphEdgeSpec(
            name="KNOWS",
            read=KnowsRead,
            identity="key",
            key_field="id",
            endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="User"),),
            directionality=GraphEdgeDirectionality.DIRECTED,
        ),
        GraphEdgeSpec(
            name="FOLLOWS",
            read=FollowsRead,
            identity="endpoints",
            endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="User"),),
            directionality=GraphEdgeDirectionality.DIRECTED,
        ),
    ),
)

_ATTESTED = QuiesceReport(planes=(), admission_held=True)


def _registry() -> FrozenSpecRegistry:
    return SpecRegistry().register(SOCIAL).freeze()


def _neo_ctx(client: Neo4jClient) -> ExecutionContext:
    return context_from_deps(
        Neo4jDepsModule(client=client, graphs={"social": Neo4jGraphConfig()})()
    )


async def _seed(command: Any) -> None:
    for index in range(4):
        await command.create_vertex("User", UserCreate(id=f"u{index}", name=f"n{index}"))

    for index in range(3):
        await command.create_edge(
            "KNOWS",
            KnowsCreate(id=f"k{index}", from_key=f"u{index}", to_key=f"u{index + 1}", weight=index),
        )

    await command.create_edge("FOLLOWS", FollowsCreate(from_key="u0", to_key="u3", since=2020))


async def _graph_rows(archive: Path) -> dict[str, list[JsonDict]]:
    """Every graph file's rows, keyed by path and sorted, so a cross-backend order difference in
    the keyset walk does not read as a divergence — the rows are the observable, not their order."""

    out: dict[str, list[JsonDict]] = {}

    for path in sorted((archive / "graph").rglob("*.jsonl.gz")):
        rows = [row async for row in read_rows(path)]
        out[str(path.relative_to(archive))] = sorted(
            rows, key=lambda row: orjson.dumps(row, option=orjson.OPT_SORT_KEYS)
        )

    return out


# ....................... #


async def test_graph_round_trips_through_real_neo4j(
    neo4j_client: Neo4jClient, tmp_path: Path
) -> None:
    registry = _registry()

    # Seed the mock (source), export it.
    mock_ctx = context_from_deps(MockDepsModule(state=MockState())())
    await _seed(mock_ctx.graph.command(SOCIAL))

    archive_a = tmp_path / "a"
    export_a = await ArchiveExporter()(
        mock_ctx, registry, archive_a, scope=FullScope(quiesce=_ATTESTED)
    )
    assert export_a.total_vertices == 4
    assert export_a.total_edges == 4  # 3 KNOWS + 1 FOLLOWS

    # Import into a live Neo4j — the real ensure_vertex / ensure_edge write path.
    neo_ctx = _neo_ctx(neo4j_client)
    result = await ArchiveImporter()(neo_ctx, registry, archive_a)
    assert result.total_vertices == 4
    assert result.total_edges == 4

    # A vertex and both edge kinds actually landed in Neo4j, addressable by their identity.
    query = neo_ctx.graph.query(SOCIAL)
    user = await query.get_vertex(VertexRef(kind="User", key="u2"))
    assert isinstance(user, UserRead) and user.name == "n2"

    knows = await query.get_edge(EdgeRef.by_key("KNOWS", "k1"))
    assert isinstance(knows, KnowsRead) and knows.weight == 1

    follows = await query.get_edge(
        EdgeRef.by_endpoints(
            "FOLLOWS", VertexRef(kind="User", key="u0"), VertexRef(kind="User", key="u3")
        )
    )
    assert isinstance(follows, FollowsRead) and follows.since == 2020

    # Re-export from real Neo4j — the real find_vertices_stream / find_edges_export_stream read path
    # — and the format is its own equality observable: Neo4j's re-export matches the mock's export.
    archive_b = tmp_path / "b"
    export_b = await ArchiveExporter()(
        neo_ctx, registry, archive_b, scope=FullScope(quiesce=_ATTESTED)
    )
    assert export_b.total_vertices == 4
    assert export_b.total_edges == 4

    assert await _graph_rows(archive_a) == await _graph_rows(archive_b), (
        "a real Neo4j graph must re-export to exactly what the mock exported"
    )

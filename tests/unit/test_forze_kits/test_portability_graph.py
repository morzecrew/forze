"""P4: the graph plane round-trips through the portable archive — vertices and edges.

# covers: forze_kits.integrations.portability (graph plane)

Edges are the hard part. ``find_edges_stream`` yields read models with no endpoint keys, so P4 added
a ``GraphEdgeExportAware`` seam (``find_edges_export_stream``) that carries the endpoints an import
needs to ``ensure_edge`` them back. This proves the whole loop on the mock: seed a social graph
(``User`` vertices, a keyed ``KNOWS`` edge, an endpoint-identified ``FOLLOWS`` edge), export, import
into a fresh backend, and every vertex lands by its key and every edge by its identity — properties
and endpoints intact. It also proves the direct ``migrate`` carries the graph plane, and that a graph
with a read-only node kind is refused rather than exported un-importably.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import BaseModel

from forze import build_runtime
from forze.application.contracts.graph import (
    EdgeRef,
    GraphEdgeDirectionality,
    GraphEdgeEndpoint,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
    VertexRef,
)
from forze.application.contracts.inventory import SpecRegistry
from forze.application.execution import ExecutionRuntime
from forze.base.exceptions import CoreException
from forze_kits.integrations.portability import (
    ExportReport,
    FullScope,
    ImportReport,
    export_archive,
    import_archive,
    migrate,
)
from forze_kits.integrations.quiesce import QuiesceReport
from forze_mock import MockDepsModule
from forze_mock.state import MockState

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


def _runtime(state: MockState, spec: GraphModuleSpec = SOCIAL) -> ExecutionRuntime:
    return build_runtime(
        MockDepsModule(state=state), specs=SpecRegistry().register(spec), allow_unregistered=True
    )


async def _seed(runtime: ExecutionRuntime) -> None:
    async with runtime.scope():
        command = runtime.get_context().graph.command(SOCIAL)

        for index in range(3):
            await command.create_vertex("User", UserCreate(id=f"u{index}", name=f"n{index}"))

        await command.create_edge(
            "KNOWS", KnowsCreate(id="k0", from_key="u0", to_key="u1", weight=5)
        )
        await command.create_edge(
            "KNOWS", KnowsCreate(id="k1", from_key="u1", to_key="u2", weight=8)
        )
        await command.create_edge("FOLLOWS", FollowsCreate(from_key="u0", to_key="u2", since=2020))


async def _export(runtime: ExecutionRuntime, dest: Path) -> ExportReport:
    async with runtime.scope():
        return await export_archive(runtime, dest, scope=FullScope(quiesce=_ATTESTED))


async def _import(runtime: ExecutionRuntime, src: Path) -> ImportReport:
    async with runtime.scope():
        return await import_archive(runtime, src)


async def _vertex(runtime: ExecutionRuntime, key: str) -> UserRead:
    async with runtime.scope():
        got = (
            await runtime.get_context()
            .graph.query(SOCIAL)
            .get_vertex(VertexRef(kind="User", key=key))
        )

    assert isinstance(got, UserRead)
    return got


async def _keyed_edge(runtime: ExecutionRuntime, key: str) -> KnowsRead:
    async with runtime.scope():
        got = await runtime.get_context().graph.query(SOCIAL).get_edge(EdgeRef.by_key("KNOWS", key))

    assert isinstance(got, KnowsRead)
    return got


async def _endpoint_edge(runtime: ExecutionRuntime, frm: str, to: str) -> FollowsRead:
    async with runtime.scope():
        got = (
            await runtime.get_context()
            .graph.query(SOCIAL)
            .get_edge(
                EdgeRef.by_endpoints(
                    "FOLLOWS", VertexRef(kind="User", key=frm), VertexRef(kind="User", key=to)
                )
            )
        )

    assert isinstance(got, FollowsRead)
    return got


# ....................... #


@pytest.mark.asyncio
async def test_graph_round_trip_preserves_vertices_and_edges(tmp_path: Path) -> None:
    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"
    report = await _export(source, archive)

    assert report.total_vertices == 3
    assert report.total_edges == 3  # two KNOWS + one FOLLOWS
    assert (archive / "graph" / "social" / "nodes" / "User.jsonl.gz").exists()
    assert (archive / "graph" / "social" / "edges" / "KNOWS.jsonl.gz").exists()
    assert (archive / "graph" / "social" / "edges" / "FOLLOWS.jsonl.gz").exists()

    target = _runtime(MockState())
    result = await _import(target, archive)

    assert result.total_vertices == 3
    assert result.total_edges == 3

    user = await _vertex(target, "u1")
    assert user.id == "u1" and user.name == "n1", "vertex lands by its key, properties intact"

    knows = await _keyed_edge(target, "k0")
    assert knows.id == "k0" and knows.weight == 5, "keyed edge lands by its key"

    follows = await _endpoint_edge(target, "u0", "u2")
    assert follows.since == 2020, "endpoint-identified edge lands by its (from, to) pair"


@pytest.mark.asyncio
async def test_graph_reimport_is_idempotent(tmp_path: Path) -> None:
    """``ensure_vertex`` / ``ensure_edge`` make a re-run converge — no duplicate, no error."""

    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"
    await _export(source, archive)

    target = _runtime(MockState())
    await _import(target, archive)
    await _import(target, archive)  # second run must not raise or duplicate

    async with target.scope():
        query = target.get_context().graph.query(SOCIAL)
        assert await query.count_edges("KNOWS") == 2
        assert await query.count_edges("FOLLOWS") == 1


@pytest.mark.asyncio
async def test_graph_migrate_carries_the_plane(tmp_path: Path) -> None:
    """The direct ports-to-ports migrate carries the graph plane too — no file, endpoints intact."""

    source = _runtime(MockState())
    await _seed(source)

    target = _runtime(MockState())
    async with source.scope(), target.scope():
        report = await migrate(source, target, scope=FullScope(quiesce=_ATTESTED))

    assert report.total_vertices == 3
    assert report.total_edges == 3

    assert (await _vertex(target, "u2")).name == "n2"
    assert (await _keyed_edge(target, "k1")).weight == 8
    assert (await _endpoint_edge(target, "u0", "u2")).since == 2020


@pytest.mark.asyncio
async def test_re_export_equals_the_original(tmp_path: Path) -> None:
    """RFC §8 for the graph plane: import then re-export yields byte-identical graph files, so the
    round-trip is lossless by the format's own definition."""

    source = _runtime(MockState())
    await _seed(source)

    archive_a = tmp_path / "a"
    await _export(source, archive_a)

    target = _runtime(MockState())
    await _import(target, archive_a)

    archive_b = tmp_path / "b"
    await _export(target, archive_b)

    for name in ("nodes/User", "edges/KNOWS", "edges/FOLLOWS"):
        a = (archive_a / "graph" / "social" / f"{name}.jsonl.gz").read_bytes()
        b = (archive_b / "graph" / "social" / f"{name}.jsonl.gz").read_bytes()
        assert a == b, f"re-exported {name} must match the original"


@pytest.mark.asyncio
async def test_read_only_node_kind_is_refused(tmp_path: Path) -> None:
    """A graph module with a node kind that declares no create model cannot be imported, so export
    refuses it by name rather than shipping a graph no target could restore."""

    read_only = GraphModuleSpec(
        name="ro",
        nodes=(GraphNodeSpec(name="User", read=UserRead),),  # no create model
        edges=(),
    )
    runtime = _runtime(MockState(), spec=read_only)

    with pytest.raises(CoreException, match="create model"):
        await _export(runtime, tmp_path / "archive")

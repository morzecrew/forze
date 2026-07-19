"""Mock graph field encryption — declared node/edge properties are sealed at rest.

# covers: forze_mock.adapters.graph, forze_mock.execution.factories

The mock graph factory resolves the same per-kind ciphers the Neo4j factory does
(``resolve_graph_codecs``, fail-closed), so a kind that declares ``FieldEncryption``
stores envelopes in ``MockState`` and every read path decrypts. No pre-pass anywhere:
the module's keyring fills synchronously.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import FieldEncryption
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
from forze.base.crypto import ENVELOPE_B64_PREFIX
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_deps

# ----------------------- #


class PatientRead(BaseModel):
    id: str
    name: str | None = None
    diagnosis: str | None = None


class PatientCreate(BaseModel):
    id: str
    name: str | None = None
    diagnosis: str | None = None


class TreatedRead(BaseModel):
    note: str | None = None


class TreatedCreate(BaseModel):
    from_key: str
    to_key: str
    note: str | None = None


def _spec() -> GraphModuleSpec:
    return GraphModuleSpec(
        name="clinic",
        nodes=(
            GraphNodeSpec(
                name="Patient",
                read=PatientRead,
                create=PatientCreate,
                encryption=FieldEncryption(encrypted=frozenset({"diagnosis"})),
            ),
        ),
        edges=(
            GraphEdgeSpec(
                name="TREATED",
                read=TreatedRead,
                identity="endpoints",
                endpoints=(GraphEdgeEndpoint(from_kind="Patient", to_kind="Patient"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
                encryption=FieldEncryption(encrypted=frozenset({"note"})),
            ),
        ),
    )


def _sealed(value: object) -> bool:
    return isinstance(value, str) and value.startswith(ENVELOPE_B64_PREFIX)


@pytest.fixture
def state() -> MockState:
    return MockState()


@pytest.fixture
def ctx(state: MockState) -> ExecutionContext:
    return context_from_deps(MockDepsModule(state=state)())


# ....................... #


@pytest.mark.asyncio
async def test_vertex_properties_seal_and_round_trip(
    ctx: ExecutionContext, state: MockState
) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    qry = ctx.graph.query(spec)

    created = await cmd.create_vertex(
        "Patient", PatientCreate(id="p1", name="Ada", diagnosis="confidential")
    )
    assert created is not None
    assert created.diagnosis == "confidential"

    stored = state.graph_vertices["clinic"][("Patient", "p1")]
    assert _sealed(stored["diagnosis"]), stored["diagnosis"]
    assert stored["name"] == "Ada"  # undeclared properties stay plaintext

    got = await qry.get_vertex(VertexRef(kind="Patient", key="p1"))
    assert got is not None
    assert got.diagnosis == "confidential"


@pytest.mark.asyncio
async def test_vertex_update_re_seals_the_patched_property(
    ctx: ExecutionContext, state: MockState
) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)

    await cmd.create_vertex("Patient", PatientCreate(id="p1", diagnosis="one"))
    before = state.graph_vertices["clinic"][("Patient", "p1")]["diagnosis"]

    updated = await cmd.update_vertex(
        VertexRef(kind="Patient", key="p1"), PatientCreate(id="p1", diagnosis="two")
    )
    assert updated.diagnosis == "two"

    after = state.graph_vertices["clinic"][("Patient", "p1")]["diagnosis"]
    assert _sealed(after)
    assert after != before  # fresh envelope, not the old ciphertext


@pytest.mark.asyncio
async def test_edge_properties_seal_and_read_paths_decrypt(
    ctx: ExecutionContext, state: MockState
) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    qry = ctx.graph.query(spec)

    await cmd.create_vertex("Patient", PatientCreate(id="a"))
    await cmd.create_vertex("Patient", PatientCreate(id="b"))
    await cmd.create_edge("TREATED", TreatedCreate(from_key="a", to_key="b", note="private"))

    rec = state.graph_edges["clinic"][0]
    assert _sealed(rec["props"]["note"]), rec["props"]["note"]

    # get_edge decrypts.
    edge = await qry.get_edge(
        EdgeRef(
            kind="TREATED",
            from_ref=VertexRef(kind="Patient", key="a"),
            to_ref=VertexRef(kind="Patient", key="b"),
        )
    )
    assert edge is not None
    assert edge.note == "private"

    # The traversal read path decrypts the via-edge too.
    out = await qry.neighbors(
        VertexRef(kind="Patient", key="a"),
        GraphDirection.OUT,
        frozenset({"TREATED"}),
        limit=10,
    )
    assert out[0].via_edge.note == "private"

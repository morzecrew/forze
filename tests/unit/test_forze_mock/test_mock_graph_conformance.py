"""The mock's leg of the graph differential — same script the Neo4j leg runs."""

from __future__ import annotations

import pytest

from forze.application.execution import ExecutionContext
from forze_dst.conformance.graph import (
    EXPECTED_BOUNDED_NEIGHBORS_PORTABLE,
    EXPECTED_CASCADE,
    run_bounded_neighbors,
    run_detach_cascade,
)
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_deps
from tests.support.graph_conformance import (
    EDGE_KIND,
    HUB_KIND,
    UNWANTED_KIND,
    WANTED_KIND,
    GraphConformanceFixture,
    graph_conformance_spec,
)

pytestmark = pytest.mark.unit


def _planes():
    spec = graph_conformance_spec()
    ctx: ExecutionContext = context_from_deps(MockDepsModule(state=MockState())())

    return ctx.graph.command(spec), ctx.graph.query(spec)


@pytest.mark.conformance(plane="graph", engine="mock")
@pytest.mark.asyncio
async def test_bounded_neighbors_fills_the_page() -> None:
    cmd, qry = _planes()

    outcome = await run_bounded_neighbors(
        cmd,
        qry,
        GraphConformanceFixture(),
        hub_kind=HUB_KIND,
        wanted_kind=WANTED_KIND,
        unwanted_kind=UNWANTED_KIND,
        edge_kind=EDGE_KIND,
    )

    assert outcome.portable() == EXPECTED_BOUNDED_NEIGHBORS_PORTABLE


@pytest.mark.conformance(plane="graph", engine="mock")
@pytest.mark.asyncio
async def test_detach_cascade_takes_the_edges_and_nothing_else() -> None:
    cmd, qry = _planes()

    outcome = await run_detach_cascade(
        cmd,
        qry,
        GraphConformanceFixture(),
        vertex_kind=HUB_KIND,
        edge_kind=EDGE_KIND,
    )

    assert outcome == EXPECTED_CASCADE

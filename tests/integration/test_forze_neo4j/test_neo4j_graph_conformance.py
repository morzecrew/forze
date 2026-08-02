"""Neo4j's leg of the graph differential — the same script the mock leg runs.

The mock leg asserts against the same constants offline, so a divergence fails whichever
suite runs first: the pair is a comparison of two answers to one script, not two
implementations that merely both pass their own tests.
"""

from __future__ import annotations

import pytest

from forze_dst.conformance.graph import (
    EXPECTED_BOUNDED_NEIGHBORS_PORTABLE,
    EXPECTED_CASCADE,
    run_bounded_neighbors,
    run_detach_cascade,
)
from forze_neo4j.adapters import Neo4jGraphAdapter
from forze_neo4j.kernel.client import Neo4jClient
from tests.support.graph_conformance import (
    EDGE_KIND,
    HUB_KIND,
    UNWANTED_KIND,
    WANTED_KIND,
    GraphConformanceFixture,
    graph_conformance_spec,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.mark.conformance(plane="graph", engine="neo4j")
async def test_bounded_neighbors_fills_the_page(neo4j_client: Neo4jClient) -> None:
    port = Neo4jGraphAdapter(spec=graph_conformance_spec(), client=neo4j_client)

    outcome = await run_bounded_neighbors(
        port,
        port,
        GraphConformanceFixture(),
        hub_kind=HUB_KIND,
        wanted_kind=WANTED_KIND,
        unwanted_kind=UNWANTED_KIND,
        edge_kind=EDGE_KIND,
    )

    assert outcome.portable() == EXPECTED_BOUNDED_NEIGHBORS_PORTABLE


@pytest.mark.conformance(plane="graph", engine="neo4j")
async def test_detach_cascade_takes_the_edges_and_nothing_else(
    neo4j_client: Neo4jClient,
) -> None:
    port = Neo4jGraphAdapter(spec=graph_conformance_spec(), client=neo4j_client)

    outcome = await run_detach_cascade(
        port,
        port,
        GraphConformanceFixture(),
        vertex_kind=HUB_KIND,
        edge_kind=EDGE_KIND,
    )

    assert outcome == EXPECTED_CASCADE

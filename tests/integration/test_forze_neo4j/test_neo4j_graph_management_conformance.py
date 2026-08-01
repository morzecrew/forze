"""Neo4j schema provisioning against the shared battery.

The leg where provisioning is load-bearing: without the uniqueness constraint Cypher
``CREATE`` writes a second node under the same key, so the battery's control check — a
provisioned module refuses a duplicate — is asserting real engine behaviour here rather
than a property of a data structure.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
import pytest_asyncio
from pydantic import BaseModel

from forze.application.contracts.graph import GraphModuleSpec, GraphNodeSpec
from forze_neo4j.adapters import Neo4jGraphAdapter
from forze_neo4j.kernel.client import Neo4jClient
from tests.support.graph_management_conformance import (
    GRAPH_MANAGEMENT_BATTERY,
    Check,
    GraphManagementHarness,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _UserRead(BaseModel):
    id: str


class _UserCreate(BaseModel):
    id: str


def _spec(name: str) -> GraphModuleSpec:
    return GraphModuleSpec(
        name=name,
        nodes=(GraphNodeSpec(name=f"N{name}", read=_UserRead, create=_UserCreate),),
        edges=(),
    )


@pytest_asyncio.fixture
async def harness(neo4j_client: Neo4jClient) -> GraphManagementHarness:
    # A per-test module name keeps each run's constraints and nodes disjoint, so the
    # teardown checks cannot wipe another test's schema.
    name = f"mgmt{uuid4().hex[:8]}"
    adapter = Neo4jGraphAdapter(spec=_spec(name), client=neo4j_client)

    try:
        yield GraphManagementHarness(
            management=adapter,
            backend="neo4j",
            create_node=lambda key: adapter.create_vertex(f"N{name}", _UserCreate(id=key)),
            unique_key_is_droppable=True,
        )
    finally:
        await adapter.drop_schema()


@pytest.mark.conformance(plane="graph_management", engine="neo4j")
@pytest.mark.parametrize("check", GRAPH_MANAGEMENT_BATTERY, ids=lambda check: check.__name__)
async def test_graph_management_battery(check: Check, harness: GraphManagementHarness) -> None:
    await check(harness)

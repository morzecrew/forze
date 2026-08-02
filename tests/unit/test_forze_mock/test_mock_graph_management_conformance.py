"""The in-memory graph control plane against the shared provisioning battery.

This leg is the reason the battery exists: the oracle had no ``GraphManagementPort`` at all,
so ``ctx.graph.management(spec)`` could not even be resolved and an application's startup
provisioning was unrunnable in unit tests and simulation.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.graph import GraphModuleSpec, GraphNodeSpec
from forze.testing import context_from_modules
from forze_mock import MockDepsModule
from tests.support.graph_management_conformance import (
    GRAPH_MANAGEMENT_BATTERY,
    Check,
    GraphManagementHarness,
)

pytestmark = pytest.mark.asyncio


class _UserRead(BaseModel):
    id: str


class _UserCreate(BaseModel):
    id: str


def _spec(name: str) -> GraphModuleSpec:
    return GraphModuleSpec(
        name=name,
        nodes=(GraphNodeSpec(name="MUser", read=_UserRead, create=_UserCreate),),
        edges=(),
    )


@pytest.fixture
def harness() -> GraphManagementHarness:
    spec = _spec(f"mgmt_{uuid4().hex[:8]}")
    ctx = context_from_modules(MockDepsModule())
    graph = ctx.graph.command(spec)

    return GraphManagementHarness(
        management=ctx.graph.management(spec),
        backend="mock",
        create_node=lambda key: graph.create_vertex("MUser", _UserCreate(id=key)),
        # The in-memory store is keyed by (kind, key), so uniqueness is intrinsic and
        # drop_schema cannot take it away — stated on the adapter, declared here.
        unique_key_is_droppable=False,
    )


@pytest.mark.conformance(plane="graph_management", engine="mock")
@pytest.mark.parametrize("check", GRAPH_MANAGEMENT_BATTERY, ids=lambda check: check.__name__)
async def test_graph_management_battery(check: Check, harness: GraphManagementHarness) -> None:
    await check(harness)

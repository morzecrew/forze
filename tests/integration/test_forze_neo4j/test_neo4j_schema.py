"""Integration coverage for GraphManagementPort schema provisioning (ensure_schema)."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.graph import (
    GraphEdgeDirectionality,
    GraphEdgeEndpoint,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
)
from forze.base.exceptions import CoreException
from forze_neo4j.adapters import Neo4jGraphAdapter
from forze_neo4j.kernel.client import Neo4jClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class UserRead(BaseModel):
    id: str


class UserCreate(BaseModel):
    id: str


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
        name="schema_it",
        nodes=(GraphNodeSpec(name="SUser", read=UserRead, create=UserCreate),),
        edges=(
            GraphEdgeSpec(
                name="SRATED",
                read=RatedRead,
                identity="key",
                key_field="id",
                endpoints=(GraphEdgeEndpoint(from_kind="SUser", to_kind="SUser"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
        ),
    )


def _adapter(client: Neo4jClient) -> Neo4jGraphAdapter:
    return Neo4jGraphAdapter(spec=_spec(), client=client)


async def test_ensure_schema_is_idempotent(neo4j_client: Neo4jClient) -> None:
    """Running ensure_schema twice is a no-op the second time (IF NOT EXISTS)."""

    adapter = _adapter(neo4j_client)
    await adapter.ensure_schema()
    await adapter.ensure_schema()

    names = {
        r["name"]
        for r in await neo4j_client.run("SHOW CONSTRAINTS YIELD name RETURN name")
    }
    assert any(n.startswith("forze_schema_it") for n in names)


async def test_node_key_uniqueness_is_enforced(neo4j_client: Neo4jClient) -> None:
    """After ensure_schema a duplicate node key is rejected by the constraint."""

    adapter = _adapter(neo4j_client)
    await adapter.ensure_schema()

    await adapter.create_vertex("SUser", UserCreate(id="dup"))

    with pytest.raises(CoreException):
        await adapter.create_vertex("SUser", UserCreate(id="dup"))


async def test_keyed_edge_uniqueness_is_enforced(neo4j_client: Neo4jClient) -> None:
    """A keyed edge's key is unique once provisioned — the concurrent-ensure dup gap closes."""

    adapter = _adapter(neo4j_client)
    await adapter.ensure_schema()

    for k in ("a", "b", "c"):
        await adapter.create_vertex("SUser", UserCreate(id=k))

    await adapter.create_edge(
        "SRATED", RatedCreate(id="r1", from_key="a", to_key="b", score=5)
    )

    # Same edge key on a different endpoint pair now violates the relationship constraint.
    with pytest.raises(CoreException):
        await adapter.create_edge(
            "SRATED", RatedCreate(id="r1", from_key="a", to_key="c", score=3)
        )


async def test_drop_schema_removes_constraints(neo4j_client: Neo4jClient) -> None:
    adapter = _adapter(neo4j_client)
    await adapter.ensure_schema()
    await adapter.drop_schema()

    names = {
        r["name"]
        for r in await neo4j_client.run("SHOW CONSTRAINTS YIELD name RETURN name")
    }
    assert not any(n.startswith("forze_schema_it") for n in names)

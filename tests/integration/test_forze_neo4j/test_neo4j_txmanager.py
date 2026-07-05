"""Integration coverage for the Neo4j transaction manager: multi-statement atomicity."""

from __future__ import annotations

import pytest

from forze_neo4j.adapters import Neo4jTxManagerAdapter
from forze_neo4j.kernel.client import Neo4jClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_writes_in_one_scope_commit_atomically(neo4j_client: Neo4jClient) -> None:
    """Two writes inside one manager scope commit together."""

    mgr = Neo4jTxManagerAdapter(client=neo4j_client)

    async with mgr.transaction():
        await neo4j_client.run("CREATE (n:TxNode {id: 'a'})")
        await neo4j_client.run("CREATE (n:TxNode {id: 'b'})")

    rows = await neo4j_client.run("MATCH (n:TxNode) RETURN n.id AS id ORDER BY id")
    assert [r["id"] for r in rows] == ["a", "b"]


async def test_scope_rolls_back_all_writes_on_error(neo4j_client: Neo4jClient) -> None:
    """An error mid-scope rolls back every write in the unit — nothing persists."""

    mgr = Neo4jTxManagerAdapter(client=neo4j_client)

    with pytest.raises(ValueError):
        async with mgr.transaction():
            await neo4j_client.run("CREATE (n:TxNode {id: 'x'})")
            await neo4j_client.run("CREATE (n:TxNode {id: 'y'})")
            raise ValueError("abort after writes")

    rows = await neo4j_client.run("MATCH (n:TxNode) RETURN n.id AS id")
    assert rows == []

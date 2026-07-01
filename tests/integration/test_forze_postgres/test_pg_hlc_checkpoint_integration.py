"""Integration tests for the Postgres HLC high-water-mark store.

# covers: HlcCheckpointPort.load
# covers: HlcCheckpointPort.advance

The headline is co-location: ``advance`` runs on the caller's transaction connection, so
the node's clock mark commits together with the HLC-stamped rows it guards — a rollback
reverts the mark (it never advances for rows that did not commit), and the mark is a
monotonic ``GREATEST`` so it never goes backwards.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from psycopg import sql

from forze.base.primitives import HlcTimestamp
from forze_postgres.adapters.hlc_checkpoint import PostgresHlcCheckpointStore
from forze_postgres.adapters.txmanager import PostgresTxManagerAdapter
from forze_postgres.execution.deps.configs import PostgresHlcCheckpointConfig
from forze_postgres.kernel.client import PostgresClient

# ----------------------- #


@pytest.fixture
async def hlc_table(pg_client: PostgresClient) -> str:
    """Create a dedicated HLC checkpoint table and return its name."""

    table = f"hlc_{uuid4().hex[:8]}"
    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {table} (
                node_key TEXT   NOT NULL,
                hlc      BIGINT NOT NULL,
                PRIMARY KEY (node_key)
            )
            """
        ).format(table=sql.Identifier("public", table))
    )
    return table


def _store(
    pg_client: PostgresClient,
    table: str,
    *,
    node_key: str = "default",
) -> PostgresHlcCheckpointStore:
    return PostgresHlcCheckpointStore(
        client=pg_client,
        config=PostgresHlcCheckpointConfig(
            relation=("public", table), node_key=node_key
        ),
    )


# ....................... #


class TestPostgresHlcCheckpoint:
    async def test_load_is_none_when_empty(
        self, pg_client: PostgresClient, hlc_table: str
    ) -> None:
        assert await _store(pg_client, hlc_table).load() is None

    async def test_advance_then_load_roundtrips(
        self, pg_client: PostgresClient, hlc_table: str
    ) -> None:
        store = _store(pg_client, hlc_table)
        await store.advance(HlcTimestamp(1_700_000_000_000, 3))

        assert await store.load() == HlcTimestamp(1_700_000_000_000, 3)

    async def test_advance_is_monotonic(
        self, pg_client: PostgresClient, hlc_table: str
    ) -> None:
        store = _store(pg_client, hlc_table)
        await store.advance(HlcTimestamp(9_000, 3))
        await store.advance(HlcTimestamp(5_000, 0))  # lower → GREATEST keeps it
        await store.advance(HlcTimestamp(9_000, 3))  # equal → unchanged

        assert await store.load() == HlcTimestamp(9_000, 3)

    async def test_advance_is_durable_after_in_tx_commit(
        self, pg_client: PostgresClient, hlc_table: str
    ) -> None:
        store = _store(pg_client, hlc_table)
        tx = PostgresTxManagerAdapter(client=pg_client)

        async with tx.transaction():
            await store.advance(HlcTimestamp(1_700_000_000_000, 2))

        # Committed atomically with the (empty) business transaction.
        assert await store.load() == HlcTimestamp(1_700_000_000_000, 2)

    async def test_advance_reverts_on_business_rollback(
        self, pg_client: PostgresClient, hlc_table: str
    ) -> None:
        store = _store(pg_client, hlc_table)
        await store.advance(HlcTimestamp(4_000, 0))  # committed baseline

        tx = PostgresTxManagerAdapter(client=pg_client)

        with pytest.raises(RuntimeError, match="rollback"):
            async with tx.transaction():
                await store.advance(HlcTimestamp(9_000, 0))
                raise RuntimeError("rollback")

        # The in-transaction advance rolled back with the business tx — the mark never
        # advanced for rows that did not commit, so it stays at the committed baseline.
        assert await store.load() == HlcTimestamp(4_000, 0)

    async def test_load_returns_max_across_node_keys(
        self, pg_client: PostgresClient, hlc_table: str
    ) -> None:
        await _store(pg_client, hlc_table, node_key="a").advance(HlcTimestamp(1_000, 0))
        await _store(pg_client, hlc_table, node_key="b").advance(HlcTimestamp(2_000, 9))

        # A restart resumes above the whole deployment's emissions, not just one node's.
        loaded = await _store(pg_client, hlc_table, node_key="a").load()
        assert loaded == HlcTimestamp(2_000, 9)

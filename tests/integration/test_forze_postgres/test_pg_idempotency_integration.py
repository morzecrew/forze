"""Integration tests for the Postgres co-located idempotency store.

# covers: IdempotencyPort.begin
# covers: IdempotencyPort.commit
# covers: IdempotencyPort.fail

The headline is atomicity: ``commit`` runs on the caller's transaction connection, so the
result record and the business writes commit together — a rollback reverts the record
(no committed effect left uncached), a commit makes both durable.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest
from psycopg import sql

from forze.application.contracts.idempotency import IdempotencyRecord, IdempotencySpec
from forze.base.exceptions import CoreException
from forze_postgres.adapters.idempotency import PostgresIdempotencyStore
from forze_postgres.adapters.txmanager import PostgresTxManagerAdapter
from forze_postgres.execution.deps.configs import PostgresIdempotencyConfig
from forze_postgres.kernel.client import PostgresClient

# ----------------------- #


@pytest.fixture
async def idem_table(pg_client: PostgresClient) -> str:
    """Create a dedicated idempotency table and return its name."""

    table = f"idem_{uuid4().hex[:8]}"
    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {table} (
                op           TEXT        NOT NULL,
                idem_key     TEXT        NOT NULL,
                payload_hash TEXT        NOT NULL,
                status       TEXT        NOT NULL,
                result       BYTEA,
                expires_at   TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (op, idem_key)
            )
            """
        ).format(table=sql.Identifier("public", table))
    )
    return table


def _store(
    pg_client: PostgresClient,
    table: str,
    *,
    ttl: timedelta = timedelta(hours=1),
) -> PostgresIdempotencyStore:
    return PostgresIdempotencyStore(
        client=pg_client,
        spec=IdempotencySpec(name="idem", ttl=ttl),
        config=PostgresIdempotencyConfig(relation=("public", table)),
    )


# ....................... #


class TestPostgresIdempotency:
    async def test_commits_in_transaction_capability(
        self, pg_client: PostgresClient, idem_table: str
    ) -> None:
        assert _store(pg_client, idem_table).commits_in_transaction is True

    async def test_record_is_durable_after_in_tx_commit(
        self, pg_client: PostgresClient, idem_table: str
    ) -> None:
        store = _store(pg_client, idem_table)
        tx = PostgresTxManagerAdapter(client=pg_client)

        assert await store.begin("op", "k", "h") is None  # fresh claim -> pending

        async with tx.transaction():
            await store.commit("op", "k", "h", IdempotencyRecord(result=b"result-1"))

        # Committed atomically with the (empty) business transaction: replayed on a duplicate.
        record = await store.begin("op", "k", "h")
        assert record is not None
        assert record.result == b"result-1"

    async def test_record_reverts_on_business_rollback(
        self, pg_client: PostgresClient, idem_table: str
    ) -> None:
        store = _store(pg_client, idem_table)
        tx = PostgresTxManagerAdapter(client=pg_client)

        assert await store.begin("op", "k", "h") is None  # pending

        with pytest.raises(RuntimeError, match="rollback"):
            async with tx.transaction():
                await store.commit("op", "k", "h", IdempotencyRecord(result=b"r"))
                raise RuntimeError("rollback")

        # The in-transaction 'done' write rolled back with the business tx: the claim is
        # back to pending — no committed effect left uncached (crash window closed).
        with pytest.raises(CoreException):  # pending -> in progress
            await store.begin("op", "k", "h")

    async def test_in_progress_duplicate_conflicts(
        self, pg_client: PostgresClient, idem_table: str
    ) -> None:
        store = _store(pg_client, idem_table)

        assert await store.begin("op", "k", "h") is None

        with pytest.raises(CoreException):
            await store.begin("op", "k", "h")  # still pending

    async def test_payload_hash_mismatch_conflicts(
        self, pg_client: PostgresClient, idem_table: str
    ) -> None:
        store = _store(pg_client, idem_table)

        assert await store.begin("op", "k", "h1") is None

        with pytest.raises(CoreException):
            await store.begin("op", "k", "h2")  # same key, different payload

    async def test_fail_releases_claim_for_reexecution(
        self, pg_client: PostgresClient, idem_table: str
    ) -> None:
        store = _store(pg_client, idem_table)

        assert await store.begin("op", "k", "h") is None
        await store.fail("op", "k", "h")

        assert await store.begin("op", "k", "h") is None  # re-claimable

    async def test_expired_claim_is_reclaimable(
        self, pg_client: PostgresClient, idem_table: str
    ) -> None:
        store = _store(pg_client, idem_table)

        assert await store.begin("op", "k", "h") is None  # pending

        # Force the claim to be expired, then a fresh begin re-claims it (returns None).
        await pg_client.execute(
            sql.SQL(
                "UPDATE {table} SET expires_at = now() - interval '1 hour'"
            ).format(table=sql.Identifier("public", idem_table))
        )

        assert await store.begin("op", "k", "h") is None

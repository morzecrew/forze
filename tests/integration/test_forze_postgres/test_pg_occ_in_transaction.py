"""Optimistic-concurrency conflicts inside caller-owned Postgres transactions.

Two conflict classes behave differently inside a caller transaction:

- A stale-revision miss is detected client-side (``UPDATE … WHERE rev = X``
  matches zero rows). No server error was reported, the transaction is still
  healthy, and under READ COMMITTED the retry's re-read observes the
  competitor's committed row — so the gateway-level retry genuinely heals the
  conflict in place. This differs from Mongo by design (snapshot reads there
  can never observe fresher state).

- A server-reported conflict (serialization failure, deadlock, lock timeout)
  aborts the whole transaction: every further command fails with "current
  transaction is aborted, commands ignored until end of transaction block".
  A gateway-level retry can never succeed, so the original conflict must
  surface as a clean ``concurrency`` error for the caller to re-run the whole
  transaction scope.
"""

from uuid import UUID, uuid4

import pytest
import pytest_asyncio

pytest.importorskip("psycopg")

from forze.application.contracts.document import DocumentWriteTypes
from forze.application.execution import Deps
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.execution.deps.utils import doc_write_gw
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import (
    PostgresClient,
    PostgresConfig,
    PostgresTransactionOptions,
)
from forze_postgres.kernel.gateways.read import PostgresReadGateway
from forze_postgres.kernel.gateways.write import PostgresWriteGateway
from tests.support.execution_context import context_from_deps

# ----------------------- #


class OccTxDoc(Document):
    name: str


class OccTxCreate(CreateDocumentCmd):
    name: str


class OccTxUpdate(BaseDTO):
    name: str | None = None


def _write_types() -> DocumentWriteTypes[OccTxDoc, OccTxCreate, OccTxUpdate]:
    return DocumentWriteTypes(
        domain=OccTxDoc,
        create_cmd=OccTxCreate,
        update_cmd=OccTxUpdate,
    )


# ....................... #


def _dsn(postgres_container) -> str:
    url = postgres_container.get_connection_url()

    if url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql+psycopg://", "postgresql://")

    return url


@pytest_asyncio.fixture(scope="function")
async def competitor_client(postgres_container):
    """A second, independent client acting as the competing committed writer."""

    client = PostgresClient()
    await client.initialize(
        dsn=_dsn(postgres_container),
        config=PostgresConfig(min_size=1, max_size=1),
    )

    yield client

    await client.close()


# ....................... #


async def _make_write_gw(
    pg_client: PostgresClient,
) -> tuple[PostgresWriteGateway, str]:
    table = f"occ_tx_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )

    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )

    write = doc_write_gw(
        ctx,
        write_types=_write_types(),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )

    return write, table


async def _competitor_bump(client: PostgresClient, table: str, pk: UUID) -> None:
    """Commit a competing update (autocommit, separate connection)."""

    await client.execute(
        f"UPDATE public.{table} SET rev = rev + 1, name = 'competitor', "
        f"last_update_at = now() WHERE id = '{pk}'"
    )


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_serialization_failure_in_caller_tx_surfaces_original_concurrency(
    pg_client: PostgresClient,
    competitor_client: PostgresClient,
) -> None:
    """A server-reported serialization failure inside a caller transaction must
    surface as the original ``concurrency`` error — never the aborted-transaction
    error ("current transaction is aborted") produced by retrying on the dead
    transaction."""

    write, table = await _make_write_gw(pg_client)
    created = await write.create(OccTxCreate(name="initial"))

    caught: CoreException | None = None

    try:
        async with pg_client.transaction(
            options=PostgresTransactionOptions(isolation="repeatable_read"),
        ):
            # Pin the repeatable-read snapshot with a transactional read.
            await write.read_gw.get(created.id)

            # A competing writer commits a newer revision on its own connection.
            await _competitor_bump(competitor_client, table, created.id)

            # The snapshot still sees the old revision; the UPDATE targets a row
            # concurrently modified by a committed transaction, so the server
            # reports a serialization failure and aborts this transaction.
            await write.touch(created.id)

    except CoreException as error:
        caught = error

    assert caught is not None, "expected the stale write to fail"
    assert caught.kind is ExceptionKind.CONCURRENCY, (
        f"expected the original concurrency conflict, got {caught.kind}: {caught}"
    )
    assert "serialization" in str(caught).lower()
    assert "transaction is aborted" not in str(caught).lower()

    # The competitor's committed state stands.
    final = await write.read_gw.get(created.id)
    assert final.name == "competitor"
    assert final.rev == created.rev + 1

    # The client is fully usable afterwards: re-running the whole transaction
    # scope (the caller's retry contract for ``concurrency``) now succeeds.
    async with pg_client.transaction(
        options=PostgresTransactionOptions(isolation="repeatable_read"),
    ):
        retried = await write.touch(created.id)

    assert retried.rev == created.rev + 2


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stale_rev_in_caller_tx_retry_reads_fresh_state_and_succeeds(
    pg_client: PostgresClient,
    competitor_client: PostgresClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale-revision miss (zero rows matched, no server error) inside a caller
    transaction leaves the transaction healthy; the gateway retry re-reads the
    competitor's committed row under READ COMMITTED and succeeds in place."""

    write, table = await _make_write_gw(pg_client)
    created = await write.create(OccTxCreate(name="initial"))

    original_get = PostgresReadGateway.get
    read_calls = 0

    async def hooked_get(self, pk, **kwargs):
        nonlocal read_calls
        result = await original_get(self, pk, **kwargs)
        read_calls += 1

        if read_calls == 1:
            # Between the gateway's read (rev observed) and its UPDATE, a
            # competing writer commits a newer revision — the classic OCC race.
            await _competitor_bump(competitor_client, table, pk)

        return result

    monkeypatch.setattr(PostgresReadGateway, "get", hooked_get)

    async with pg_client.transaction():
        touched = await write.touch(created.id)

    # First attempt lost the race (zero rows: rev moved 1 -> 2); the in-place
    # retry re-read rev 2 and committed rev 3.
    assert read_calls == 2
    assert touched.rev == created.rev + 2

    final = await write.read_gw.get(created.id)
    assert final.rev == created.rev + 2
    assert final.name == "competitor"

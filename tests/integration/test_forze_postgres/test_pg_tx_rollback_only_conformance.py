"""Postgres ``rollback_only()`` against the shared conformance battery.

Run on a lazy and an eager client, since each opens its root on its own path in the client and
each path carries its own ``except Exception`` error log the quiet rollback must not reach.
Nested scopes are savepoints.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from psycopg import sql
from testcontainers.postgres import PostgresContainer

from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.kernel.client import PostgresConfig
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps
from tests.support.tx_rollback_only_conformance import (
    TX_ROLLBACK_ONLY_BATTERY,
    Check,
    RollbackOnlyHarness,
)

# ----------------------- #

_ROUTE = "postgres"


class _Boom(RuntimeError):
    pass


@pytest.fixture(params=["lazy", "eager"])
async def client(
    request: pytest.FixtureRequest, postgres_container: PostgresContainer
) -> AsyncIterator[PostgresClient]:
    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
    client = PostgresClient()
    config = PostgresConfig(min_size=1, max_size=3, lazy_transaction=request.param == "lazy")
    await client.initialize(dsn=url, config=config)

    yield client

    await client.close()


@pytest.fixture
async def harness(client: PostgresClient, caplog: pytest.LogCaptureFixture) -> RollbackOnlyHarness:
    table = sql.Identifier(f"rollback_only_{uuid4().hex[:12]}")
    await client.execute(sql.SQL("CREATE TABLE {} (k text PRIMARY KEY)").format(table))

    async def write(key: str) -> None:
        await client.execute(sql.SQL("INSERT INTO {} (k) VALUES (%s)").format(table), (key,))

    async def stored() -> set[str]:
        rows = await client.fetch_all(sql.SQL("SELECT k FROM {}").format(table))
        return {row["k"] for row in rows}  # type: ignore[call-overload]

    return RollbackOnlyHarness(
        ctx=context_from_deps(PostgresDepsModule(client=client, tx={_ROUTE})()),
        route=_ROUTE,
        write=write,
        stored=stored,
        savepoints=True,
        caplog=caplog,
        backend="postgres",
    )


@pytest.mark.conformance(plane="tx_rollback_only", engine="postgres")
@pytest.mark.parametrize("check", TX_ROLLBACK_ONLY_BATTERY, ids=lambda check: check.__name__)
async def test_tx_rollback_only_battery(check: Check, harness: RollbackOnlyHarness) -> None:
    await check(harness)


async def test_a_real_error_still_logs_at_error(harness: RollbackOnlyHarness) -> None:
    """The control: the client's error log is still there for a real error, and caplog sees it."""

    with pytest.raises(_Boom):
        async with harness.ctx.tx_ctx.scope(harness.route):
            await harness.write("doomed")
            raise _Boom

    errors = [r.getMessage() for r in harness.caplog.records if r.levelname == "ERROR"]

    assert any("rolling back" in message for message in errors), errors
    assert await harness.stored() == set()

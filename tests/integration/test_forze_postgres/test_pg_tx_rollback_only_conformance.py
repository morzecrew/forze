"""Postgres ``rollback_only()`` against the shared conformance battery.

Run on a lazy and an eager client, since each opens its root on its own path in the client and
each path carries its own ``except Exception`` error log the quiet rollback must not reach.
Nested scopes are savepoints.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import uuid4

import attrs
import pytest
from psycopg import sql
from testcontainers.postgres import PostgresContainer

from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze_kits.invariants import propose
from forze_postgres.adapters import PostgresTxManagerAdapter
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


async def test_propose_keeps_nothing_and_logs_no_error(harness: RollbackOnlyHarness) -> None:
    """``propose`` dry-runs through ``rollback_only()``, not a raise the client logs."""

    async def apply(_ctx: ExecutionContext) -> None:
        await harness.write("proposed")

    verdict = await propose(harness.ctx, apply, [], route=harness.route)

    errors = [r.getMessage() for r in harness.caplog.records if r.levelname == "ERROR"]

    assert verdict.holds
    assert errors == []
    assert await harness.stored() == set()


# ....................... #
# Routes on two clients: a scope on the other client's route has a transaction of its own.


@attrs.define(slots=True, kw_only=True, frozen=True)
class _TwoClients:
    ctx: ExecutionContext
    first: PostgresClient
    second: PostgresClient
    table: sql.Identifier

    async def write(self, client: PostgresClient, key: str) -> None:
        await client.execute(sql.SQL("INSERT INTO {} (k) VALUES (%s)").format(self.table), (key,))

    async def stored(self) -> set[str]:
        rows = await self.first.fetch_all(sql.SQL("SELECT k FROM {}").format(self.table))
        return {row["k"] for row in rows}  # type: ignore[call-overload]


@pytest.fixture
async def second_client(postgres_container: PostgresContainer) -> AsyncIterator[PostgresClient]:
    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
    client = PostgresClient()
    await client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=3))

    yield client

    await client.close()


@pytest.fixture
async def two(pg_client: PostgresClient, second_client: PostgresClient) -> _TwoClients:
    table = sql.Identifier(f"rollback_only_{uuid4().hex[:12]}")
    await pg_client.execute(sql.SQL("CREATE TABLE {} (k text PRIMARY KEY)").format(table))

    # "a" and "same" share the first client, as one deps module with tx={"a", "same"} does.
    managers = {
        "a": lambda _ctx: PostgresTxManagerAdapter(client=pg_client),
        "same": lambda _ctx: PostgresTxManagerAdapter(client=pg_client),
        "b": lambda _ctx: PostgresTxManagerAdapter(client=second_client),
    }
    ctx = context_from_deps(Deps.routed({TransactionManagerDepKey: managers}))

    return _TwoClients(ctx=ctx, first=pg_client, second=second_client, table=table)


async def test_a_dry_run_declared_first_covers_another_clients_route(two: _TwoClients) -> None:
    async with two.ctx.tx_ctx.scope("a") as tx:
        tx.rollback_only()
        await two.write(two.first, "first")

        async with two.ctx.tx_ctx.scope("b"):
            await two.write(two.second, "second")

    assert await two.stored() == set()


async def test_propose_rolls_back_a_scope_on_another_clients_route(two: _TwoClients) -> None:
    async def apply(ctx: ExecutionContext) -> None:
        await two.write(two.first, "first")

        async with ctx.tx_ctx.scope("b"):
            await two.write(two.second, "second")

    verdict = await propose(two.ctx, apply, [], route="a")

    assert verdict.holds
    assert await two.stored() == set()


async def test_a_dry_run_decided_after_another_client_committed_is_refused(
    two: _TwoClients,
) -> None:
    with pytest.raises(CoreException) as caught:
        async with two.ctx.tx_ctx.scope("a") as tx:
            await two.write(two.first, "first")

            async with two.ctx.tx_ctx.scope("b"):
                await two.write(two.second, "second")

            tx.rollback_only()

    assert caught.value.kind is ExceptionKind.PRECONDITION
    assert caught.value.code == "tx_rollback_after_independent_commit"
    # The other client's transaction committed at its own exit; the root rolled back.
    assert await two.stored() == {"second"}


async def test_a_route_on_the_same_client_joins_a_dry_run_decided_at_the_end(
    two: _TwoClients,
) -> None:
    async with two.ctx.tx_ctx.scope("a") as tx:
        await two.write(two.first, "first")

        async with two.ctx.tx_ctx.scope("same"):
            await two.write(two.first, "same-client")

        tx.rollback_only()

    assert await two.stored() == set()

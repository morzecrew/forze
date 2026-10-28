"""Locking reads on a document whose read relation is a ``LEFT JOIN`` view.

Postgres refuses ``FOR UPDATE`` on the nullable side of an outer join, so the read gateway
locks the row on the write table and reads the view afterwards. Each test checks the lock
from a second session, which is the only thing that can tell a lock was really taken.
"""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID, uuid4

import pytest

psycopg = pytest.importorskip("psycopg")

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.contracts.transaction.deps import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.execution.deps import ConfigurablePostgresDocument, postgres_txmanager
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

# ----------------------- #


class _Doc(Document):
    tenant_id: UUID | None = None
    title: str
    warehouse_id: UUID | None = None
    parent_id: UUID | None = None


class _Create(CreateDocumentCmd):
    title: str
    warehouse_id: UUID | None = None
    parent_id: UUID | None = None


class _Update(BaseDTO):
    title: str | None = None


class _Read(ReadDocument):
    title: str
    warehouse: str | None = None
    parent_title: str | None = None


_SPEC = DocumentSpec(
    name="view_lock_ns",
    read=_Read,
    write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
)


# ....................... #


async def _setup(
    pg_client: PostgresClient,
    *,
    tenant_aware: bool = False,
) -> tuple[ExecutionContext, str, UUID]:
    """Create the write table, a warehouse table and the ``LEFT JOIN`` read view.

    :returns: The context, the write table's name and a warehouse id.
    """

    suffix = uuid4().hex[:12]
    table, warehouses, view = f"vl_{suffix}", f"vl_w_{suffix}", f"vl_v_{suffix}"
    tenant_col = "tenant_id uuid NOT NULL," if tenant_aware else "tenant_id uuid,"
    warehouse = uuid4()

    await pg_client.execute(
        f"""
        CREATE TABLE {warehouses} (id uuid PRIMARY KEY, name text NOT NULL);
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            {tenant_col}
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            warehouse_id uuid,
            parent_id uuid
        );
        CREATE VIEW {view} AS
            SELECT d.*, w.name AS warehouse, p.title AS parent_title
            FROM {table} d
            LEFT JOIN {warehouses} w ON w.id = d.warehouse_id
            LEFT JOIN {table} p ON p.id = d.parent_id;
        INSERT INTO {warehouses} (id, name) VALUES ('{warehouse}', 'north');
        """
    )

    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", view),
            write=("public", table),
            bookkeeping_strategy="application",
            tenant_aware=tenant_aware,
        )
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: doc,
                DocumentCommandDepKey: doc,
            }
        ).merge(Deps.routed({TransactionManagerDepKey: {"main": postgres_txmanager}}))
    )

    return ctx, table, warehouse


def _dsn(postgres_container: Any) -> str:
    return postgres_container.get_connection_url().replace(
        "postgresql+psycopg://", "postgresql://"
    )


async def _held_by_someone(dsn: str, table: str, pk: UUID) -> bool:
    """Whether a second session finds the row on *table* locked."""

    other = await psycopg.AsyncConnection.connect(dsn, connect_timeout=5)

    async with other:
        try:
            async with other.transaction():
                await other.execute(
                    f"SELECT id FROM {table} WHERE id = %s FOR UPDATE NOWAIT",  # type: ignore[arg-type]
                    [pk],
                )
        except psycopg.errors.LockNotAvailable:
            return True

    return False


async def _wait_until_blocked(dsn: str, table: str) -> None:
    """Return once a backend waits on a lock in a statement naming *table*."""

    watcher = await psycopg.AsyncConnection.connect(dsn, connect_timeout=5, autocommit=True)

    async with watcher:
        for _ in range(200):
            cur = await watcher.execute(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE wait_event_type = 'Lock' AND query LIKE %s",
                [f"%{table}%"],
            )
            row = await cur.fetchone()

            if row is not None and row[0] > 0:
                return

            await asyncio.sleep(0.02)

    raise AssertionError("the locking read never blocked")


# ....................... #


@pytest.mark.parametrize("read", ["get", "find", "find_by_view_column", "project"])
async def test_a_locking_read_locks_the_write_row(
    pg_client: PostgresClient,
    postgres_container: Any,
    read: str,
) -> None:
    ctx, table, warehouse = await _setup(pg_client)
    cmd = ctx.doc.command(_SPEC)
    parent = await cmd.create(_Create(title="parent"))
    row = await cmd.create(_Create(title="child", warehouse_id=warehouse, parent_id=parent.id))
    await cmd.create(_Create(title="orphan"))

    async with ctx.tx_ctx.scope("main"):
        query = ctx.doc.query(_SPEC)

        match read:
            case "get":
                got: Any = await query.get(row.id, for_update=True)
            case "find":
                got = await query.find({"$values": {"title": "child"}}, for_update=True)
            case "find_by_view_column":
                got = await query.find({"$values": {"warehouse": "north"}}, for_update=True)
            case _:
                got = await query.project(
                    {"$values": {"title": "child"}},
                    ("id", "warehouse", "parent_title"),
                    for_update=True,
                )

        if isinstance(got, dict):
            assert got == {"id": row.id, "warehouse": "north", "parent_title": "parent"}
        else:
            assert (got.id, got.warehouse, got.parent_title) == (row.id, "north", "parent")

        assert await _held_by_someone(_dsn(postgres_container), table, row.id)
        assert not await _held_by_someone(_dsn(postgres_container), table, parent.id)

    assert not await _held_by_someone(_dsn(postgres_container), table, row.id)


async def test_a_locking_read_without_a_match_locks_nothing(pg_client: PostgresClient) -> None:
    ctx, _, _ = await _setup(pg_client)

    async with ctx.tx_ctx.scope("main"):
        query = ctx.doc.query(_SPEC)

        assert await query.find({"$values": {"title": "nobody"}}, for_update=True) is None

        with pytest.raises(CoreException) as caught:
            await query.get(uuid4(), for_update=True)

        assert caught.value.kind is ExceptionKind.NOT_FOUND


@pytest.mark.parametrize("read", ["get", "find"])
async def test_nowait_on_a_held_row_is_a_concurrency_error(
    pg_client: PostgresClient,
    postgres_container: Any,
    read: str,
) -> None:
    ctx, table, _ = await _setup(pg_client)
    row = await ctx.doc.command(_SPEC).create(_Create(title="held"))
    other = await psycopg.AsyncConnection.connect(_dsn(postgres_container), connect_timeout=5)

    async with other, other.transaction():
        await other.execute(f"SELECT id FROM {table} WHERE id = %s FOR UPDATE", [row.id])  # type: ignore[arg-type]

        with pytest.raises(CoreException) as caught:
            async with ctx.tx_ctx.scope("main"):
                query = ctx.doc.query(_SPEC)

                if read == "get":
                    await query.get(row.id, for_update="nowait")
                else:
                    await query.find({"$values": {"title": "held"}}, for_update="nowait")

        assert caught.value.kind is ExceptionKind.CONCURRENCY


async def test_skip_locked_passes_over_a_held_row(
    pg_client: PostgresClient,
    postgres_container: Any,
) -> None:
    ctx, table, _ = await _setup(pg_client)
    cmd = ctx.doc.command(_SPEC)
    held = await cmd.create(_Create(title="job"))
    free = await cmd.create(_Create(title="job"))
    other = await psycopg.AsyncConnection.connect(_dsn(postgres_container), connect_timeout=5)

    async with other, other.transaction():
        await other.execute(f"SELECT id FROM {table} WHERE id = %s FOR UPDATE", [held.id])  # type: ignore[arg-type]

        async with ctx.tx_ctx.scope("main"):
            query = ctx.doc.query(_SPEC)

            got = await query.find({"$values": {"title": "job"}}, for_update="skip_locked")
            assert got is not None and got.id == free.id

            with pytest.raises(CoreException) as caught:
                await query.get(held.id, for_update="skip_locked")

            assert caught.value.kind is ExceptionKind.NOT_FOUND


async def test_a_writer_on_the_base_row_waits_for_the_lock(
    pg_client: PostgresClient,
    postgres_container: Any,
) -> None:
    ctx, table, _ = await _setup(pg_client)
    row = await ctx.doc.command(_SPEC).create(_Create(title="before"))
    writer = await psycopg.AsyncConnection.connect(
        _dsn(postgres_container), connect_timeout=5, autocommit=True
    )

    async with writer:
        await writer.execute("SET lock_timeout = '200ms'")

        async with ctx.tx_ctx.scope("main"):
            await ctx.doc.query(_SPEC).get(row.id, for_update=True)

            with pytest.raises(psycopg.errors.LockNotAvailable):
                await writer.execute(
                    f"UPDATE {table} SET title = 'after' WHERE id = %s",  # type: ignore[arg-type]
                    [row.id],
                )

        await writer.execute(f"UPDATE {table} SET title = 'after' WHERE id = %s", [row.id])  # type: ignore[arg-type]


async def test_a_read_that_waited_for_its_lock_returns_the_committed_row(
    pg_client: PostgresClient,
    postgres_container: Any,
) -> None:
    """The view is read after the lock is held, so a commit made while waiting is visible."""

    ctx, table, _ = await _setup(pg_client)
    row = await ctx.doc.command(_SPEC).create(_Create(title="before"))
    dsn = _dsn(postgres_container)
    other = await psycopg.AsyncConnection.connect(dsn, connect_timeout=5)

    async def locking_get() -> _Read:
        async with ctx.tx_ctx.scope("main"):
            return await ctx.doc.query(_SPEC).get(row.id, for_update=True)

    async with other:
        async with other.transaction():
            await other.execute(f"UPDATE {table} SET title = 'after' WHERE id = %s", [row.id])  # type: ignore[arg-type]
            task = asyncio.create_task(locking_get())
            await _wait_until_blocked(dsn, table)

        got = await task

    assert got.title == "after"


async def test_a_row_that_stops_matching_while_awaited_is_skipped(
    pg_client: PostgresClient,
    postgres_container: Any,
) -> None:
    """The locked row no longer matches once committed, so the next match is locked instead."""

    ctx, table, _ = await _setup(pg_client)
    cmd = ctx.doc.command(_SPEC)
    first = await cmd.create(_Create(title="pending"))
    second = await cmd.create(_Create(title="idle"))
    dsn = _dsn(postgres_container)
    other = await psycopg.AsyncConnection.connect(dsn, connect_timeout=5)

    async def claim() -> tuple[_Read | None, bool]:
        async with ctx.tx_ctx.scope("main"):
            got = await ctx.doc.query(_SPEC).find({"$values": {"title": "pending"}}, for_update=True)

            return got, await _held_by_someone(dsn, table, first.id)

    async with other:
        async with other.transaction():
            await other.execute(f"UPDATE {table} SET title = 'done' WHERE id = %s", [first.id])  # type: ignore[arg-type]
            await other.execute(
                f"UPDATE {table} SET title = 'pending' WHERE id = %s",  # type: ignore[arg-type]
                [second.id],
            )
            task = asyncio.create_task(claim())
            await _wait_until_blocked(dsn, table)

        got, first_still_held = await task

    assert got is not None and (got.id, got.title) == (second.id, "pending")
    # Postgres keeps the lock on a row that failed its recheck, and so does this read.
    assert first_still_held


async def test_a_tenant_scoped_locking_read_never_reaches_another_tenant(
    pg_client: PostgresClient,
    postgres_container: Any,
) -> None:
    ctx, table, _ = await _setup(pg_client, tenant_aware=True)
    tenants = uuid4(), uuid4()

    def bind(tenant: UUID) -> Any:
        return ctx.inv_ctx.bind(
            metadata=InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4()),
            authn=AuthnIdentity(principal_id=uuid4()),
            tenant=TenantIdentity(tenant_id=tenant),
        )

    rows = []

    for tenant in tenants:
        with bind(tenant):
            rows.append(await ctx.doc.command(_SPEC).create(_Create(title="shared")))

    with bind(tenants[0]):
        async with ctx.tx_ctx.scope("main"):
            query = ctx.doc.query(_SPEC)

            got = await query.find({"$values": {"title": "shared"}}, for_update=True)
            assert got is not None and got.id == rows[0].id

            with pytest.raises(CoreException) as caught:
                await query.get(rows[1].id, for_update=True)

            assert caught.value.kind is ExceptionKind.NOT_FOUND
            assert await _held_by_someone(_dsn(postgres_container), table, rows[0].id)
            assert not await _held_by_someone(_dsn(postgres_container), table, rows[1].id)

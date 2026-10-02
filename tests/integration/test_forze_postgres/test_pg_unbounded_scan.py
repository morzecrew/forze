"""A read with no limit on Postgres returns every row once, ties in id order."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.execution import Deps
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from forze_postgres.kernel.gateways.read import PostgresReadGateway
from tests.integration.test_forze_postgres._document_fixtures import document_context
from tests.support.execution_context import context_from_deps
from tests.support.unbounded_scan_parity import (
    POSTGRES_COLUMNS,
    ScanCreate,
    ScanDoc,
    ScanRead,
    run_id_first_cursor_parity,
    run_unbounded_scan_parity,
    seed,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _context(pg_client: PostgresClient, table: str, read_validation: Any) -> Any:
    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
            read_validation=read_validation,
        )
    )

    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: doc,
                DocumentCommandDepKey: doc,
            }
        )
    )


@pytest.mark.parametrize("read_validation", ["strict", "trusted"])
async def test_a_read_without_a_limit_orders_ties_by_id(
    pg_client: PostgresClient, read_validation: str
) -> None:
    # Trusted decoding refuses columns a model does not declare: a seek read's extra
    # sort-key columns must not reach it.
    table = f"scan_{uuid4().hex[:12]}"
    await pg_client.execute(f"CREATE TABLE {table} ({POSTGRES_COLUMNS});")
    spec = DocumentSpec(
        name="scan",
        read=ScanRead,
        write=DocumentWriteTypes(domain=ScanDoc, create_cmd=ScanCreate),
    )
    ctx = _context(pg_client, table, read_validation)

    await run_unbounded_scan_parity(ctx.document.command(spec), ctx.document.query(spec))


async def test_a_cursor_sorted_by_id_first_orders_by_id(pg_client: PostgresClient) -> None:
    table = f"scan_{uuid4().hex[:12]}"
    await pg_client.execute(f"CREATE TABLE {table} ({POSTGRES_COLUMNS});")
    spec = DocumentSpec(
        name="scan",
        read=ScanRead,
        write=DocumentWriteTypes(domain=ScanDoc, create_cmd=ScanCreate),
    )
    ctx = document_context(pg_client, table)

    await run_id_first_cursor_parity(ctx.document.command(spec), ctx.document.query(spec))


@pytest.mark.parametrize("action", ["update", "delete"])
@pytest.mark.parametrize("read", ["scan", "cursor"])
async def test_a_row_changed_between_pages_costs_no_other_row(
    pg_client: PostgresClient,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
    read: str,
) -> None:
    # Right after a page's statement, its last row moves to a later group or goes away. The
    # next page must continue from where that statement left it: no other row may be lost,
    # and a vanished row must not end the read.
    table = f"scan_{uuid4().hex[:12]}"
    await pg_client.execute(f"CREATE TABLE {table} ({POSTGRES_COLUMNS});")
    spec = DocumentSpec(
        name="scan",
        read=ScanRead,
        write=DocumentWriteTypes(domain=ScanDoc, create_cmd=ScanCreate),
    )
    ctx = document_context(pg_client, table)
    created = await ctx.document.command(spec).create_many(seed())
    query = ctx.document.query(spec)
    batch = 200 if read == "scan" else 50
    original = PostgresReadGateway._read_all  # pyright: ignore[reportPrivateUsage]
    changed: list[Any] = []

    async def changing(self: Any, *args: Any, **kwargs: Any) -> Any:
        rows = await original(self, *args, **kwargs)

        if not changed and len(rows) == batch + 1:
            edge = rows[-2]["id"]  # the page's last row; the extra one only signals more
            changed.append(edge)
            statement = "UPDATE {} SET grp = 2 WHERE id = %s" if action == "update" else (
                "DELETE FROM {} WHERE id = %s"
            )
            await pg_client.execute(statement.format(table), [edge])

        return rows

    monkeypatch.setattr(PostgresReadGateway, "_read_all", changing)

    if read == "scan":
        ids = [hit.id for hit in (await query.find_many(sorts={"grp": "asc"})).hits]
    else:
        ids = []
        cursor: dict[str, Any] = {"limit": batch}

        while True:
            page = await query.find_cursor(sorts={"grp": "asc"}, cursor=cursor)
            ids += [hit.id for hit in page.hits]

            if not page.has_more:
                break

            cursor = {"limit": batch, "after": page.next_cursor}

    survivors = {row.id for row in created} - (set(changed) if action == "delete" else set())

    assert changed, "the read never reached a second page"
    assert survivors <= set(ids), f"{len(survivors - set(ids))} rows lost"

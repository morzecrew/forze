"""Postgres finds a resent unchanged instant unchanged against the aware ``timestamptz`` it
reads back: on the domain path, set-based, and with revision history."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.integration.test_forze_postgres._document_fixtures import document_context
from tests.support.document_unchanged_instant import (
    InstantCreate,
    InstantDoc,
    InstantRead,
    InstantUpdate,
    assert_a_batch_writes_only_the_rows_it_changes,
    assert_a_stale_resend_keeps_a_concurrent_change,
    assert_an_unchanged_instant_is_not_a_change,
)
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _table(pg_client: PostgresClient) -> str:
    t = f"pg_instant_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            at timestamptz NOT NULL
        );
        """
    )
    return t


def _spec(*, history_enabled: bool = False) -> DocumentSpec:
    return DocumentSpec(
        name="instant",
        read=InstantRead,
        write=DocumentWriteTypes(
            domain=InstantDoc, create_cmd=InstantCreate, update_cmd=InstantUpdate
        ),
        history_enabled=history_enabled,
    )


async def test_an_unchanged_instant_is_not_a_change(pg_client: PostgresClient) -> None:
    ctx = document_context(pg_client, await _table(pg_client))

    await assert_an_unchanged_instant_is_not_a_change(
        ctx.document.command(_spec()), ctx.document.query(_spec())
    )


async def _history_ctx(pg_client: PostgresClient) -> tuple[ExecutionContext, str]:
    """A context whose document keeps revision history, and the history table's name."""

    t = await _table(pg_client)
    hist = f"{t}_h"
    await pg_client.execute(
        f"""
        CREATE TABLE {hist} (
            source text NOT NULL,
            id uuid NOT NULL,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            data jsonb NOT NULL,
            PRIMARY KEY (source, id, rev)
        );
        """
    )
    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", t),
            write=("public", t),
            history=("public", hist),
            bookkeeping_strategy="application",
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
        )
    )

    return ctx, hist


async def test_an_unchanged_instant_writes_no_history(pg_client: PostgresClient) -> None:
    ctx, hist = await _history_ctx(pg_client)
    spec = _spec(history_enabled=True)

    created = await assert_an_unchanged_instant_is_not_a_change(
        ctx.document.command(spec), ctx.document.query(spec), set_based=False
    )

    # One row per create and one for the move at the end: nothing for the resends.
    count = await pg_client.fetch_value(f"SELECT COUNT(*) FROM {hist}", [])
    assert count == len(created) + 1


async def test_a_stale_resend_keeps_a_concurrent_change(pg_client: PostgresClient) -> None:
    ctx, _ = await _history_ctx(pg_client)
    spec = _spec(history_enabled=True)

    await assert_a_stale_resend_keeps_a_concurrent_change(
        ctx.document.command(spec), ctx.document.query(spec)
    )


async def test_a_batch_writes_only_the_rows_it_changes(pg_client: PostgresClient) -> None:
    ctx, hist = await _history_ctx(pg_client)
    spec = _spec(history_enabled=True)

    async def history_rows(pk: UUID) -> int:
        return int(await pg_client.fetch_value(f"SELECT COUNT(*) FROM {hist} WHERE id = %s", [pk]))

    await assert_a_batch_writes_only_the_rows_it_changes(
        ctx.document.command(spec), ctx.document.query(spec), history_rows
    )

"""Postgres finds a resent unchanged instant unchanged against the aware ``timestamptz`` it
reads back: on the domain path, set-based, and with revision history. A column that keeps
fewer fractional digits rounds the instant, and a resend finer than it is no change either."""

from __future__ import annotations

from datetime import UTC, datetime, time, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from forze_postgres.kernel.gateways.write import _rounded_as_stored
from tests.integration.test_forze_postgres._document_fixtures import document_context
from tests.support.document_unchanged_instant import (
    AT_UTC,
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

at_types = pytest.mark.parametrize("at_type", ["timestamptz", "timestamptz(3)", "timestamp(3)"])
"""The instant's column: full precision, and milliseconds, aware and naive."""


async def _table(pg_client: PostgresClient, at_type: str = "timestamptz") -> str:
    t = f"pg_instant_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            at {at_type} NOT NULL
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


@at_types
async def test_an_unchanged_instant_is_not_a_change(
    pg_client: PostgresClient, at_type: str
) -> None:
    ctx = document_context(pg_client, await _table(pg_client, at_type))

    await assert_an_unchanged_instant_is_not_a_change(
        ctx.document.command(_spec()), ctx.document.query(_spec())
    )


async def _history_ctx(pg_client: PostgresClient, at_type: str) -> tuple[ExecutionContext, str]:
    """A context whose document keeps revision history, and the history table's name."""

    t = await _table(pg_client, at_type)
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


@at_types
async def test_an_unchanged_instant_writes_no_history(
    pg_client: PostgresClient, at_type: str
) -> None:
    ctx, hist = await _history_ctx(pg_client, at_type)
    spec = _spec(history_enabled=True)

    created = await assert_an_unchanged_instant_is_not_a_change(
        ctx.document.command(spec), ctx.document.query(spec), set_based=False
    )

    # One row per create and one for the move at the end: nothing for the resends.
    count = await pg_client.fetch_value(f"SELECT COUNT(*) FROM {hist}", [])
    assert count == len(created) + 1


@at_types
async def test_a_stale_resend_keeps_a_concurrent_change(
    pg_client: PostgresClient, at_type: str
) -> None:
    ctx, _ = await _history_ctx(pg_client, at_type)
    spec = _spec(history_enabled=True)

    await assert_a_stale_resend_keeps_a_concurrent_change(
        ctx.document.command(spec), ctx.document.query(spec)
    )


@at_types
async def test_a_batch_writes_only_the_rows_it_changes(
    pg_client: PostgresClient, at_type: str
) -> None:
    ctx, hist = await _history_ctx(pg_client, at_type)
    spec = _spec(history_enabled=True)

    async def history_rows(pk: UUID) -> int:
        return int(await pg_client.fetch_value(f"SELECT COUNT(*) FROM {hist} WHERE id = %s", [pk]))

    await assert_a_batch_writes_only_the_rows_it_changes(
        ctx.document.command(spec), ctx.document.query(spec), history_rows
    )


_PLUS_3 = timezone(timedelta(hours=3))

_SENT: dict[str, list[datetime] | list[time]] = {
    "timestamp": [
        datetime(2026, 1, 1, 12, 0, 0, us)
        for us in (123456, 123500, 123499, 123450, 999500, 500000)
    ]
    + [datetime(1999, 12, 31, 23, 59, 59, us) for us in (123500, 123501, 999500, 500000, 123450)]
    + [datetime(2000, 1, 1, 0, 0, 0, 500), datetime(1969, 7, 20, 20, 17, 40, 5)],
    "timestamptz": [
        datetime(2026, 1, 1, 12, 0, 0, 123500, tzinfo=UTC),
        datetime(1999, 12, 31, 23, 59, 59, 123500, tzinfo=UTC),
        # Before the epoch in UTC, after it on the wall clock.
        datetime(2000, 1, 1, 2, 0, 0, 500, tzinfo=_PLUS_3),
        datetime(2000, 1, 1, 2, 0, 0, 123450, tzinfo=_PLUS_3),
    ],
    "time": [time(12, 0, 0, us) for us in (123456, 123500, 123499, 123450, 999500, 500000)],
    "timetz": [time(12, 0, 0, 123500, tzinfo=_PLUS_3), time(0, 0, 0, 500, tzinfo=UTC)],
}
"""Values at and around half a step, after and before Postgres's epoch, 2000-01-01."""


@pytest.mark.parametrize("digits", range(6))
async def test_an_instant_rounds_as_postgres_stores_it(
    pg_client: PostgresClient, digits: int
) -> None:
    for column, sent in _SENT.items():
        for value in sent:
            stored = await pg_client.fetch_value(f"SELECT %s::{column}({digits})", [value])

            assert _rounded_as_stored(value, digits) == stored, (column, value)

        # An array, element by element.
        stored = await pg_client.fetch_value(f"SELECT %s::{column}({digits})[]", [sent])
        assert [_rounded_as_stored(x, digits) for x in sent] == stored, column


class _Instants(BaseModel):
    ats: list[datetime]


class InstantsCreate(CreateDocumentCmd, _Instants):
    pass


class InstantsDoc(Document, _Instants):
    pass


class InstantsRead(ReadDocument, _Instants):
    pass


class InstantsUpdate(BaseDTO):
    ats: list[datetime] | None = None


async def test_an_unchanged_instant_array_is_not_a_change(pg_client: PostgresClient) -> None:
    t = f"pg_instants_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            ats timestamptz(3)[] NOT NULL
        );
        """
    )
    spec = DocumentSpec(
        name="instants",
        read=InstantsRead,
        write=DocumentWriteTypes(
            domain=InstantsDoc, create_cmd=InstantsCreate, update_cmd=InstantsUpdate
        ),
    )
    ctx = document_context(pg_client, t)
    command, query = ctx.document.command(spec), ctx.document.query(spec)
    ats = [AT_UTC, AT_UTC + timedelta(days=1)]

    doc = await command.create(InstantsCreate(ats=ats))
    _, diff = await command.update(doc.id, doc.rev, InstantsUpdate(ats=ats), return_diff=True)
    stored = await query.get(doc.id, skip_cache=True)

    assert (diff, stored.rev) == ({}, doc.rev)

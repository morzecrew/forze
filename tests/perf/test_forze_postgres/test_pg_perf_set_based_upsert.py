"""Set-based ``upsert_many`` on Postgres: a fresh load into an empty table and a reload of the
same rows, unchanged.

Four call shapes: a mirror per source document of a few dozen rows, the shape of a bulk import
that upserts what each document produces, a call of one default chunk, a call of a few thousand
rows, and one call of every row. A mixed load adds a fresh load into a table that already holds
an eighth of the rows, unchanged, as a mirror's later revisions of a document do: there every
set-based chunk runs all four of its statements. Each round starts from the state it measures (an empty table for a fresh load,
the loaded rows for a reload) and times only the upserts; the payloads are built before it.

Each shape runs set-based (``set_based``) and as the statement issue #792 measures it against
(``hand_written``): one ``INSERT … ON CONFLICT DO UPDATE`` per call, its rows dumped to one JSON
parameter, nothing built through the domain model or adapted per value. The gap between the two
is what the set-based path spends on correctness and on its extra statements.

The database is a local container, so a round trip costs far less here than against a remote
one: these numbers weigh client-side encoding more, and each chunk's three or four statements
less, than a deployment would.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from anyio import from_thread, to_thread
from anyio.lowlevel import current_token
from pydantic import Field

pytest.importorskip("psycopg")

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
    UpsertItem,
)
from forze.application.execution import Deps
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

_ROWS = 4_000
_ROUNDS = 10
_SHAPES = [
    pytest.param(40, 200, id="40_per_call"),
    pytest.param(200, 200, id="200_per_call"),
    pytest.param(2_000, 1_000, id="2000_per_call"),
    pytest.param(_ROWS, 200, id="4000_per_call"),
]
_STORED_SHARE = 8
"""One row in this many is already stored when the mixed load runs."""
_WAYS = ["set_based", "hand_written"]


class _Fields(BaseDTO):
    source: str
    seq: int
    name: str
    amount: float
    active: bool
    happened_at: datetime
    note: str | None = None
    code: str | None = None
    tags: list[str] = Field(default_factory=list)
    qty: int = 0


class _Create(_Fields):
    pass


class _Doc(Document, _Fields):
    pass


class _Read(ReadDocument, _Fields):
    pass


class _Update(BaseDTO):
    name: str | None = None
    amount: float | None = None


_SPEC = DocumentSpec(
    name="perf_set_based_upsert",
    read=_Read,
    write=DocumentWriteTypes(domain=_Doc, create_cmd=_Create, update_cmd=_Update),
)


def _items(ids: Sequence[Any]) -> list[UpsertItem[_Create, Any]]:
    at = datetime(2026, 1, 1, tzinfo=UTC)

    return [
        UpsertItem.mirror(
            pk,
            _Create(
                source="src",
                seq=i,
                name=f"name {i}",
                amount=i * 1.5,
                active=bool(i % 2),
                happened_at=at,
                note=None if i % 3 else f"note {i}",
                code=f"c{i}",
                tags=["a", "b"],
                qty=i,
            ),
        )
        for i, pk in enumerate(ids)
    ]


async def _command(pg_client: PostgresClient, batch_size: int) -> tuple[Any, str]:
    table = f"perf_upsert_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            source text NOT NULL,
            seq integer NOT NULL,
            name text NOT NULL,
            amount double precision NOT NULL,
            active boolean NOT NULL,
            happened_at timestamptz NOT NULL,
            note text,
            code text,
            tags jsonb NOT NULL,
            qty integer NOT NULL
        );
        """
    )
    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
            batch_size=batch_size,
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
    return ctx.document.command(_SPEC), table


def _loader(
    command: Any, pg_client: PostgresClient, table: str, per_call: int, way: str
) -> Callable[[Sequence[Any]], Awaitable[None]]:
    async def set_based(items: Sequence[Any]) -> None:
        for offset in range(0, len(items), per_call):
            await command.upsert_many(
                items[offset : offset + per_call], return_new=False, set_based=True
            )

    # Every item sets every field, so each call is one group of rows writing the same fields.
    fields = list(_Fields.model_fields)
    columns = ", ".join(["id", "rev", "created_at", "last_update_at", *fields])
    excluded = ", ".join(f"excluded.{f}" for f in fields)
    stmt = (
        f"INSERT INTO {table} AS t ({columns}) SELECT {columns} "
        f"FROM jsonb_populate_recordset(NULL::{table}, %s::jsonb) ORDER BY id "
        f"ON CONFLICT (id) DO UPDATE SET ({', '.join(fields)}) = ROW({excluded}), "
        "last_update_at = now(), rev = t.rev + 1 "
        f"WHERE ROW({', '.join(f't.{f}' for f in fields)}) IS DISTINCT FROM ROW({excluded})"
    )

    async def hand_written(items: Sequence[Any]) -> None:
        for offset in range(0, len(items), per_call):
            now = datetime.now(UTC).isoformat()
            rows = [
                {
                    "id": str(item.id),
                    "rev": 1,
                    "created_at": now,
                    "last_update_at": now,
                    **item.create.model_dump(mode="json"),
                }
                for item in items[offset : offset + per_call]
            ]

            async with pg_client.transaction():
                await pg_client.execute(stmt, [json.dumps(rows)])

    return set_based if way == "set_based" else hand_written


async def _pedantic(
    benchmark: Any,
    load: Callable[[Sequence[Any]], Awaitable[None]],
    setup: Callable[[], Awaitable[Sequence[Any]]],
) -> None:
    """Time *load* over :data:`_ROUNDS` rounds, each on what *setup* returns (untimed)."""

    token = current_token()

    def target(items: Sequence[Any]) -> None:
        from_thread.run(load, items, token=token)

    def prepare() -> tuple[tuple[Sequence[Any]], dict[str, Any]]:
        return (from_thread.run(setup, token=token),), {}

    await to_thread.run_sync(
        lambda: benchmark.pedantic(target, setup=prepare, rounds=_ROUNDS, warmup_rounds=1)
    )


@pytest.mark.perf
@pytest.mark.asyncio
@pytest.mark.parametrize("way", _WAYS)
@pytest.mark.parametrize(("per_call", "batch_size"), _SHAPES)
async def test_pg_set_based_upsert_fresh_load_benchmark(
    benchmark: Any, pg_client: PostgresClient, per_call: int, batch_size: int, way: str
) -> None:
    command, table = await _command(pg_client, batch_size)

    async def setup() -> Sequence[Any]:
        await pg_client.execute(f"TRUNCATE {table}")
        return _items([uuid4() for _ in range(_ROWS)])

    await _pedantic(benchmark, _loader(command, pg_client, table, per_call, way), setup)
    assert (await pg_client.fetch_one(f"SELECT count(*) AS n FROM {table}", [])) == {"n": _ROWS}


@pytest.mark.perf
@pytest.mark.asyncio
@pytest.mark.parametrize("way", _WAYS)
@pytest.mark.parametrize(("per_call", "batch_size"), _SHAPES)
async def test_pg_set_based_upsert_reload_benchmark(
    benchmark: Any, pg_client: PostgresClient, per_call: int, batch_size: int, way: str
) -> None:
    command, table = await _command(pg_client, batch_size)
    items = _items([uuid4() for _ in range(_ROWS)])
    load = _loader(command, pg_client, table, per_call, way)
    await load(items)

    async def setup() -> Sequence[Any]:
        return items

    await _pedantic(benchmark, load, setup)
    # Unchanged rows are not written: every one is still at its first revision.
    assert (await pg_client.fetch_one(f"SELECT max(rev) AS rev FROM {table}", [])) == {"rev": 1}


@pytest.mark.perf
@pytest.mark.asyncio
@pytest.mark.parametrize("way", _WAYS)
@pytest.mark.parametrize(("per_call", "batch_size"), _SHAPES)
async def test_pg_set_based_upsert_mixed_load_benchmark(
    benchmark: Any, pg_client: PostgresClient, per_call: int, batch_size: int, way: str
) -> None:
    command, table = await _command(pg_client, batch_size)
    load = _loader(command, pg_client, table, per_call, way)

    async def setup() -> Sequence[Any]:
        await pg_client.execute(f"TRUNCATE {table}")
        items = _items([uuid4() for _ in range(_ROWS)])
        await load(items[::_STORED_SHARE])
        return items

    await _pedantic(benchmark, load, setup)
    assert (await pg_client.fetch_one(f"SELECT count(*) AS n FROM {table}", [])) == {"n": _ROWS}
    assert (await pg_client.fetch_one(f"SELECT max(rev) AS rev FROM {table}", [])) == {"rev": 1}

"""Integration: ``ChildCollectionSync`` against real Postgres unique constraints (RFC 0050).

The in-memory store enforces a declared ``UniqueTogether``; these tests hold the component to a
real index instead, declared nowhere in the spec. A new child taking a position a removed child
held commits only because the delete runs first, and a reorder commits under a deferred
constraint. A refused batch rolls back the creates before it. A collection larger than one read
batch is read whole, a child changed since the read is not deleted, and values a timestamptz
and a numeric column return meet a naive datetime and a float.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.transaction.deps import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_kits.aggregates import ChildCollectionChanges, ChildCollectionSync
from forze_postgres.execution.deps import ConfigurablePostgresDocument, postgres_txmanager
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #


class _Line(Document):
    order_id: UUID
    sku: str
    position: int


class _Create(CreateDocumentCmd):
    order_id: UUID
    sku: str
    position: int


class _Update(BaseDTO):
    position: int | None = None


class _Read(ReadDocument):
    order_id: UUID
    sku: str
    position: int


SPEC = DocumentSpec(
    name="pg_child_lines",
    read=_Read,
    write={"domain": _Line, "create_cmd": _Create, "update_cmd": _Update},
)
SYNC = ChildCollectionSync(
    child_spec=SPEC,
    parent_fk="order_id",
    key_fields=("sku",),
    compare_fields=frozenset({"position"}),
)


async def _ctx(
    pg_client: PostgresClient, *, deferred: bool, batch_size: int = 200
) -> ExecutionContext:
    table = f"child_lines_{uuid4().hex[:12]}"
    when = "DEFERRABLE INITIALLY DEFERRED" if deferred else ""

    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            order_id uuid NOT NULL,
            sku text NOT NULL,
            position integer NOT NULL,
            CONSTRAINT {table}_position UNIQUE (order_id, position) {when}
        );
        """
    )

    return _context(pg_client, table, batch_size=batch_size)


def _context(pg_client: PostgresClient, table: str, *, batch_size: int = 200) -> ExecutionContext:
    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
            batch_size=batch_size,
        )
    )
    plain = Deps.plain(
        {
            PostgresClientDepKey: pg_client,
            PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            DocumentQueryDepKey: doc,
            DocumentCommandDepKey: doc,
        }
    )

    return context_from_deps(
        plain.merge(Deps.routed({TransactionManagerDepKey: {"main": postgres_txmanager}}))
    )


def _lines(order: UUID, skus: Sequence[str]) -> list[_Create]:
    return [_Create(order_id=order, sku=s, position=i) for i, s in enumerate(skus)]


async def _seed(ctx: ExecutionContext, order: UUID, skus: Sequence[str]) -> None:
    async with ctx.tx_ctx.scope("main"):
        await ctx.document.command(SPEC).create_many(_lines(order, skus))


async def _positions(ctx: ExecutionContext, order: UUID) -> dict[str, int]:
    page = await ctx.document.query(SPEC).find_many(filters={"$values": {"order_id": order}})

    return {row.sku: row.position for row in page.hits}


async def _reconcile(ctx: ExecutionContext, order: UUID, lines: list[_Create]) -> Any:
    async with ctx.tx_ctx.scope("main"):
        return await SYNC.reconcile(ctx, order, lines)


# ....................... #


async def test_the_index_refuses_a_create_before_the_delete(pg_client: PostgresClient) -> None:
    # The control: the position is taken while the old row exists.
    ctx = await _ctx(pg_client, deferred=False)
    order = uuid4()
    await _seed(ctx, order, "a")

    with pytest.raises(CoreException) as ei:
        await _seed(ctx, order, "b")

    assert ei.value.kind is ExceptionKind.CONFLICT


async def test_a_new_child_takes_a_removed_childs_position(pg_client: PostgresClient) -> None:
    ctx = await _ctx(pg_client, deferred=False)
    order = uuid4()
    await _seed(ctx, order, "ab")

    changes = await _reconcile(ctx, order, _lines(order, "cb"))

    assert [row.sku for row in changes.removed] == ["a"]
    assert [row.sku for row in changes.created] == ["c"]
    assert changes.updated == []
    assert await _positions(ctx, order) == {"c": 0, "b": 1}


async def test_a_reorder_commits_under_a_deferred_constraint(pg_client: PostgresClient) -> None:
    ctx = await _ctx(pg_client, deferred=True)
    order = uuid4()
    await _seed(ctx, order, "abcd")

    changes = await _reconcile(ctx, order, _lines(order, "dabc"))

    assert len(changes.updated) == 4
    assert await _positions(ctx, order) == {"d": 0, "a": 1, "b": 2, "c": 3}


async def test_a_refused_update_batch_rolls_back_the_creates(pg_client: PostgresClient) -> None:
    ctx = await _ctx(pg_client, deferred=False)
    order = uuid4()
    await _seed(ctx, order, "ab")

    # "a" moves onto the position "b" keeps, so the update batch is refused after "c" is made.
    lines = [
        _Create(order_id=order, sku="a", position=1),
        _Create(order_id=order, sku="b", position=1),
        _Create(order_id=order, sku="c", position=2),
    ]

    with pytest.raises(CoreException) as ei:
        await _reconcile(ctx, order, lines)

    assert ei.value.kind is ExceptionKind.CONFLICT
    assert await _positions(ctx, order) == {"a": 0, "b": 1}


async def test_a_collection_larger_than_one_read_batch(pg_client: PostgresClient) -> None:
    # Batches of ten: every child past the first batch must still be read, or it would be
    # created again (a duplicate position) or never removed.
    ctx = await _ctx(pg_client, deferred=True, batch_size=10)
    order = uuid4()
    before = [f"s{i:02}" for i in range(25)]
    after = [*before[1:], "s25"]
    await _seed(ctx, order, before)

    changes = await _reconcile(ctx, order, _lines(order, after))

    assert [row.sku for row in changes.removed] == ["s00"]
    assert [row.sku for row in changes.created] == ["s25"]
    assert len(changes.updated) == 24
    assert await _positions(ctx, order) == {s: i for i, s in enumerate(after)}


async def test_a_child_changed_since_the_read_is_not_deleted(pg_client: PostgresClient) -> None:
    ctx = await _ctx(pg_client, deferred=False)
    order = uuid4()
    await _seed(ctx, order, "ab")
    query = ctx.document.query(SPEC)
    stale = (await query.find_many(filters={"$values": {"order_id": order}})).hits
    a = next(row for row in stale if row.sku == "a")

    async with ctx.tx_ctx.scope("main"):
        await ctx.document.command(SPEC).update(a.id, a.rev, _Update(position=5))

    async def read_stale(*_: Any, **__: Any) -> AsyncIterator[Sequence[Any]]:
        yield stale

    with patch.object(type(query), "find_stream", read_stale), pytest.raises(CoreException) as ei:
        await _reconcile(ctx, order, [_Create(order_id=order, sku="b", position=1)])

    assert (ei.value.kind, ei.value.code) == (ExceptionKind.PRECONDITION, "revision_mismatch")
    assert await _positions(ctx, order) == {"a": 5, "b": 1}


# ....................... #


class _Grade(Enum):
    A = "a"


class _Priced(Document):
    order_id: UUID
    grade: _Grade
    price: Decimal
    due: datetime


class _CreatePriced(CreateDocumentCmd):
    order_id: UUID
    grade: str
    price: float
    due: datetime


class _UpdatePriced(BaseDTO):
    price: Decimal | None = None
    due: datetime | None = None


class _ReadPriced(ReadDocument):
    order_id: UUID
    grade: _Grade
    price: Decimal
    due: datetime


async def test_stored_values_meet_a_float_a_string_and_a_naive_datetime(
    pg_client: PostgresClient,
) -> None:
    table = f"child_priced_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            order_id uuid NOT NULL,
            grade text NOT NULL,
            price numeric(18, 4) NOT NULL,
            due timestamptz NOT NULL
        );
        """
    )
    ctx = _context(pg_client, table)
    spec = DocumentSpec(
        name="pg_child_priced",
        read=_ReadPriced,
        write={"domain": _Priced, "create_cmd": _CreatePriced, "update_cmd": _UpdatePriced},
    )
    sync = ChildCollectionSync(
        child_spec=spec,
        parent_fk="order_id",
        key_fields=("grade",),
        compare_fields=frozenset({"price", "due"}),
    )
    order = uuid4()
    line = _CreatePriced(order_id=order, grade="a", price=1.1, due=datetime(2026, 1, 1, 9, 30))

    async with ctx.tx_ctx.scope("main"):
        (stored,) = await ctx.document.command(spec).create_many([line])

    # The column returns Decimal("1.1000") and an aware instant.
    assert (stored.price, stored.due.tzinfo is not None) == (Decimal("1.1"), True)

    async with ctx.tx_ctx.scope("main"):
        changes = await sync.reconcile(ctx, order, [line])

    assert changes == ChildCollectionChanges(removed=[], created=[], updated=[])

"""Integration: ``ChildCollectionSync`` against real Postgres unique constraints (RFC 0050).

The in-memory store enforces a declared ``UniqueTogether``; these tests hold the component to a
real index instead, declared nowhere in the spec. A new child taking a position a removed child
held commits only because the delete runs first, and a reorder commits under a deferred
constraint. A refused batch rolls back the creates before it.
"""

from __future__ import annotations

from typing import Any
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
from forze_kits.aggregates import ChildCollectionSync
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


async def _ctx(pg_client: PostgresClient, *, deferred: bool) -> ExecutionContext:
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

    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table), write=("public", table), bookkeeping_strategy="application"
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


def _lines(order: UUID, skus: str) -> list[_Create]:
    return [_Create(order_id=order, sku=s, position=i) for i, s in enumerate(skus)]


async def _seed(ctx: ExecutionContext, order: UUID, skus: str) -> None:
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

"""Integration: a two-phase (prepare/apply) operation against real Postgres.

Proves Option A (lazy acquisition, default-on) and Option B (two-phase handlers)
compose end-to-end through the execution engine: ``prepare`` computes outside the
transaction, the engine opens a Postgres transaction, ``apply`` writes via the
command port, and the row commits and is readable — both through the query port
and via a raw connection after the operation.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.contracts.transaction.deps import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_kits.aggregates.document import (
    TwoPhaseDocumentBuilder,
    TwoPhaseDocumentHandler,
)
from forze_postgres.execution.deps import (
    ConfigurablePostgresDocument,
    postgres_txmanager,
)
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

pytest.importorskip("psycopg")

# ----------------------- #


class Order(Document):
    amount: int = 0


class OrderCreate(CreateDocumentCmd):
    amount: int


class OrderUpdate(BaseDTO):
    amount: int | None = None


class OrderRead(ReadDocument):
    amount: int


ORDER_SPEC = DocumentSpec(
    name="orders",
    read=OrderRead,
    write=DocumentWriteTypes(
        domain=Order,
        create_cmd=OrderCreate,
        update_cmd=OrderUpdate,
    ),
)


class QuoteRequest(BaseDTO):
    item: str


@attrs.define(slots=True, kw_only=True, frozen=True)
class PriceAndCreate(
    TwoPhaseDocumentHandler[QuoteRequest, int, OrderRead, OrderCreate]
):
    """prepare: compute a price (outside the tx). apply: write the order (inside it)."""

    async def prepare(self, args: QuoteRequest) -> int:
        return len(args.item) * 100  # stands in for an external pricing call

    async def apply(self, args: QuoteRequest, payload: int) -> OrderRead:
        return await self.writer.create(OrderCreate(amount=payload))


def _context(pg_client: PostgresClient, table: str) -> ExecutionContext:
    doc: ConfigurablePostgresDocument[Any, Any, Any, Any] = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
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
    routed = Deps.routed({TransactionManagerDepKey: {"main": postgres_txmanager}})
    return context_from_deps(plain.merge(routed))


# ....................... #


@pytest.mark.asyncio
async def test_two_phase_prepare_outside_apply_writes_and_commits(
    pg_client: PostgresClient,
) -> None:
    table = f"two_phase_orders_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            amount integer NOT NULL
        );
        """
    )

    ctx = _context(pg_client, table)
    reg = (
        OperationRegistry(
            handlers={
                "orders.price_and_create": TwoPhaseDocumentBuilder(
                    spec=ORDER_SPEC,
                    build=lambda reader, writer: PriceAndCreate(
                        reader=reader, writer=writer
                    ),
                )
            }
        )
        .bind("orders.price_and_create")
        .two_phase()
        .bind_tx()
        .set_route("main")
        .finish(deep=True)
        .freeze()
    )

    created = await reg.resolve("orders.price_and_create", ctx)(
        QuoteRequest(item="widget")
    )

    assert created.amount == len("widget") * 100  # priced in prepare
    assert isinstance(created.id, UUID)
    assert created.rev == 1

    # Readable through the query port after the operation (committed).
    fetched = await ctx.document.query(ORDER_SPEC).get(created.id)
    assert fetched is not None
    assert fetched.amount == created.amount

    # And visible to a raw out-of-transaction query — the apply transaction committed.
    rows = await pg_client.fetch_all(f"SELECT amount FROM {table}")
    assert [r["amount"] for r in rows] == [created.amount]


@pytest.mark.asyncio
async def test_two_phase_apply_error_rolls_back(
    pg_client: PostgresClient,
) -> None:
    table = f"two_phase_orders_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            amount integer NOT NULL
        );
        """
    )

    @attrs.define(slots=True, kw_only=True, frozen=True)
    class CreateThenFail(
        TwoPhaseDocumentHandler[QuoteRequest, int, OrderRead, OrderCreate]
    ):
        async def prepare(self, args: QuoteRequest) -> int:
            return 7

        async def apply(self, args: QuoteRequest, payload: int) -> OrderRead:
            await self.writer.create(OrderCreate(amount=payload))
            raise RuntimeError("apply boom")  # after the write, before commit

    ctx = _context(pg_client, table)
    reg = (
        OperationRegistry(
            handlers={
                "orders.create_then_fail": TwoPhaseDocumentBuilder(
                    spec=ORDER_SPEC,
                    build=lambda reader, writer: CreateThenFail(
                        reader=reader, writer=writer
                    ),
                )
            }
        )
        .bind("orders.create_then_fail")
        .two_phase()
        .bind_tx()
        .set_route("main")
        .finish(deep=True)
        .freeze()
    )

    with pytest.raises(RuntimeError, match="apply boom"):
        await reg.resolve("orders.create_then_fail", ctx)(QuoteRequest(item="x"))

    # The write rolled back with the transaction — nothing committed.
    rows = await pg_client.fetch_all(f"SELECT count(*) AS n FROM {table}")
    assert rows[0]["n"] == 0

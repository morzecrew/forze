"""Integration tests for Postgres typed writes and JSON dot-path filter coercion."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import uuid4

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
from forze_postgres.execution.deps.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.execution.deps.utils import doc_write_gw
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient


# ----------------------- #
# update_many VALUES casts + typed element writes (write gateway)
# ----------------------- #


class _DeadlineDoc(Document):
    name: str
    deadline: datetime | None = None


class _DeadlineCreate(CreateDocumentCmd):
    name: str
    deadline: datetime | None = None


class _DeadlineUpdate(BaseDTO):
    deadline: datetime | None = None


class _PricedDoc(Document):
    sku: str
    amount: Decimal
    tags: list[str]


class _PricedCreate(CreateDocumentCmd):
    sku: str
    amount: Decimal
    tags: list[str] = []


class _PricedUpdate(BaseDTO):
    amount: Decimal | None = None
    tags: list[str] | None = None


class _PricedRead(ReadDocument):
    sku: str
    amount: Decimal
    tags: list[str]


# ----------------------- #
# JSON dot-path type inference (document query port)
# ----------------------- #


class _Meta(BaseModel):
    score: int
    label: str = ""


class _JsonDoc(Document):
    title: str
    meta: _Meta


class _JsonCreate(CreateDocumentCmd):
    title: str
    meta: _Meta


class _JsonRead(ReadDocument):
    title: str
    meta: _Meta


class _MetaCmp(BaseModel):
    score: int
    min_score: int = 0


class _JsonCmpDoc(Document):
    title: str
    meta: _MetaCmp


class _JsonCmpCreate(CreateDocumentCmd):
    title: str
    meta: _MetaCmp


class _JsonCmpRead(ReadDocument):
    title: str
    meta: _MetaCmp


def _doc_ctx(pg_client: PostgresClient, table: str) -> ExecutionContext:
    fac = ConfigurablePostgresDocument(
        config={
            "read": ("public", table),
            "write": ("public", table),
            "bookkeeping_strategy": "application",
        }
    )
    return ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            }
        )
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_many_values_cast_nullable_timestamptz(
    pg_client: PostgresClient,
) -> None:
    """``UPDATE … FROM (VALUES…)`` must CAST all-NULL timestamptz cells (not infer ``text``)."""
    table = f"pg_cast_ts_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            deadline timestamptz
        );
        """
    )
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    write = doc_write_gw(
        ctx,
        write_types=DocumentWriteTypes(
            domain=_DeadlineDoc,
            create_cmd=_DeadlineCreate,
            update_cmd=_DeadlineUpdate,
        ),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )

    a = await write.create(_DeadlineCreate(name="a"))
    b = await write.create(_DeadlineCreate(name="b"))

    updated, _ = await write.update_many(
        [a.id, b.id],
        [_DeadlineUpdate(deadline=None), _DeadlineUpdate(deadline=None)],
        revs=[a.rev, b.rev],
    )
    assert len(updated) == 2
    assert all(row.deadline is None for row in updated)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_many_coerces_numeric_and_native_text_array(
    pg_client: PostgresClient,
) -> None:
    """Batch patch coerces ``numeric`` elements and persists native ``text[]`` lists."""
    table = f"pg_cast_mix_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            sku text NOT NULL,
            amount numeric(12, 2) NOT NULL,
            tags text[] NOT NULL DEFAULT '{{}}'
        );
        """
    )
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    write = doc_write_gw(
        ctx,
        write_types=DocumentWriteTypes(
            domain=_PricedDoc,
            create_cmd=_PricedCreate,
            update_cmd=_PricedUpdate,
        ),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )
    read = write.read_gw

    x = await write.create(_PricedCreate(sku="x", amount=Decimal("1.25"), tags=[]))
    y = await write.create(_PricedCreate(sku="y", amount=Decimal("2.00"), tags=["a"]))

    patched, _ = await write.update_many(
        [x.id, y.id],
        [
            _PricedUpdate(amount=Decimal("10.50"), tags=["x", "y"]),
            _PricedUpdate(amount=Decimal("20.75"), tags=[]),
        ],
        revs=[x.rev, y.rev],
    )
    assert patched[0].amount == Decimal("10.50")
    assert patched[0].tags == ["x", "y"]
    assert patched[1].amount == Decimal("20.75")
    assert patched[1].tags == []

    rx = await read.get(x.id)
    assert rx.amount == Decimal("10.50")
    assert rx.tags == ["x", "y"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_json_dot_path_filter_coerces_string_operand_to_int(
    pg_client: PostgresClient,
) -> None:
    """``meta.score`` filters coerce string operands using the nested Pydantic field type."""
    table = f"pg_json_coerce_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            meta jsonb NOT NULL
        );
        """
    )
    ctx = _doc_ctx(pg_client, table)
    spec = DocumentSpec(
        name="json_coerce_ns",
        read=_JsonRead,
        write={"domain": _JsonDoc, "create_cmd": _JsonCreate, "update_cmd": BaseDTO},
    )
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_JsonCreate(title="low", meta=_Meta(score=3)))
    await cmd.create(_JsonCreate(title="high", meta=_Meta(score=30)))

    filt = {"$values": {"meta.score": {"$eq": "3"}}}
    assert await query.count(filt) == 1
    row = await query.find(filt)
    assert row is not None
    assert row.title == "low"
    assert row.meta.score == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_json_dot_path_compare_uses_inferred_int_types(
    pg_client: PostgresClient,
) -> None:
    """Field-to-field compares on nested JSON paths use integer semantics from the model."""
    table = f"pg_json_cmp_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            meta jsonb NOT NULL
        );
        """
    )
    ctx = _doc_ctx(pg_client, table)
    spec = DocumentSpec(
        name="json_cmp_ns",
        read=_JsonCmpRead,
        write={
            "domain": _JsonCmpDoc,
            "create_cmd": _JsonCmpCreate,
            "update_cmd": BaseDTO,
        },
    )
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_JsonCmpCreate(title="ok", meta=_MetaCmp(score=10, min_score=5)))
    await cmd.create(_JsonCmpCreate(title="bad", meta=_MetaCmp(score=3, min_score=10)))
    await cmd.create(_JsonCmpCreate(title="tie", meta=_MetaCmp(score=7, min_score=7)))

    filt = {"$fields": {"meta.score": {"$gte": "meta.min_score"}}}
    assert await query.count(filt) == 2
    page = await query.find_page(filt, pagination={"limit": 10, "offset": 0})
    assert {r.title for r in page.hits} == {"ok", "tie"}

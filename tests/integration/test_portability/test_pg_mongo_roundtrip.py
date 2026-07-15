"""The RFC 0017 §8 trust milestone: a portable archive round-trips through *real* infrastructure.

# covers: forze_kits.integrations.portability.ArchiveExporter
# covers: forze_kits.integrations.portability.ArchiveImporter

P1 was only ever proven mock↔mock, and the mock is where this feature's bugs hide — the two we
already fixed (``get_many`` raising on absent ids; import not binding the tenant) were both
invisible until a real adapter was in the loop. So the corpus here is deliberately the one that
breaks the mock horizon: every document carries a ``UUID``, a ``datetime`` and a ``Decimal`` —
the types a ``str``-only payload never asks a serializer or a real column to handle.

One test round-trips **Postgres → Postgres**, the other **Postgres → Mongo** — both across real
containers, both proving all three types land unchanged. The cross-backend leg surfaced (and
forced the fix of) a pre-existing forze_mongo gap: a ``UUID`` or ``Decimal`` *business* field
could not be written to BSON at all, hidden until now because every Mongo test model was
``str``/``int``-only. That was a forze_mongo value-adaptation fix (``UUID``→string, ``Decimal``→
``Decimal128``, both directions); the portability machinery had carried both types correctly.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.contracts.inventory import FrozenSpecRegistry, SpecRegistry
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze.testing import context_from_deps
from forze_kits.dto import ImportTimestamps
from forze_kits.integrations.portability import (
    ArchiveExporter,
    ArchiveImporter,
    TenantScope,
)
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient

# ----------------------- #


class _OrderDoc(Document):
    ref: UUID
    placed_at: datetime
    total: Decimal
    label: str


class _OrderRead(ReadDocument):
    ref: UUID
    placed_at: datetime
    total: Decimal
    label: str


class _OrderCreate(ImportTimestamps):
    ref: UUID
    placed_at: datetime
    total: Decimal
    label: str


class _OrderUpdate(BaseDTO):
    label: str | None = None


ORDER_SPEC: DocumentSpec[_OrderRead, _OrderDoc, _OrderCreate, _OrderUpdate] = DocumentSpec(
    name="orders",
    read=_OrderRead,
    write=DocumentWriteTypes(domain=_OrderDoc, create_cmd=_OrderCreate, update_cmd=_OrderUpdate),
)

# Whole-second timestamps: BSON datetimes are millisecond-precision, so this keeps the round-trip
# about *type* fidelity (does a datetime survive at all) rather than a precision divergence that
# belongs to the P3 divergence catalog.
_CREATED = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
_UPDATED = datetime(2026, 3, 4, 5, 6, 7, tzinfo=UTC)

_PG_COLUMNS = """
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    ref uuid NOT NULL,
    placed_at timestamptz NOT NULL,
    total numeric NOT NULL,
    label text NOT NULL
"""


def _registry() -> FrozenSpecRegistry:
    return SpecRegistry().register(ORDER_SPEC).freeze()


def _pg_ctx(pg_client: PostgresClient, table: str) -> ExecutionContext:
    configurable = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
        )
    )
    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )


def _mongo_ctx(mongo_client: MongoClient, db_name: str, collection: str) -> ExecutionContext:
    configurable = ConfigurableMongoDocument(
        config=MongoDocumentConfig(read=(db_name, collection), write=(db_name, collection))
    )
    return context_from_deps(
        Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )


async def _create_pg_table(pg_client: PostgresClient, table: str) -> None:
    await pg_client.execute(f"CREATE TABLE {table} ({_PG_COLUMNS});")


def _corpus(count: int) -> list[tuple[UUID, _OrderCreate]]:
    return [
        (
            uuid4(),
            _OrderCreate(
                ref=uuid4(),
                placed_at=datetime(2026, 6, index + 1, 12, 0, 0, tzinfo=UTC),
                total=Decimal(f"{index + 1}.99"),
                label=f"order-{index}",
                created_at=_CREATED,
                last_update_at=_UPDATED,
            ),
        )
        for index in range(count)
    ]


async def _seed_pg(
    ctx: ExecutionContext, corpus: list[tuple[UUID, _OrderCreate]]
) -> dict[UUID, _OrderRead]:
    command = ctx.document.command(ORDER_SPEC)
    seeded: dict[UUID, _OrderRead] = {}

    for order_id, create in corpus:
        seeded[order_id] = await command.ensure(order_id, create)

    return seeded


async def _read_all(ctx: ExecutionContext, ids: list[UUID]) -> dict[UUID, _OrderRead]:
    query = ctx.document.query(ORDER_SPEC)
    found = await query.get_many(ids)

    return {doc.id: doc for doc in found}


def _assert_faithful(restored: dict[UUID, _OrderRead], original: dict[UUID, _OrderRead]) -> None:
    assert set(restored) == set(original)

    for doc_id, want in original.items():
        got = restored[doc_id]
        assert got.ref == want.ref, "UUID field must survive"
        assert got.placed_at == want.placed_at, "datetime field must survive"
        assert got.total == want.total, "Decimal field must survive exactly"
        assert got.label == want.label
        assert got.created_at == want.created_at, "created_at preserved via ImportTimestamps"
        assert got.last_update_at == want.last_update_at
        assert got.rev == 1  # optimistic-concurrency lineage resets by design (RFC §7)


# ....................... #


@pytest.mark.asyncio
async def test_roundtrip_through_real_postgres_preserves_rich_types(
    pg_client: PostgresClient,
    tmp_path: Path,
) -> None:
    """Export from one real Postgres table, import into another — UUID + datetime + Decimal all
    land unchanged, through a genuine SQL round-trip and canonical-JSON archive."""

    tenant = uuid4()
    source_table = f"orders_src_{uuid4().hex[:8]}"
    target_table = f"orders_dst_{uuid4().hex[:8]}"
    await _create_pg_table(pg_client, source_table)
    await _create_pg_table(pg_client, target_table)

    source = _pg_ctx(pg_client, source_table)
    seeded = await _seed_pg(source, _corpus(count=4))

    archive = tmp_path / "archive"
    export = await ArchiveExporter()(
        source, _registry(), archive, scope=TenantScope(tenant_id=tenant)
    )
    assert export.total_rows == 4

    target = _pg_ctx(pg_client, target_table)
    result = await ArchiveImporter()(target, _registry(), archive)
    assert result.total_imported == 4

    restored = await _read_all(target, list(seeded))
    _assert_faithful(restored, seeded)


@pytest.mark.asyncio
async def test_export_postgres_import_mongo_preserves_rich_types(
    pg_client: PostgresClient,
    mongo_client: MongoClient,
    tmp_path: Path,
) -> None:
    """The cross-backend trust leg (RFC §8): export from real Postgres, import into real Mongo,
    UUID + datetime + Decimal all landing unchanged. This exercised — and forced the fix of — a
    pre-existing forze_mongo gap (a UUID/Decimal business field could not be written to BSON)."""

    tenant = uuid4()
    table = f"orders_{uuid4().hex[:8]}"
    await _create_pg_table(pg_client, table)

    source = _pg_ctx(pg_client, table)
    seeded = await _seed_pg(source, _corpus(count=4))

    archive = tmp_path / "archive"
    await ArchiveExporter()(source, _registry(), archive, scope=TenantScope(tenant_id=tenant))

    db_name = (await mongo_client.db()).name
    mongo = _mongo_ctx(mongo_client, db_name, f"orders_{uuid4().hex[:8]}")
    result = await ArchiveImporter()(mongo, _registry(), archive)
    assert result.total_imported == 4

    restored = await _read_all(mongo, list(seeded))
    _assert_faithful(restored, seeded)

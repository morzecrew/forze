"""RFC 0017 P3: the direct ``migrate`` mode and the ``run_export_import_roundtrip`` family, on
*real* Postgres and Mongo — plus the datetime-precision divergence the catalog names, pinned.

# covers: forze_kits.integrations.portability.ArchiveMigrator
# covers: forze_kits.integrations.portability.conformance

``migrate`` fuses export and import per chunk, so nothing is ever written to disk — the strongest
form of the cross-backend proof, because it is also the recommended migration path (no plaintext
artifact, KEK re-sealed on the target). The round-trip family runs the export → import → re-export
equality observable across the same two real backends. Both reuse the shared portability corpus
(UUID + datetime + Decimal); only the Postgres/Mongo context wiring is local, so the corpus module
itself stays backend-free for the unit tests.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.inventory import FrozenSpecRegistry
from forze.application.execution import Deps, ExecutionContext
from forze.testing import context_from_deps
from forze_kits.integrations.portability import ArchiveMigrator, TenantScope
from forze_kits.integrations.portability.conformance import run_export_import_roundtrip
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
from tests.support.portability_corpus import (
    ORDER_SPEC,
    OrderCreate,
    assert_orders_faithful,
    order_corpus,
    order_registry,
    read_orders,
    seed_orders,
)

# ----------------------- #

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
    return order_registry().freeze()


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


# ....................... #


@pytest.mark.asyncio
async def test_migrate_postgres_to_mongo_preserves_rich_types(
    pg_client: PostgresClient,
    mongo_client: MongoClient,
) -> None:
    """The flagship P3 leg: migrate straight from a real Postgres table into a real Mongo
    collection — no file, no plaintext at rest — with UUID + datetime + Decimal all landing
    unchanged."""

    tenant = uuid4()
    table = f"orders_{uuid4().hex[:8]}"
    await _create_pg_table(pg_client, table)

    source = _pg_ctx(pg_client, table)
    seeded = await seed_orders(source, order_corpus(count=4), tenant=tenant)

    db_name = (await mongo_client.db()).name
    target = _mongo_ctx(mongo_client, db_name, f"orders_{uuid4().hex[:8]}")

    report = await ArchiveMigrator()(
        source, target, _registry(), scope=TenantScope(tenant_id=tenant)
    )
    assert report.total_imported == 4

    restored = await read_orders(target, list(seeded), tenant=tenant)
    assert_orders_faithful(restored, seeded)


@pytest.mark.asyncio
async def test_migrate_mongo_to_postgres_preserves_rich_types(
    pg_client: PostgresClient,
    mongo_client: MongoClient,
) -> None:
    """The reverse direct leg — real Mongo out, real Postgres in — exercising the forze_mongo read
    side (Decimal128 → Decimal, stored-string → UUID) straight into typed Postgres columns."""

    tenant = uuid4()
    db_name = (await mongo_client.db()).name
    source = _mongo_ctx(mongo_client, db_name, f"orders_{uuid4().hex[:8]}")
    seeded = await seed_orders(source, order_corpus(count=4), tenant=tenant)

    table = f"orders_{uuid4().hex[:8]}"
    await _create_pg_table(pg_client, table)
    target = _pg_ctx(pg_client, table)

    report = await ArchiveMigrator()(
        source, target, _registry(), scope=TenantScope(tenant_id=tenant)
    )
    assert report.total_imported == 4

    restored = await read_orders(target, list(seeded), tenant=tenant)
    assert_orders_faithful(restored, seeded)


@pytest.mark.asyncio
async def test_roundtrip_conformance_postgres_to_mongo_is_lossless(
    pg_client: PostgresClient,
    mongo_client: MongoClient,
    tmp_path: Path,
) -> None:
    """The conformance family across two real backends: export Postgres → import Mongo → re-export
    Mongo, and the two archives' row projections are identical — losslessness by the format's own
    definition (RFC §8), with the whole-second corpus keeping datetime precision out of scope."""

    tenant = uuid4()
    table = f"orders_{uuid4().hex[:8]}"
    await _create_pg_table(pg_client, table)

    source = _pg_ctx(pg_client, table)
    db_name = (await mongo_client.db()).name
    target = _mongo_ctx(mongo_client, db_name, f"orders_{uuid4().hex[:8]}")

    async def seed(ctx: ExecutionContext) -> None:
        await seed_orders(ctx, order_corpus(count=4), tenant=tenant)

    outcome = await run_export_import_roundtrip(
        source,
        target,
        _registry(),
        seed=seed,
        workdir=tmp_path,
        scope=TenantScope(tenant_id=tenant),
    )

    assert outcome.lossless
    assert outcome.documents_match
    assert outcome.exported == outcome.imported == outcome.reexported == 4


@pytest.mark.asyncio
async def test_subsecond_datetime_truncates_on_mongo(mongo_client: MongoClient) -> None:
    """Pins the ``datetime-subsecond-precision`` divergence as a checked fact, not a note.

    A microsecond-precision datetime written to Mongo comes back truncated to milliseconds — BSON's
    datetime is int64 ms since epoch. That is exactly why the conformance corpus uses whole-second
    timestamps: a sub-ms value legitimately differs across backends, so it must stay out of the
    lossless claim. Not a portability bug — the field survives as a datetime; only sub-ms digits
    the target cannot store are dropped."""

    micros = datetime(2026, 6, 1, 12, 0, 0, 123456, tzinfo=UTC)  # 123456 µs = 123.456 ms

    db_name = (await mongo_client.db()).name
    mongo = _mongo_ctx(mongo_client, db_name, f"orders_{uuid4().hex[:8]}")

    order_id = uuid4()
    await mongo.document.command(ORDER_SPEC).ensure(
        order_id,
        OrderCreate(
            ref=uuid4(),
            placed_at=micros,
            total=Decimal("1.00"),
            label="x",
            created_at=micros,
            last_update_at=micros,
        ),
    )

    (restored,) = await mongo.document.query(ORDER_SPEC).get_many([order_id])

    assert restored.placed_at != micros, "BSON cannot hold microsecond precision"
    assert restored.placed_at == micros.replace(microsecond=123000), "truncated to milliseconds"

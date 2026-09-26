"""Postgres document reads with ``owned_by`` — the shared battery, uncached and cached.

The cached leg is the one that matters most: the cache is keyed by primary key alone, so a
row the owner's read put there must still be refused to anyone else.

# covers: DocumentQueryPort.get
# covers: DocumentQueryPort.get_many
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

psycopg = pytest.importorskip("psycopg")

from forze.application.contracts.cache import CacheDepKey, CacheSpec
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    OwnedBy,
)
from forze.application.contracts.transaction.deps import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockCacheAdapter, MockState, MockStateDepKey
from forze_postgres.execution.deps import ConfigurablePostgresDocument, postgres_txmanager
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps
from tests.support.owned_reads_conformance import (
    OWNED_DDL,
    OWNED_READS_BATTERY,
    Check,
    OwnedCreate,
    OwnedReadsHarness,
    owned_spec,
)

# ----------------------- #


async def _context(pg_client: PostgresClient, *, cached: bool) -> tuple[ExecutionContext, Any, MockState]:
    table = f"owned_{uuid4().hex[:12]}"
    await pg_client.execute(OWNED_DDL.format(table=table))

    spec = owned_spec(
        f"doc_{table}",
        **({"cache": CacheSpec(name=f"cache_{table}")} if cached else {}),
    )
    factory = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
        )
    )
    state = MockState()

    def _cache(ctx: ExecutionContext, cache_spec: CacheSpec) -> MockCacheAdapter:
        return MockCacheAdapter(state=ctx.deps.provide(MockStateDepKey), namespace=cache_spec.name)

    ctx = context_from_deps(
        Deps.plain(
            {
                MockStateDepKey: state,
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: factory,
                DocumentCommandDepKey: factory,
                CacheDepKey: _cache,
            }
        ).merge(Deps.routed({TransactionManagerDepKey: {"main": postgres_txmanager}}))
    )

    return ctx, spec, state


@pytest_asyncio.fixture(params=["uncached", "cached"])
async def harness(request: pytest.FixtureRequest, pg_client: PostgresClient) -> OwnedReadsHarness:
    cached = request.param == "cached"
    ctx, spec, state = await _context(pg_client, cached=cached)

    async def _cache_holds(pk: UUID) -> bool:
        return any(key[0] == str(pk) for key in state.cache_bodies.get(spec.cache.name, {}))

    return OwnedReadsHarness(
        query=ctx.doc.query(spec),
        command=ctx.doc.command(spec),
        spec_name=str(spec.name),
        cache_holds=_cache_holds if cached else None,
    )


@pytest.mark.conformance(plane="owned_reads", engine="postgres")
@pytest.mark.parametrize("check", OWNED_READS_BATTERY, ids=lambda check: check.__name__)
async def test_owned_reads_battery(check: Check, harness: OwnedReadsHarness) -> None:
    await check(harness)


# ....................... #


async def test_a_locking_read_never_locks_a_foreign_row(
    pg_client: PostgresClient,
    postgres_container: Any,
) -> None:
    """The owner is in the predicate, so ``FOR UPDATE`` on a foreign row locks nothing.

    Checked from a second session while the refusing transaction is still open: ``NOWAIT``
    fails at once on a row somebody holds.
    """

    ctx, spec, _ = await _context(pg_client, cached=False)
    row = await ctx.doc.command(spec).create(OwnedCreate(owner_id=uuid4()))
    table = str(spec.name).removeprefix("doc_")
    dsn = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")

    async with ctx.tx_ctx.scope("main"):
        with pytest.raises(CoreException) as caught:
            await ctx.doc.query(spec).get(
                row.id,
                owned_by=OwnedBy(field="owner_id", value=uuid4()),
                for_update=True,
            )

        assert caught.value.kind is ExceptionKind.NOT_FOUND

        other = await psycopg.AsyncConnection.connect(dsn, connect_timeout=5)

        async with other, other.transaction():
            await other.execute(
                f"SELECT id FROM {table} WHERE id = %s FOR UPDATE NOWAIT",  # type: ignore[arg-type]
                [row.id],
            )

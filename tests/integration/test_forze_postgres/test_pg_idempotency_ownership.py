"""The three shipped shapes of the Postgres ownership fence, against real Postgres.

The ``owner`` column is optional, so this store has three configurations in the field and
they fail differently — which is why one battery leg (the migrated, owner-carrying store)
is not enough:

1. **The column is absent.** A statement naming it would not execute at all, so the store
   detects it and runs the legacy statements: no fencing, and — the part worth a test — no
   error either, because an upgrade must not require a migration first.
2. **The column exists and an owner is wired.** Fenced; the shared battery runs this one.
3. **The column exists and no owner is available** — a store built outside an invocation,
   or with no provider. The predicate must be *omitted*, not bound to ``NULL``: ``owner =
   NULL`` is ``UNKNOWN`` in SQL, so binding it would match no owned row while looking like
   a working fence. That failure is invisible in shape 1 and shape 2, which is what this
   file exists to catch.

The ``IS NULL`` arm gets its own test for the same reason: rows written before the column
existed (or by a store with no provider) stay completable by their own caller, and nothing
else here would notice if the predicate stopped accepting them.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from psycopg import sql

from forze.application.contracts.idempotency import IdempotencyRecord, IdempotencySpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze_postgres.adapters.idempotency import (
    _UNFENCED_RELATIONS,  # pyright: ignore[reportPrivateUsage]
    PostgresIdempotencyStore,
)
from forze_postgres.execution.deps.configs import PostgresIdempotencyConfig
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client import PostgresClient

# ----------------------- #

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

OP = "owned_op"
HASH = "hash-aaaa"
_SHORT = timedelta(milliseconds=50)


async def _table(pg_client: PostgresClient, *, owner_column: bool) -> str:
    """Create an idempotency table, with or without the optional ``owner`` column."""

    table = f"idem_own_{uuid4().hex[:8]}"
    owner = sql.SQL(", owner UUID") if owner_column else sql.SQL("")

    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {table} (
                op           TEXT        NOT NULL,
                idem_key     TEXT        NOT NULL,
                payload_hash TEXT        NOT NULL,
                status       TEXT        NOT NULL,
                result       BYTEA,
                expires_at   TIMESTAMPTZ NOT NULL{owner},
                PRIMARY KEY (op, idem_key)
            )
            """
        ).format(table=sql.Identifier("public", table), owner=owner)
    )

    return table


def _store(
    pg_client: PostgresClient,
    table: str,
    *,
    owner: UUID | None,
    ttl: timedelta = timedelta(hours=1),
) -> PostgresIdempotencyStore:
    return PostgresIdempotencyStore(
        client=pg_client,
        spec=IdempotencySpec(name="idem", ttl=ttl),
        config=PostgresIdempotencyConfig(relation=("public", table)),
        owner_provider=(lambda: owner),
        # A fresh introspector per store: detection is what is under test here, so sharing
        # one cache across the legs would let a warm answer stand in for a cold one.
        introspector=PostgresIntrospector(client=pg_client),
    )


async def _lapse(store: PostgresIdempotencyStore, key: str) -> None:
    """Take a claim under a short window and wait it out, so the key is reclaimable."""

    assert await store.begin(OP, key, HASH) is None

    await asyncio.sleep(_SHORT.total_seconds() * 1.5 + 0.05)


# ....................... #


class TestUnmigratedTable:
    """Shape 1: no ``owner`` column, so the store runs unfenced rather than failing."""

    async def test_the_store_works_without_the_column(self, pg_client: PostgresClient) -> None:
        table = await _table(pg_client, owner_column=False)
        store = _store(pg_client, table, owner=uuid4())
        key = f"k-{uuid4().hex[:8]}"

        assert await store.begin(OP, key, HASH) is None
        await store.commit(OP, key, HASH, IdempotencyRecord(result=b"done"))

        replayed = await store.begin(OP, key, HASH)

        assert replayed is not None
        assert replayed.result == b"done"

    async def test_a_reclaimed_claim_is_still_committable(
        self, pg_client: PostgresClient
    ) -> None:
        # The behaviour the migration buys back. Asserted rather than left implicit so the
        # cost of not migrating is written down in a test a reader can run.
        table = await _table(pg_client, owner_column=False)
        key = f"k-{uuid4().hex[:8]}"

        displaced = _store(pg_client, table, owner=uuid4(), ttl=_SHORT)
        await _lapse(displaced, key)

        reclaimer = _store(pg_client, table, owner=uuid4())

        assert await reclaimer.begin(OP, key, HASH) is None

        await displaced.commit(OP, key, HASH, IdempotencyRecord(result=b"stale"))

    async def test_the_relation_is_reported_once(self, pg_client: PostgresClient) -> None:
        table = await _table(pg_client, owner_column=False)
        store = _store(pg_client, table, owner=uuid4())

        await store.begin(OP, f"k-{uuid4().hex[:8]}", HASH)

        # An unfenced relation is knowable rather than silent — the mitigation for a
        # guarantee that holds only where the table was migrated.
        assert f"public.{table}" in _UNFENCED_RELATIONS


class TestMigratedTableWithoutAnOwner:
    """Shape 3: the column exists, but this caller cannot name itself."""

    async def test_commit_succeeds_against_an_owned_claim(
        self, pg_client: PostgresClient
    ) -> None:
        # The ``NULL``-binding trap: an omitted predicate commits, a predicate bound to
        # NULL matches nothing and raises. Nothing else in the suite tells those apart.
        table = await _table(pg_client, owner_column=True)
        key = f"k-{uuid4().hex[:8]}"

        owned = _store(pg_client, table, owner=uuid4())

        assert await owned.begin(OP, key, HASH) is None

        anonymous = _store(pg_client, table, owner=None)
        await anonymous.commit(OP, key, HASH, IdempotencyRecord(result=b"done"))

        replayed = await anonymous.begin(OP, key, HASH)

        assert replayed is not None
        assert replayed.result == b"done"

    async def test_fail_releases_an_owned_claim(self, pg_client: PostgresClient) -> None:
        table = await _table(pg_client, owner_column=True)
        key = f"k-{uuid4().hex[:8]}"

        owned = _store(pg_client, table, owner=uuid4())

        assert await owned.begin(OP, key, HASH) is None

        anonymous = _store(pg_client, table, owner=None)
        await anonymous.fail(OP, key, HASH)

        # Released, not merely unfenced-and-ignored: the key is claimable again.
        assert await anonymous.begin(OP, key, HASH) is None


class TestOwnerlessRows:
    """The ``IS NULL`` arm: a claim taken before the column carried anyone."""

    async def test_an_ownerless_claim_is_committable_by_an_owner(
        self, pg_client: PostgresClient
    ) -> None:
        table = await _table(pg_client, owner_column=True)
        key = f"k-{uuid4().hex[:8]}"

        # What a rolling deploy looks like: the process that claimed had no owner, the one
        # that commits does. Refusing here would fail live requests during the upgrade.
        anonymous = _store(pg_client, table, owner=None)

        assert await anonymous.begin(OP, key, HASH) is None

        owned = _store(pg_client, table, owner=uuid4())
        await owned.commit(OP, key, HASH, IdempotencyRecord(result=b"done"))

        replayed = await owned.begin(OP, key, HASH)

        assert replayed is not None
        assert replayed.result == b"done"

    async def test_a_reclaim_takes_ownership_of_the_row(self, pg_client: PostgresClient) -> None:
        table = await _table(pg_client, owner_column=True)
        key = f"k-{uuid4().hex[:8]}"

        # An ownerless claim that lapses must not stay ownerless once reclaimed: the
        # ``IS NULL`` arm would then accept anyone, and the fence would be off for every
        # key that had ever been claimed by an un-owned store. The reclaim has to write
        # its own owner over the row, which is what the third caller's refusal proves.
        stale = _store(pg_client, table, owner=None, ttl=_SHORT)
        await _lapse(stale, key)

        reclaimer = _store(pg_client, table, owner=uuid4())

        assert await reclaimer.begin(OP, key, HASH) is None

        stranger = _store(pg_client, table, owner=uuid4())

        with pytest.raises(CoreException) as ei:
            await stranger.commit(OP, key, HASH, IdempotencyRecord(result=b"stale"))

        assert ei.value.kind == ExceptionKind.CONFLICT

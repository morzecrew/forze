"""Postgres rotating-credential store against a live database.

The mock proves the ordering; only a real database proves the *mechanisms* it rests on —
that ``FOR UPDATE`` genuinely serializes two independent connections, and that a write
failing inside the row-locked transaction surfaces as a lost credential rather than a
retryable error.

# covers: RotatingCredentialStorePort.get
# covers: RotatingCredentialStorePort.refresh
# covers: RotatingCredentialStorePort.put
# covers: RotatingCredentialStorePort.burn
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
import pytest_asyncio
from psycopg import sql
from psycopg.types.json import Jsonb

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.secrets import ExchangedCredential, SecretRef
from forze.application.integrations.crypto import Keyring
from forze.base.exceptions import CoreException
from forze.base.primitives import JsonDict
from forze_mock import MockKeyManagement
from forze_postgres.adapters.rotating_credentials import (
    PostgresRotatingCredentialsAdmin,
    PostgresRotatingCredentialStore,
)
from forze_postgres.kernel.client.client import PostgresClient, PostgresConfig
from tests.support.rotating_credentials import (
    EXCHANGE_TIMEOUT,
    REF,
    ROTATING_STORE_BATTERY,
    Check,
    FakeCounterparty,
    RotatingStoreHarness,
    TenantCell,
)

# ----------------------- #


@pytest_asyncio.fixture
async def credentials_table(pg_client: PostgresClient) -> str:
    table = f"rotating_credentials_{uuid4().hex[:8]}"

    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {table} (
                tenant_id    text        NOT NULL,
                ref          text        NOT NULL,
                payload      jsonb       NOT NULL,
                expires_at   timestamptz,
                version      bigint      NOT NULL,
                burnt_reason text,
                created_at   timestamptz NOT NULL,
                updated_at   timestamptz NOT NULL,
                PRIMARY KEY (tenant_id, ref)
            )
            """
        ).format(table=sql.Identifier("public", table))
    )
    # The control-plane scan's documented index — the battery runs against the real DDL.
    await pg_client.execute(
        sql.SQL("CREATE INDEX ON {table} (tenant_id, updated_at)").format(
            table=sql.Identifier("public", table)
        )
    )

    return table


@pytest_asyncio.fixture
async def harness(
    pg_client: PostgresClient,
    credentials_table: str,
) -> RotatingStoreHarness:
    counterparty = FakeCounterparty()
    tenant = TenantCell()
    store = PostgresRotatingCredentialStore(
        client=pg_client,
        relation=("public", credentials_table),
        exchanger=counterparty,
        exchange_timeout=EXCHANGE_TIMEOUT,
        tenant_provider=tenant,
        # A real keyring: the point of running the battery here is that the envelope
        # survives a genuine jsonb round-trip, not just a dict in memory.
        cipher=Keyring(
            kms=MockKeyManagement(),
            aead=AesGcmAead(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk-rotating")),
        ),
    )

    def _key() -> str:
        return "" if tenant.tenant_id is None else str(tenant.tenant_id)

    async def stored_payload(ref: SecretRef) -> JsonDict:
        row = await pg_client.fetch_one(
            sql.SQL(
                "SELECT payload FROM {table} WHERE tenant_id = %(tenant)s AND ref = %(ref)s"
            ).format(table=sql.Identifier("public", credentials_table)),
            {"tenant": _key(), "ref": ref.path},
        )

        assert row is not None
        return dict(row["payload"])

    async def write_stored_payload(ref: SecretRef, payload: JsonDict) -> None:
        await pg_client.execute(
            sql.SQL(
                "UPDATE {table} SET payload = %(payload)s "
                "WHERE tenant_id = %(tenant)s AND ref = %(ref)s"
            ).format(table=sql.Identifier("public", credentials_table)),
            {"payload": Jsonb(payload), "tenant": _key(), "ref": ref.path},
        )

    @contextlib.asynccontextmanager
    async def break_persist() -> AsyncIterator[None]:
        # A real database-side failure at the real write, not a patched method: the upsert
        # takes its DO UPDATE path on a seeded row, and this trigger raises there.
        trigger = f"break_{credentials_table}"

        await pg_client.execute(
            sql.SQL(
                """
                CREATE FUNCTION {fn}() RETURNS trigger AS $$
                BEGIN
                    RAISE EXCEPTION 'persist deliberately broken';
                END
                $$ LANGUAGE plpgsql
                """
            ).format(fn=sql.Identifier(trigger))
        )
        await pg_client.execute(
            # Scoped to writes that actually change the credential. A real persist failure
            # (constraint, serialization, commit) breaks *that* write; it does not disable
            # every future update, and the store's recovery path has to be able to mark the
            # spent grant unusable afterwards.
            sql.SQL(
                "CREATE TRIGGER {trigger} BEFORE UPDATE ON {table} "
                "FOR EACH ROW WHEN (NEW.payload IS DISTINCT FROM OLD.payload) "
                "EXECUTE FUNCTION {fn}()"
            ).format(
                trigger=sql.Identifier(trigger),
                table=sql.Identifier("public", credentials_table),
                fn=sql.Identifier(trigger),
            )
        )

        try:
            yield

        finally:
            await pg_client.execute(
                sql.SQL("DROP TRIGGER {trigger} ON {table}").format(
                    trigger=sql.Identifier(trigger),
                    table=sql.Identifier("public", credentials_table),
                )
            )
            await pg_client.execute(
                sql.SQL("DROP FUNCTION {fn}()").format(fn=sql.Identifier(trigger))
            )

    return RotatingStoreHarness(
        store=store,
        counterparty=counterparty,
        tenant=tenant,
        admin=PostgresRotatingCredentialsAdmin(
            client=pg_client,
            relation=("public", credentials_table),
            tenant_provider=tenant,
        ),
        break_persist=break_persist,
        stored_payload=stored_payload,
        write_stored_payload=write_stored_payload,
    )


# ....................... #


@pytest.mark.conformance(plane="rotating_credentials", engine="postgres")
@pytest.mark.parametrize("check", ROTATING_STORE_BATTERY, ids=lambda check: check.__name__)
async def test_rotating_store_battery(check: Check, harness: RotatingStoreHarness) -> None:
    await check(harness)


# ....................... #


async def test_the_row_lock_serializes_two_independent_connections(
    postgres_container,
    pg_client: PostgresClient,
    credentials_table: str,
) -> None:
    """The property the in-process stripe cannot provide.

    Two stores over two separate clients stand in for two workers in two processes. The
    loser must block on the row, then re-read a version that has moved and converge on the
    winner's document — never present a token the counterparty has already burned.
    """

    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
    second_client = PostgresClient()
    await second_client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=3))

    try:
        counterparty = FakeCounterparty(delay=0.4)
        stores = [
            PostgresRotatingCredentialStore(
                client=client,
                relation=("public", credentials_table),
                exchanger=counterparty,
            )
            for client in (pg_client, second_client)
        ]

        await stores[0].put(
            REF, ExchangedCredential(access_token="seed-access", refresh_token="seed-refresh")
        )
        observed = (await stores[0].get(REF)).version

        first, second = await asyncio.gather(
            stores[0].refresh(REF, observed=observed),
            stores[1].refresh(REF, observed=observed),
        )

        # Exactly one exchange happened across both "processes", and the grant survived.
        assert counterparty.presented == ["seed-refresh"]
        assert not counterparty.family_revoked

        # Both callers hold the same, committed document.
        assert first.access_token == second.access_token
        assert first.version == second.version
        assert (await stores[1].get(REF)).access_token == first.access_token

    finally:
        await second_client.close()


# ....................... #


async def test_a_waiting_worker_never_sees_a_failed_rotation_as_live(
    postgres_container,
    pg_client: PostgresClient,
    credentials_table: str,
    harness: RotatingStoreHarness,
) -> None:
    """The race the in-place poison exists to close.

    A second process blocks on the row lock while the first rotates. When the first fails
    after presenting the token, whatever the second sees the instant the lock is released is
    what decides whether the spent token gets replayed. Poisoning after the lock is gone
    loses that race by construction; poisoning under it cannot.
    """

    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
    second_client = PostgresClient()
    await second_client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=3))

    try:
        contender = PostgresRotatingCredentialStore(
            client=second_client,
            relation=("public", credentials_table),
            exchanger=harness.counterparty,
            cipher=harness.store.cipher,  # type: ignore[attr-defined]
        )

        await harness.seed()
        before = await harness.store.get(REF)

        # Slow enough that the contender is genuinely queued on the row lock while the
        # first worker is mid-exchange — the only arrangement in which the race exists.
        harness.counterparty.delay = 0.15

        async def _rotate_and_fail() -> None:
            async with harness.break_persist():
                with pytest.raises(CoreException):
                    await harness.store.refresh(REF, observed=before.version)

        async def _contend() -> None:
            # Queues on the row lock, then acts on whatever it finds when granted.
            await asyncio.sleep(0.05)

            with pytest.raises(CoreException):
                await contender.refresh(REF, observed=before.version)

        await asyncio.gather(_rotate_and_fail(), _contend())

        # One presentation, total: the contender found the grant already unusable rather
        # than a restored row it would have replayed the spent token into.
        assert harness.counterparty.presented == ["refresh-seed"]
        assert not harness.counterparty.family_revoked

    finally:
        await second_client.close()


# ....................... #


async def test_a_cancelled_rotation_does_not_hand_the_row_back_live(
    postgres_container,
    pg_client: PostgresClient,
    credentials_table: str,
    harness: RotatingStoreHarness,
) -> None:
    """Cancellation must not release the row lock to a waiter with the token still spent.

    The rollback that a cancellation triggers is the one unwind an after-the-fact mark can
    never win: it hands the lock straight to the queued worker. So the locked section
    declines to be abandoned, and the waiter's first sight of the row is an outcome rather
    than a restored version it would refresh again.
    """

    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
    second_client = PostgresClient()
    await second_client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=3))

    try:
        contender = PostgresRotatingCredentialStore(
            client=second_client,
            relation=("public", credentials_table),
            exchanger=harness.counterparty,
            cipher=harness.store.cipher,  # type: ignore[attr-defined]
        )

        await harness.seed()
        before = await harness.store.get(REF)
        harness.counterparty.delay = 5.0  # outlives the store's own exchange bound

        rotating = asyncio.ensure_future(harness.store.refresh(REF, observed=before.version))
        await asyncio.sleep(0.05)  # the token is presented; the row lock is held

        contending = asyncio.ensure_future(contender.refresh(REF, observed=before.version))
        await asyncio.sleep(0.05)  # the contender is now queued on that row

        rotating.cancel()

        with pytest.raises(asyncio.CancelledError):
            await rotating

        # Whatever the contender is granted, it must not be a live row at `before.version`.
        with pytest.raises(CoreException):
            await contending

        assert harness.counterparty.presented == ["refresh-seed"], "presented exactly once"
        assert not harness.counterparty.family_revoked

    finally:
        harness.counterparty.delay = 0.0
        await second_client.close()


# ....................... #


async def test_a_failure_at_commit_is_also_a_lost_credential(
    pg_client: PostgresClient,
    credentials_table: str,
    harness: RotatingStoreHarness,
) -> None:
    """The write can succeed and the *commit* still fail — same verdict, different moment.

    A deferred constraint trigger fails at ``COMMIT``, which happens as the transaction
    context exits. That is why the failure handling has to wrap the whole scope rather than
    just the write: durability is decided at commit, and a caller must never be handed a
    credential whose commit did not land.
    """

    trigger = f"defer_{credentials_table}"

    await pg_client.execute(
        sql.SQL(
            """
            CREATE FUNCTION {fn}() RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'commit deliberately broken';
            END
            $$ LANGUAGE plpgsql
            """
        ).format(fn=sql.Identifier(trigger))
    )
    await pg_client.execute(
        # Scoped to the credential write, like the immediate-failure trigger: the commit
        # that fails is the one carrying the new payload, and the store must still be able
        # to mark the spent grant unusable afterwards.
        sql.SQL(
            "CREATE CONSTRAINT TRIGGER {trigger} AFTER UPDATE ON {table} "
            "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
            "WHEN (NEW.payload IS DISTINCT FROM OLD.payload) EXECUTE FUNCTION {fn}()"
        ).format(
            trigger=sql.Identifier(trigger),
            table=sql.Identifier("public", credentials_table),
            fn=sql.Identifier(trigger),
        )
    )

    try:
        await harness.seed()
        before = await harness.store.get(REF)

        with pytest.raises(CoreException) as lost:
            await harness.store.refresh(REF, observed=before.version)

        assert lost.value.code == "credential_persist_lost"
        assert harness.counterparty.presented == ["refresh-seed"]

    finally:
        await pg_client.execute(
            sql.SQL("DROP TRIGGER {trigger} ON {table}").format(
                trigger=sql.Identifier(trigger),
                table=sql.Identifier("public", credentials_table),
            )
        )
        await pg_client.execute(sql.SQL("DROP FUNCTION {fn}()").format(fn=sql.Identifier(trigger)))

    # Nothing was half-applied, and nothing was left replayable either: the token that
    # reached the counterparty is spent, so the grant is marked unusable rather than
    # restored to a version a waiting worker would happily refresh again.
    with pytest.raises(CoreException) as poisoned:
        await harness.store.get(REF)

    assert poisoned.value.code == "credential_burnt"


# ....................... #


async def test_a_burn_notice_survives_the_transaction_that_recorded_it(
    harness: RotatingStoreHarness,
) -> None:
    """The burn notice is committed, not rolled back by the error that follows it.

    The refresh raises after the exchange is permanently rejected — but the notice must
    already be durable, or the next worker would present the dead token all over again.
    """

    await harness.seed()
    observed = (await harness.store.get(REF)).version
    harness.counterparty.fail_permanently = True

    with pytest.raises(CoreException) as burnt:
        await harness.store.refresh(REF, observed=observed)

    assert burnt.value.code == "credential_burnt"

    # Committed: a *different* read, in its own transaction, sees the notice.
    harness.counterparty.fail_permanently = False

    with pytest.raises(CoreException) as still_burnt:
        await harness.store.get(REF)

    assert still_burnt.value.code == "credential_burnt"


# ....................... #


class TestCredentialSweepEndToEnd:
    """The sweeper against the real Postgres store and scan — the full 0037 loop.

    The kit's unit tests run over the mock; this is the leg that catches wire-format
    facts the oracle cannot: the scanned ``SecretVersion`` token is a Postgres bigint
    rendered to text, it rides through a durable run's JSON input, and it must come back
    as exactly the ``observed`` the store's row-locked recheck compares against. It also
    caught the resolution bug the mock's original singleton wiring masked — the sweeper
    resolving deps with ``provide`` worked against a singleton and would have returned
    the factory raw against this wiring.
    """

    async def test_sweep_scan_refresh_loop_against_postgres(
        self,
        pg_client: PostgresClient,
        credentials_table: str,
    ) -> None:
        from datetime import timedelta

        from forze.application.contracts.secrets import (
            ExchangedCredential,
            RotatingCredentialsAdminDepKey,
            RotatingCredentialsDepKey,
        )
        from forze.application.execution import Deps
        from forze_kits.integrations.durable import durable_kits_deps
        from forze_kits.integrations.durable.registry import DurableFunctionRegistry
        from forze_kits.integrations.secrets import CredentialSweeper
        from forze_mock import MockDepsModule, MockState
        from tests.support.execution_context import context_from_deps

        counterparty = FakeCounterparty()
        store = PostgresRotatingCredentialStore(
            client=pg_client,
            relation=("public", credentials_table),
            exchanger=counterparty,
            exchange_timeout=EXCHANGE_TIMEOUT,
        )
        admin = PostgresRotatingCredentialsAdmin(
            client=pg_client,
            relation=("public", credentials_table),
        )

        registry = DurableFunctionRegistry()
        sweeper = CredentialSweeper(refresh_if_idle_for=timedelta(microseconds=1))
        sweeper.register(registry)
        durable_deps, runner, _ = durable_kits_deps(registry=registry)

        # Mock durable substrate under a real credential store: the sweep's run journal
        # is not what this leg is about, the Postgres row round-trip is.
        ctx = context_from_deps(
            MockDepsModule(state=MockState())(),
            durable_deps,
            Deps.plain(
                {
                    # Factories, as the real modules register them: resolve_simple invokes
                    # the registered value with the context.
                    RotatingCredentialsDepKey: lambda _ctx: store,
                    RotatingCredentialsAdminDepKey: lambda _ctx: admin,
                }
            ),
        )

        ref = SecretRef("oauth/pg-sweep")
        await store.put(
            ref,
            ExchangedCredential(access_token="access-0", refresh_token="refresh-0"),
        )
        before = await store.get(ref)

        sweep = await sweeper.sweep_now(ctx)
        assert sweep.output_json is not None
        assert sweep.output_json["enqueued"] == 1

        drained = 0
        while claimed := await runner.recover(ctx, limit=10):
            drained += claimed
        assert drained == 1

        # The exchange happened exactly once, and the persisted version moved on.
        assert counterparty.presented == ["refresh-0"]
        after = await store.get(ref)
        assert after.version != before.version

        # The clock reset: the grant is no longer due at a cutoff just past its seed
        # stamp, and a second full pass converges without presenting another token.
        second = await sweeper.sweep_now(ctx, idempotency_key="second-pass")
        assert second.output_json is not None

        while await runner.recover(ctx, limit=10):
            pass

        assert len(counterparty.presented) <= 2  # at most the refreshed token, once
        assert not counterparty.family_revoked

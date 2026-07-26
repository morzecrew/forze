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
from forze_postgres.adapters.rotating_credentials import PostgresRotatingCredentialStore
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
            sql.SQL(
                "CREATE TRIGGER {trigger} BEFORE UPDATE ON {table} "
                "FOR EACH ROW EXECUTE FUNCTION {fn}()"
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
        break_persist=break_persist,
        stored_payload=stored_payload,
        write_stored_payload=write_stored_payload,
    )


# ....................... #


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
        sql.SQL(
            "CREATE CONSTRAINT TRIGGER {trigger} AFTER UPDATE ON {table} "
            "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {fn}()"
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

    # Nothing was half-applied: the stored grant is still the one the exchange superseded.
    assert (await harness.store.get(REF)).version == before.version


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

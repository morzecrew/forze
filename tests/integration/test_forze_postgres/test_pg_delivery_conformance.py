"""The crash-recovery delivery scenario, run against real Postgres — the mock↔real differential.

The same `forze_dst.conformance.run_crash_recovery_delivery` scenario that passes against the mock is
run here against a real Postgres outbox + inbox (testcontainers): stage + flush in a transaction →
claim → publish → **crash before mark_published** → reclaim the stuck `processing` rows → re-claim →
re-publish → mark → consume with inbox dedup. Asserting the same `DeliveryOutcome` on both backends is
the differential — the mock's write-through outbox/inbox reproduce Postgres's delivery-under-crash
semantics (at-least-once + exactly-once effect), so a DST crash simulation of the outbox path can trust
the mock. The `outbox-inbox-write-through` divergence is a *concurrent-visibility* gap; atomicity on the
crash path holds, so this positive property agrees on both engines — which this pins.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
import pytest_asyncio
from psycopg import sql

from forze.application.execution import (
    DepsRegistry,
    ExecutionContext,
    ExecutionRuntime,
)
from forze.testing import context_from_deps
from forze_dst.conformance import (
    DELIVERY_EVENTS,
    DELIVERY_INBOX,
    DELIVERY_OUTBOX,
    DeliveryOutcome,
    observe_uncommitted_outbox_visibility,
    run_crash_recovery_delivery,
)
from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.execution.deps.configs import (
    PostgresInboxConfig,
    PostgresOutboxConfig,
)
from forze_postgres.kernel.client import PostgresClient

# ----------------------- #

_N = len(DELIVERY_EVENTS)
_TX_SCOPE = "default"
# Both specs share this name → it is the outbox route, the inbox route, and the config key on both
# `outboxes=` and `inboxes=`.
_ROUTE = DELIVERY_OUTBOX.name


@pytest_asyncio.fixture(scope="function")
async def delivery_tables(pg_client: PostgresClient):
    """A dedicated outbox + inbox table pair (application-provided schema)."""

    suffix = uuid4().hex[:8]
    outbox = f"conformance_outbox_{suffix}"
    inbox = f"conformance_inbox_{suffix}"

    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {t} (
                id UUID PRIMARY KEY,
                outbox_route TEXT NOT NULL,
                event_id UUID NOT NULL,
                event_type TEXT NOT NULL,
                tenant_id UUID,
                execution_id UUID,
                correlation_id UUID,
                causation_id UUID,
                occurred_at TIMESTAMPTZ NOT NULL,
                payload JSONB NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                published_at TIMESTAMPTZ,
                processing_at TIMESTAMPTZ,
                last_error TEXT,
                attempts INT NOT NULL DEFAULT 0,
                available_at TIMESTAMPTZ,
                ordering_key TEXT,
                hlc BIGINT,
                traceparent TEXT,
                UNIQUE (outbox_route, event_id)
            )
            """
        ).format(t=sql.Identifier("public", outbox))
    )
    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {t} (
                inbox_route  TEXT NOT NULL,
                message_id   TEXT NOT NULL,
                processed_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (inbox_route, message_id)
            )
            """
        ).format(t=sql.Identifier("public", inbox))
    )

    yield outbox, inbox

    await pg_client.execute(
        sql.SQL("DROP TABLE IF EXISTS {t}").format(t=sql.Identifier("public", outbox))
    )
    await pg_client.execute(
        sql.SQL("DROP TABLE IF EXISTS {t}").format(t=sql.Identifier("public", inbox))
    )


def _runtime(
    pg_client: PostgresClient, outbox_table: str, inbox_table: str
) -> ExecutionRuntime:
    module = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={_ROUTE: PostgresOutboxConfig(relation=("public", outbox_table))},
        inboxes={_ROUTE: PostgresInboxConfig(relation=("public", inbox_table))},
    )
    return ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())


def _outbox_context(pg_client: PostgresClient, outbox_table: str) -> ExecutionContext:
    # One independent session over the pooled client (its own connection per task), so the producer
    # and the relay run as genuinely concurrent Postgres transactions — mirrors the isolation battery.
    deps = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={_ROUTE: PostgresOutboxConfig(relation=("public", outbox_table))},
    )()
    return context_from_deps(deps)


# ....................... #


@pytest.mark.integration
class TestPostgresCrashRecoveryDelivery:
    async def test_exactly_once_effect_with_inbox(
        self, pg_client: PostgresClient, delivery_tables
    ) -> None:
        outbox_table, inbox_table = delivery_tables
        runtime = _runtime(pg_client, outbox_table, inbox_table)

        async with runtime.scope():
            outcome = await run_crash_recovery_delivery(
                runtime.get_context(), tx_scope=_TX_SCOPE, dedup=True
            )

        # Real Postgres produces the SAME outcome the mock does: the crash re-published every event
        # (delivered twice), the restart reclaimed the crashed round's rows, and the inbox collapsed
        # the duplicate to a single effect — mock ≡ real for the crash-recovery delivery path.
        assert outcome == DeliveryOutcome(
            staged=_N,
            delivered=2 * _N,
            reclaimed=_N,
            applied=_N,
            distinct_applied=_N,
        )

    async def test_duplicate_is_real_without_inbox(
        self, pg_client: PostgresClient, delivery_tables
    ) -> None:
        outbox_table, inbox_table = delivery_tables
        runtime = _runtime(pg_client, outbox_table, inbox_table)

        async with runtime.scope():
            outcome = await run_crash_recovery_delivery(
                runtime.get_context(), tx_scope=_TX_SCOPE, dedup=False
            )

        # Without dedup the redelivery applies twice on real Postgres too — the reclaim genuinely
        # re-published (the inbox is doing real work, not masking a no-op).
        assert outcome == DeliveryOutcome(
            staged=_N,
            delivered=2 * _N,
            reclaimed=_N,
            applied=2 * _N,
            distinct_applied=_N,
        )


@pytest.mark.integration
class TestPostgresOutboxOverVisibility:
    """The other side of the ``outbox-inbox-write-through`` divergence: real Postgres READ COMMITTED
    does NOT let a concurrent relay claim a producer's uncommitted outbox row — where the mock does
    (the unit leg asserts the mock side). Pins the documented disagreement from both ends."""

    async def test_relay_cannot_see_uncommitted_row_on_real_postgres(
        self, pg_client: PostgresClient, delivery_tables
    ) -> None:
        outbox_table, _ = delivery_tables
        producer = _outbox_context(pg_client, outbox_table)
        relay = _outbox_context(pg_client, outbox_table)
        over_visible = await observe_uncommitted_outbox_visibility(
            producer, relay, tx_scope=_TX_SCOPE
        )
        # Real Postgres prevents the dirty read — the relay claims nothing while the producer holds
        # its transaction open. The mock's write-through over-visibility is a mock-only artifact.
        assert over_visible is False

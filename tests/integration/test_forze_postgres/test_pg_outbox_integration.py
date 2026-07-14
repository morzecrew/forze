"""Integration tests for Postgres outbox adapter.

# covers: OutboxCommandPort.flush
# covers: OutboxQueryPort.claim_pending
# covers: OutboxQueryPort.mark_published
# covers: OutboxQueryPort.mark_retry
# covers: OutboxQueryPort.mark_failed
# covers: OutboxQueryPort.reclaim_stale_processing
# covers: OutboxQueryPort.requeue_failed
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from pydantic import BaseModel

from forze.application.contracts.outbox import (
    IntegrationEvent,
    OutboxClaim,
    OutboxDestination,
    OutboxSpec,
    OutboxStatus,
)
from forze.application.contracts.queue import (
    QueueCommandDepKey,
    QueueQueryDepKey,
    QueueSpec,
)
from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.stream import StreamQueryDepKey, StreamSpec
from forze.application.execution import (
    Deps,
    DepsRegistry,
    ExecutionRuntime,
    LifecyclePlan,
)
from forze.base.primitives import utcnow
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import (
    OutboxRelay,
    outbox_relay_background_lifecycle_step,
)
from forze_kits.integrations.outbox._relay_core import relay_outbox_claims
from forze_mock import MockStateDepKey
from forze_mock.adapters import MockState
from forze_mock.execution.module import ConfigurableMockQueue, MockDepsModule
from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.execution.deps.configs import PostgresOutboxConfig
from forze_postgres.execution.lifecycle.capabilities import POSTGRES_CLIENT_CAPABILITY
from forze_postgres.kernel.client import PostgresClient
from forze_redis import (
    RedisDepsModule,
    RedisStreamConfig,
    RedisStreamGroupConfig,
)
from forze_redis.kernel.client import RedisClient


class _OutboxPayload(BaseModel):
    label: str


class _RichPayload(BaseModel):
    """The most ordinary integration-event payload there is.

    Every other payload model in this suite is one or two ``str`` fields, and that is exactly
    how the outbox went so long unable to carry a ``UUID``: the staged map goes into a ``JSONB``
    column via psycopg's ``Jsonb``, which serializes with stdlib ``json.dumps``, and a
    ``str``-only payload never asks it to encode anything it cannot.
    """

    order_id: UUID
    placed_at: datetime
    total: Decimal
    label: str


def _mock_queue_deps(shared_state: MockState) -> Deps:
    mock_module = MockDepsModule(state=shared_state)
    queue = ConfigurableMockQueue(module=mock_module)
    return Deps.plain({MockStateDepKey: shared_state}).merge(
        Deps.routed(
            {
                QueueCommandDepKey: {"relay": queue},
                QueueQueryDepKey: {"relay": queue},
            }
        )
    )


@pytest.fixture
async def outbox_table(pg_client: PostgresClient) -> str:
    """Create a dedicated outbox table and return its qualified name suffix."""

    schema = "public"
    table = f"outbox_{uuid4().hex[:8]}"

    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {table} (
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
        ).format(table=sql.Identifier(schema, table))
    )

    yield table

    await pg_client.execute(
        sql.SQL("DROP TABLE IF EXISTS {table}").format(table=sql.Identifier(schema, table))
    )


@pytest.mark.asyncio
async def test_outbox_flush_commits_with_transaction(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    pg_module = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={
            "integration": PostgresOutboxConfig(
                relation=("public", outbox_table),
            ),
        },
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(pg_module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        async with ctx.tx_ctx.scope("default"):
            outbox = ctx.outbox.command(outbox_spec)
            await outbox.stage("demo.created", _OutboxPayload(label="ok"))
            assert await outbox.flush() == 1

        rows = await pg_client.fetch_all(
            sql.SQL("SELECT status FROM {t} WHERE outbox_route = %s").format(
                t=sql.Identifier("public", outbox_table)
            ),
            ("integration",),
        )

    assert len(rows) == 1
    assert rows[0]["status"] == "pending"


@pytest.mark.asyncio
async def test_outbox_rollback_discards_staged_rows(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    pg_module = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={
            "integration": PostgresOutboxConfig(
                relation=("public", outbox_table),
            ),
        },
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(pg_module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        with pytest.raises(RuntimeError):
            async with ctx.tx_ctx.scope("default"):
                await ctx.outbox.command(outbox_spec).stage(
                    "demo.created",
                    _OutboxPayload(label="rollback"),
                )
                raise RuntimeError("abort")

        rows = await pg_client.fetch_all(
            sql.SQL("SELECT id FROM {t}").format(t=sql.Identifier("public", outbox_table))
        )

    assert rows == []


@pytest.mark.asyncio
async def test_outbox_relay_to_mock_queue(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(
        name="integration",
        codec=codec,
        destination=OutboxDestination.queue(route="relay", channel="relay"),
    )
    queue_spec = QueueSpec(name="relay", codec=codec)
    pg_module = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={
            "integration": PostgresOutboxConfig(
                relation=("public", outbox_table),
            ),
        },
    )
    shared_state = MockState()
    mock_queue_deps = _mock_queue_deps(shared_state)
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(pg_module).with_deps(mock_queue_deps).freeze(),
    )

    async with runtime.scope():
        ctx = runtime.get_context()
        async with ctx.tx_ctx.scope("default"):
            await ctx.outbox.command(outbox_spec).stage(
                "demo.created",
                _OutboxPayload(label="relay"),
            )
            await ctx.outbox.command(outbox_spec).flush()

        result = await OutboxRelay(outbox_spec=outbox_spec).to_queue(ctx, queue_spec)

    assert result.published == 1
    state = shared_state

    assert len(state.queues["relay"]["relay"]) == 1


@pytest.mark.asyncio
async def test_an_event_payload_of_uuid_datetime_and_decimal_round_trips(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    """A payload carrying a UUID, a datetime and a Decimal survives stage → relay → consume.

    It did not. ``StagedOutboxCommand`` encoded the payload in the codec's default *python*
    mode, which keeps those three as Python objects — correct for a driver that binds them
    natively, and wrong for the ``JSONB`` column this map is actually bound into. So
    ``stage()`` raised ``TypeError: Object of type UUID is not JSON serializable`` on Postgres
    for the most natural event payload anyone would write, while the *same* payload staged
    happily on Mongo (a BSON subdocument takes both) and on any encrypting route (orjson takes
    both) — the accepted field types depended on the backend and on the encryption setting.

    Asserting the decoded values rather than the stored bytes is the point: the payload has to
    come back as the types it went in as, or the consumer gets strings where it declared a UUID.
    """

    codec = PydanticModelCodec(_RichPayload)
    outbox_spec = OutboxSpec(
        name="integration",
        codec=codec,
        destination=OutboxDestination.queue(route="relay", channel="relay"),
    )
    queue_spec = QueueSpec(name="relay", codec=codec)
    pg_module = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={
            "integration": PostgresOutboxConfig(relation=("public", outbox_table)),
        },
    )
    shared_state = MockState()
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(pg_module)
        .with_deps(_mock_queue_deps(shared_state))
        .freeze(),
    )

    sent = _RichPayload(
        order_id=uuid4(),
        placed_at=datetime(2026, 7, 14, 12, 30, tzinfo=UTC),
        total=Decimal("19.99"),
        label="rich",
    )

    async with runtime.scope():
        ctx = runtime.get_context()

        async with ctx.tx_ctx.scope("default"):
            await ctx.outbox.command(outbox_spec).stage("order.placed", sent)
            assert await ctx.outbox.command(outbox_spec).flush() == 1

        # The row is really in Postgres, and the JSONB really holds JSON-native values.
        rows = await pg_client.fetch_all(
            sql.SQL("SELECT payload FROM {t} WHERE outbox_route = %s").format(
                t=sql.Identifier("public", outbox_table)
            ),
            ("integration",),
        )
        stored = rows[0]["payload"]

        assert stored["order_id"] == str(sent.order_id)
        assert stored["total"] == "19.99"  # Decimal keeps its precision as a string

        # …and decoding it gives back the model, with its declared types intact.
        assert codec.decode_mapping(stored) == sent

        result = await OutboxRelay(outbox_spec=outbox_spec).to_queue(ctx, queue_spec)

    assert result.published == 1

    # The whole journey: the consumer receives the payload it was sent, not a bag of strings.
    delivered = shared_state.queues["relay"]["relay"]
    assert len(delivered) == 1

    received = delivered[0].message.payload

    assert received == sent
    assert isinstance(received.order_id, UUID)
    assert isinstance(received.placed_at, datetime)
    assert isinstance(received.total, Decimal)


@pytest.mark.asyncio
async def test_outbox_ordering_key_round_trips_stage_column_claim(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    pg_module = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={
            "integration": PostgresOutboxConfig(
                relation=("public", outbox_table),
            ),
        },
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(pg_module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        async with ctx.tx_ctx.scope("default"):
            outbox = ctx.outbox.command(outbox_spec)
            await outbox.stage(
                "demo.created",
                _OutboxPayload(label="keyed"),
                ordering_key="order-1",
            )
            await outbox.stage("demo.updated", _OutboxPayload(label="unkeyed"))
            assert await outbox.flush() == 2

        rows = await pg_client.fetch_all(
            sql.SQL("SELECT event_type, ordering_key FROM {t} WHERE outbox_route = %s").format(
                t=sql.Identifier("public", outbox_table)
            ),
            ("integration",),
        )
        by_type = {row["event_type"]: row["ordering_key"] for row in rows}
        assert by_type == {"demo.created": "order-1", "demo.updated": None}

        claims = {c.event_type: c for c in await ctx.outbox.query(outbox_spec).claim_pending()}
        assert claims["demo.created"].ordering_key == "order-1"
        assert claims["demo.updated"].ordering_key is None


@pytest.mark.asyncio
async def test_outbox_relay_publishes_ordering_key_to_queue(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(
        name="integration",
        codec=codec,
        destination=OutboxDestination.queue(route="relay", channel="relay"),
    )
    queue_spec = QueueSpec(name="relay", codec=codec)
    pg_module = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={
            "integration": PostgresOutboxConfig(
                relation=("public", outbox_table),
            ),
        },
    )
    shared_state = MockState()
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(pg_module)
        .with_deps(_mock_queue_deps(shared_state))
        .freeze(),
    )
    unkeyed_event_id = uuid4()

    async with runtime.scope():
        ctx = runtime.get_context()
        async with ctx.tx_ctx.scope("default"):
            outbox = ctx.outbox.command(outbox_spec)
            await outbox.stage(
                "demo.created",
                _OutboxPayload(label="keyed"),
                ordering_key="order-1",
            )
            await outbox.stage(
                "demo.updated",
                _OutboxPayload(label="unkeyed"),
                event_id=unkeyed_event_id,
            )
            await outbox.flush()

        result = await OutboxRelay(outbox_spec=outbox_spec).to_queue(ctx, queue_spec)

    assert result.published == 2
    messages = [e.message for e in shared_state.queues["relay"]["relay"]]
    by_type = {m.type: m for m in messages}
    # Staged ordering key occupies the transport key...
    assert by_type["demo.created"].key == "order-1"
    # ...and the no-key event keeps the pre-ordering-key fallback.
    assert by_type["demo.updated"].key == str(unkeyed_event_id)
    # The event id always rides the envelope header for consumer dedup.
    assert by_type["demo.updated"].headers["forze_event_id"] == str(unkeyed_event_id)
    assert by_type["demo.created"].headers["forze_event_id"] != "order-1"


@pytest.mark.asyncio
async def test_outbox_bulk_flush_writes_multiple_rows(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    pg_module = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={
            "integration": PostgresOutboxConfig(
                relation=("public", outbox_table),
            ),
        },
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(pg_module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        async with ctx.tx_ctx.scope("default"):
            outbox = ctx.outbox.command(outbox_spec)
            await outbox.stage_many(
                [
                    ("a", _OutboxPayload(label="one")),
                    ("b", _OutboxPayload(label="two")),
                    ("c", _OutboxPayload(label="three")),
                ]
            )
            assert await outbox.flush() == 3

        rows = await pg_client.fetch_all(
            sql.SQL("SELECT id FROM {t} WHERE outbox_route = %s").format(
                t=sql.Identifier("public", outbox_table)
            ),
            ("integration",),
        )

    assert len(rows) == 3


@pytest.mark.asyncio
async def test_outbox_duplicate_event_id_flush_is_idempotent(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    pg_module = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={
            "integration": PostgresOutboxConfig(
                relation=("public", outbox_table),
            ),
        },
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(pg_module).freeze())
    event_id = uuid4()

    async with runtime.scope():
        ctx = runtime.get_context()
        async with ctx.tx_ctx.scope("default"):
            outbox = ctx.outbox.command(outbox_spec)
            await outbox.stage_event(
                IntegrationEvent(
                    event_type="demo.created",
                    payload=_OutboxPayload(label="first"),
                    event_id=event_id,
                )
            )
            assert await outbox.flush() == 1

    async with runtime.scope():
        ctx = runtime.get_context()
        async with ctx.tx_ctx.scope("default"):
            outbox = ctx.outbox.command(outbox_spec)
            await outbox.stage_event(
                IntegrationEvent(
                    event_type="demo.created",
                    payload=_OutboxPayload(label="second"),
                    event_id=event_id,
                )
            )
            assert await outbox.flush() == 0

    rows = await pg_client.fetch_all(
        sql.SQL("SELECT payload FROM {t} WHERE event_id = %s").format(
            t=sql.Identifier("public", outbox_table)
        ),
        (event_id,),
    )

    assert len(rows) == 1
    assert rows[0]["payload"]["label"] == "first"


@pytest.mark.asyncio
async def test_outbox_relay_reclaims_stale_processing(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(
        name="integration",
        codec=codec,
        destination=OutboxDestination.queue(route="relay", channel="relay"),
    )
    queue_spec = QueueSpec(name="relay", codec=codec)
    pg_module = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={
            "integration": PostgresOutboxConfig(
                relation=("public", outbox_table),
            ),
        },
    )
    shared_state = MockState()
    mock_queue_deps = _mock_queue_deps(shared_state)
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(pg_module).with_deps(mock_queue_deps).freeze(),
    )
    row_id = uuid4()
    event_id = uuid4()
    stale_at = utcnow() - timedelta(hours=1)

    async with runtime.scope():
        ctx = runtime.get_context()
        await pg_client.execute(
            sql.SQL(
                """
                INSERT INTO {t} (
                    id, outbox_route, event_id, event_type,
                    occurred_at, payload, status, created_at, processing_at
                ) VALUES (
                    %(id)s, %(route)s, %(event_id)s, %(event_type)s,
                    %(occurred_at)s, %(payload)s, %(status)s, %(created_at)s,
                    %(processing_at)s
                )
                """
            ).format(t=sql.Identifier("public", outbox_table)),
            {
                "id": row_id,
                "route": "integration",
                "event_id": event_id,
                "event_type": "demo.created",
                "occurred_at": utcnow(),
                "payload": Jsonb({"label": "stale"}),
                "status": OutboxStatus.PROCESSING.value,
                "created_at": utcnow(),
                "processing_at": stale_at,
            },
        )

        result = await OutboxRelay(
            outbox_spec=outbox_spec, reclaim_stale_after=timedelta(minutes=5)
        ).to_queue(ctx, queue_spec)

    assert result.reclaimed >= 1
    assert result.published == 1
    assert len(shared_state.queues["relay"]["relay"]) == 1


@pytest.mark.asyncio
async def test_outbox_requeue_failed_then_relay(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(
        name="integration",
        codec=codec,
        destination=OutboxDestination.queue(route="relay", channel="relay"),
    )
    queue_spec = QueueSpec(name="relay", codec=codec)
    pg_module = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={
            "integration": PostgresOutboxConfig(
                relation=("public", outbox_table),
            ),
        },
    )
    shared_state = MockState()
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(pg_module)
        .with_deps(_mock_queue_deps(shared_state))
        .freeze(),
    )
    row_id = uuid4()
    event_id = uuid4()

    await pg_client.execute(
        sql.SQL(
            """
            INSERT INTO {t} (
                id, outbox_route, event_id, event_type,
                occurred_at, payload, status, created_at
            ) VALUES (
                %(id)s, %(route)s, %(event_id)s, %(event_type)s,
                %(occurred_at)s, %(payload)s, %(status)s, %(created_at)s
            )
            """
        ).format(t=sql.Identifier("public", outbox_table)),
        {
            "id": row_id,
            "route": "integration",
            "event_id": event_id,
            "event_type": "demo.created",
            "occurred_at": utcnow(),
            "payload": Jsonb({"label": "failed"}),
            "status": OutboxStatus.FAILED.value,
            "created_at": utcnow(),
        },
    )

    async with runtime.scope():
        ctx = runtime.get_context()
        query = ctx.outbox.query(outbox_spec)
        assert await query.requeue_failed([row_id]) == 1

        result = await OutboxRelay(outbox_spec=outbox_spec).to_queue(ctx, queue_spec)

    assert result.published == 1
    assert len(shared_state.queues["relay"]["relay"]) == 1


# ----------------------- #
# Relay failure model (mark_retry / mark_failed / requeue_failed / available_at)


def _pg_runtime(pg_client: PostgresClient, outbox_table: str) -> ExecutionRuntime:
    pg_module = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={
            "integration": PostgresOutboxConfig(
                relation=("public", outbox_table),
            ),
        },
    )
    return ExecutionRuntime(deps=DepsRegistry.from_modules(pg_module).freeze())


@pytest.mark.asyncio
async def test_outbox_retry_cycle_publishes_after_transient_failures(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    runtime = _pg_runtime(pg_client, outbox_table)

    fail_counts: dict[str, int] = {}

    async def flaky_publish(claim: OutboxClaim, payload: Any) -> None:
        count = fail_counts.get(str(claim.event_id), 0)
        if count < 2:
            fail_counts[str(claim.event_id)] = count + 1
            raise RuntimeError("transient broker error")

    async with runtime.scope():
        ctx = runtime.get_context()
        async with ctx.tx_ctx.scope("default"):
            outbox = ctx.outbox.command(outbox_spec)
            await outbox.stage_many(
                [
                    ("a", _OutboxPayload(label="one")),
                    ("b", _OutboxPayload(label="two")),
                    ("c", _OutboxPayload(label="three")),
                ]
            )
            assert await outbox.flush() == 3

        published_total = 0
        retried_total = 0

        for _ in range(20):
            result = await relay_outbox_claims(
                ctx,
                outbox_spec=outbox_spec,
                publish_one=flaky_publish,
                reclaim_stale_after=None,
                max_attempts=5,
                retry_base_delay=timedelta(milliseconds=10),
                retry_max_backoff=timedelta(milliseconds=40),
            )
            published_total += result.published
            retried_total += result.retried
            assert result.failed == 0

            if published_total == 3:
                break

            # Sleep past the maximum possible backoff (capped at 40ms).
            await asyncio.sleep(0.06)

        rows = await pg_client.fetch_all(
            sql.SQL(
                "SELECT status, attempts, published_at FROM {t} WHERE outbox_route = %s"
            ).format(t=sql.Identifier("public", outbox_table)),
            ("integration",),
        )

    assert published_total == 3
    assert retried_total == 6  # two reschedules per row
    assert len(rows) == 3

    for row in rows:
        assert row["status"] == OutboxStatus.PUBLISHED.value
        assert row["attempts"] == 2
        assert row["published_at"] is not None


@pytest.mark.asyncio
async def test_outbox_terminal_failure_after_max_attempts(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    runtime = _pg_runtime(pg_client, outbox_table)

    async def always_fail(claim: OutboxClaim, payload: Any) -> None:
        raise RuntimeError("broker exploded")

    async with runtime.scope():
        ctx = runtime.get_context()
        async with ctx.tx_ctx.scope("default"):
            outbox = ctx.outbox.command(outbox_spec)
            await outbox.stage("demo.created", _OutboxPayload(label="doomed"))
            assert await outbox.flush() == 1

        failed_total = 0

        for _ in range(20):
            result = await relay_outbox_claims(
                ctx,
                outbox_spec=outbox_spec,
                publish_one=always_fail,
                reclaim_stale_after=None,
                max_attempts=2,
                retry_base_delay=timedelta(milliseconds=10),
                retry_max_backoff=timedelta(milliseconds=20),
            )
            failed_total += result.failed
            assert result.published == 0

            if failed_total:
                break

            await asyncio.sleep(0.05)

        rows = await pg_client.fetch_all(
            sql.SQL("SELECT status, attempts, last_error FROM {t} WHERE outbox_route = %s").format(
                t=sql.Identifier("public", outbox_table)
            ),
            ("integration",),
        )

    assert failed_total == 1
    assert len(rows) == 1
    assert rows[0]["status"] == OutboxStatus.FAILED.value
    assert rows[0]["attempts"] == 1  # one reschedule before the terminal failure
    assert rows[0]["last_error"] is not None
    assert "broker exploded" in rows[0]["last_error"]


@pytest.mark.asyncio
async def test_outbox_requeue_failed_resets_attempts_then_drains(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    runtime = _pg_runtime(pg_client, outbox_table)
    row_id = uuid4()

    await pg_client.execute(
        sql.SQL(
            """
            INSERT INTO {t} (
                id, outbox_route, event_id, event_type, occurred_at,
                payload, status, created_at, last_error, attempts, available_at
            ) VALUES (
                %(id)s, %(route)s, %(event_id)s, %(event_type)s, %(occurred_at)s,
                %(payload)s, %(status)s, %(created_at)s, %(last_error)s,
                %(attempts)s, %(available_at)s
            )
            """
        ).format(t=sql.Identifier("public", outbox_table)),
        {
            "id": row_id,
            "route": "integration",
            "event_id": uuid4(),
            "event_type": "demo.created",
            "occurred_at": utcnow(),
            "payload": Jsonb({"label": "revived"}),
            "status": OutboxStatus.FAILED.value,
            "created_at": utcnow(),
            "last_error": "exhausted",
            "attempts": 3,
            "available_at": utcnow() + timedelta(hours=1),
        },
    )

    published: list[Any] = []

    async def record_publish(claim: OutboxClaim, payload: Any) -> None:
        published.append(payload)

    async with runtime.scope():
        ctx = runtime.get_context()
        query = ctx.outbox.query(outbox_spec)
        assert await query.requeue_failed([row_id]) == 1

        rows = await pg_client.fetch_all(
            sql.SQL(
                "SELECT status, attempts, available_at, last_error FROM {t} WHERE id = %s"
            ).format(t=sql.Identifier("public", outbox_table)),
            (row_id,),
        )
        assert rows[0]["status"] == OutboxStatus.PENDING.value
        assert rows[0]["attempts"] == 0
        assert rows[0]["available_at"] is None
        assert rows[0]["last_error"] is None

        result = await relay_outbox_claims(
            ctx,
            outbox_spec=outbox_spec,
            publish_one=record_publish,
            reclaim_stale_after=None,
        )

    assert result.published == 1
    assert published == [_OutboxPayload(label="revived")]


@pytest.mark.asyncio
async def test_outbox_future_available_at_invisible_to_claim(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    runtime = _pg_runtime(pg_client, outbox_table)
    row_id = uuid4()

    await pg_client.execute(
        sql.SQL(
            """
            INSERT INTO {t} (
                id, outbox_route, event_id, event_type, occurred_at,
                payload, status, created_at, attempts, available_at
            ) VALUES (
                %(id)s, %(route)s, %(event_id)s, %(event_type)s, %(occurred_at)s,
                %(payload)s, %(status)s, %(created_at)s, %(attempts)s,
                %(available_at)s
            )
            """
        ).format(t=sql.Identifier("public", outbox_table)),
        {
            "id": row_id,
            "route": "integration",
            "event_id": uuid4(),
            "event_type": "demo.created",
            "occurred_at": utcnow(),
            "payload": Jsonb({"label": "later"}),
            "status": OutboxStatus.PENDING.value,
            "created_at": utcnow(),
            "attempts": 1,
            "available_at": utcnow() + timedelta(hours=1),
        },
    )

    async with runtime.scope():
        ctx = runtime.get_context()
        claims = await ctx.outbox.query(outbox_spec).claim_pending()

        rows = await pg_client.fetch_all(
            sql.SQL("SELECT status FROM {t} WHERE id = %s").format(
                t=sql.Identifier("public", outbox_table)
            ),
            (row_id,),
        )

    assert list(claims) == []
    assert rows[0]["status"] == OutboxStatus.PENDING.value


@pytest.mark.asyncio
async def test_hlc_ordering_persists_and_claims_in_causal_order(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    """With hlc_ordering on, a single-batch flush (shared created_at) still
    claims in stamp order: the HLC breaks the otherwise-arbitrary tie."""

    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    pg_module = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={
            "integration": PostgresOutboxConfig(
                relation=("public", outbox_table),
                hlc_ordering=True,
            ),
        },
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(pg_module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        async with ctx.tx_ctx.scope("default"):
            outbox = ctx.outbox.command(outbox_spec)
            await outbox.stage("demo.a", _OutboxPayload(label="a"))
            await outbox.stage("demo.b", _OutboxPayload(label="b"))
            await outbox.stage("demo.c", _OutboxPayload(label="c"))
            assert await outbox.flush() == 3

        # The hlc column is persisted for every staged row.
        hlc_rows = await pg_client.fetch_all(
            sql.SQL("SELECT hlc FROM {t} WHERE outbox_route = %s").format(
                t=sql.Identifier("public", outbox_table)
            ),
            ["integration"],
        )
        assert all(row["hlc"] is not None for row in hlc_rows)

        async with ctx.tx_ctx.scope("default"):
            claims = list(await ctx.outbox.query(outbox_spec).claim_pending())

    # Claimed in stamp order (a, b, c) with strictly ascending HLCs.
    assert [c.event_type for c in claims] == ["demo.a", "demo.b", "demo.c"]
    assert all(c.hlc is not None for c in claims)
    packed = [c.hlc.pack() for c in claims]  # type: ignore[union-attr]
    assert packed == sorted(packed)
    assert len(set(packed)) == 3


@pytest.mark.asyncio
async def test_propagate_trace_persists_and_round_trips(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    """With propagate_trace on, the publishing span's W3C traceparent persists on the row and
    round-trips back on the claim (so the relay can forward it for cross-async trace linkage)."""

    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    from forze.application.execution.tracing.propagation import current_traceparent

    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    pg_module = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={
            "integration": PostgresOutboxConfig(
                relation=("public", outbox_table),
                propagate_trace=True,
            ),
        },
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(pg_module).freeze())

    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(InMemorySpanExporter()))
    tracer = provider.get_tracer("test")

    async with runtime.scope():
        ctx = runtime.get_context()

        with tracer.start_as_current_span("publish"):
            expected = current_traceparent()
            async with ctx.tx_ctx.scope("default"):
                await ctx.outbox.command(outbox_spec).stage("demo.a", _OutboxPayload(label="a"))
                assert await ctx.outbox.command(outbox_spec).flush() == 1

        assert expected is not None

        # Persisted on the row exactly as captured at staging.
        rows = await pg_client.fetch_all(
            sql.SQL("SELECT traceparent FROM {t} WHERE outbox_route = %s").format(
                t=sql.Identifier("public", outbox_table)
            ),
            ["integration"],
        )
        assert [row["traceparent"] for row in rows] == [expected]

        async with ctx.tx_ctx.scope("default"):
            claims = list(await ctx.outbox.query(outbox_spec).claim_pending())

    assert len(claims) == 1
    assert claims[0].traceparent == expected


@pytest.mark.asyncio
async def test_outbox_relays_to_module_wired_redis_stream(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    outbox_table: str,
) -> None:
    """Production shape: a Postgres outbox relays to a Redis stream wired via RedisDepsModule."""

    codec = PydanticModelCodec(_OutboxPayload)
    channel = f"it:outbox:stream:{uuid4().hex[:8]}"
    outbox_spec = OutboxSpec(
        name="integration",
        codec=codec,
        destination=OutboxDestination.stream(route=channel, channel=channel),
    )
    stream_spec = StreamSpec(name=channel, codec=codec)

    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(
            PostgresDepsModule(
                client=pg_client,
                tx={"default"},
                outboxes={
                    "integration": PostgresOutboxConfig(relation=("public", outbox_table)),
                },
            ),
            RedisDepsModule(
                client=redis_client,
                streams={channel: RedisStreamConfig(tenant_aware=False)},
                stream_groups={channel: RedisStreamGroupConfig(tenant_aware=False)},
            ),
        ).freeze()
    )

    async with runtime.scope():
        ctx = runtime.get_context()

        async with ctx.tx_ctx.scope("default"):
            await ctx.outbox.command(outbox_spec).stage(
                "demo.created", _OutboxPayload(label="from-pg-outbox")
            )
            await ctx.outbox.command(outbox_spec).flush()

        result = await OutboxRelay(outbox_spec=outbox_spec, reclaim_stale_after=None).to_stream(
            ctx, stream_spec
        )
        assert result.published == 1

        # The event landed in the real Redis stream, read back via the module's query port.
        query = ctx.deps.resolve_configurable(
            ctx, StreamQueryDepKey, stream_spec, route=stream_spec.name
        )
        messages = await query.read({channel: "0"}, limit=10)

        assert len(messages) == 1
        assert messages[0].payload.label == "from-pg-outbox"
        assert messages[0].type == "demo.created"


# ----------------------- #
# Relay lifecycle: drain on shutdown
#
# covers: forze_kits.integrations.outbox.outbox_relay_background_lifecycle_step


def _drain_relay_runtime(
    pg_client: PostgresClient,
    outbox_table: str,
    shared_state: MockState,
    *,
    drain_on_shutdown: bool,
) -> tuple[ExecutionRuntime, OutboxSpec[_OutboxPayload]]:
    """A runtime whose only lifecycle step is the background relay for *outbox_table*."""

    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    queue_spec = QueueSpec(name="relay", codec=codec)
    pg_module = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={
            "integration": PostgresOutboxConfig(relation=("public", outbox_table)),
        },
    )

    # The drain reads and writes the outbox *during* teardown, so the relay must be torn
    # down before whatever owns the client. Declaring the capability is what puts it in a
    # later wave; the stub stands in for the real pool step, which would close the pool this
    # test still reads from afterwards.
    client_step = LifecycleStep(id="pg_client_stub", provides=(POSTGRES_CLIENT_CAPABILITY,))
    relay_step = outbox_relay_background_lifecycle_step(
        outbox_spec=outbox_spec,
        queue_spec=queue_spec,
        interval=timedelta(hours=1),  # no tick of its own: only the drain can publish
        reclaim_stale_after=None,
        drain_on_shutdown=drain_on_shutdown,
        requires=(POSTGRES_CLIENT_CAPABILITY,) if drain_on_shutdown else (),
    )

    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(pg_module)
        .with_deps(_mock_queue_deps(shared_state))
        .freeze(),
        lifecycle=LifecyclePlan.from_steps(client_step, relay_step).freeze(),
    )

    return runtime, outbox_spec


async def _stage_after_first_tick(
    runtime: ExecutionRuntime,
    outbox_spec: OutboxSpec[_OutboxPayload],
) -> None:
    """Run a scope that stages one row *after* the relay's first tick has come up empty."""

    async with runtime.scope():
        ctx = runtime.get_context()
        await asyncio.sleep(0.05)

        async with ctx.tx_ctx.scope("default"):
            outbox = ctx.outbox.command(outbox_spec)
            await outbox.stage("demo.created", _OutboxPayload(label="drain"))
            await outbox.flush()
    # Scope exit runs lifecycle shutdown — the only thing that can publish this row.


@pytest.mark.asyncio
async def test_relay_drains_the_outbox_on_shutdown(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    shared_state = MockState()
    runtime, outbox_spec = _drain_relay_runtime(
        pg_client, outbox_table, shared_state, drain_on_shutdown=True
    )

    await _stage_after_first_tick(runtime, outbox_spec)

    rows = await pg_client.fetch_all(
        sql.SQL("SELECT status FROM {t} WHERE outbox_route = %s").format(
            t=sql.Identifier("public", outbox_table)
        ),
        ("integration",),
    )

    assert [row["status"] for row in rows] == ["published"]
    assert len(shared_state.queues["relay"]["relay"]) == 1  # it really reached the queue


@pytest.mark.asyncio
async def test_relay_without_drain_leaves_the_row_for_the_next_process(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    # The default. Together with the test above this is the whole feature: the row is either
    # published at teardown, or it waits — nothing in between, and never stuck 'processing'.
    shared_state = MockState()
    runtime, outbox_spec = _drain_relay_runtime(
        pg_client, outbox_table, shared_state, drain_on_shutdown=False
    )

    await _stage_after_first_tick(runtime, outbox_spec)

    rows = await pg_client.fetch_all(
        sql.SQL("SELECT status FROM {t} WHERE outbox_route = %s").format(
            t=sql.Identifier("public", outbox_table)
        ),
        ("integration",),
    )

    assert [row["status"] for row in rows] == ["pending"]
    assert shared_state.queues == {}


# ----------------------- #
# Admin (observability) port
#
# covers: OutboxAdminPort.has_undrained
# covers: OutboxAdminPort.depth
# covers: OutboxAdminPort.oldest_pending_age


async def _seed_row(
    pg_client: PostgresClient,
    outbox_table: str,
    *,
    status: OutboxStatus,
    created_at: Any,
    available_at: Any = None,
) -> None:
    await pg_client.execute(
        sql.SQL(
            """
            INSERT INTO {t} (
                id, outbox_route, event_id, event_type, occurred_at, payload,
                status, created_at, available_at, attempts
            )
            VALUES (%s, 'integration', %s, 'demo.created', %s, %s, %s, %s, %s, 0)
            """
        ).format(t=sql.Identifier("public", outbox_table)),
        (
            uuid4(),
            uuid4(),
            created_at,
            Jsonb({"label": "x"}),
            status.value,
            created_at,
            available_at,
        ),
    )


def _admin_runtime(
    pg_client: PostgresClient, outbox_table: str
) -> tuple[ExecutionRuntime, OutboxSpec[Any]]:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    pg_module = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={
            "integration": PostgresOutboxConfig(relation=("public", outbox_table)),
        },
    )

    return ExecutionRuntime(deps=DepsRegistry.from_modules(pg_module).freeze()), outbox_spec


@pytest.mark.asyncio
async def test_outbox_depth_counts_undrained_and_ignores_published(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    now = utcnow()

    for status in (
        OutboxStatus.PENDING,
        OutboxStatus.PENDING,
        OutboxStatus.PROCESSING,
        OutboxStatus.FAILED,
        OutboxStatus.PUBLISHED,
    ):
        await _seed_row(pg_client, outbox_table, status=status, created_at=now)

    runtime, outbox_spec = _admin_runtime(pg_client, outbox_table)

    async with runtime.scope():
        admin = runtime.get_context().outbox.admin(outbox_spec)
        depth = await admin.depth()
        undrained = await admin.has_undrained()

    assert (depth.pending, depth.processing, depth.failed) == (2, 1, 1)
    assert depth.undrained == 3  # failed is parked, not on its way out
    assert undrained is True


@pytest.mark.asyncio
async def test_outbox_depth_counts_rows_parked_for_a_future_retry(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    # The semantic that makes quiesce trustworthy: `claim_pending` hides a row backing off,
    # but it is still undelivered work. A depth that agreed with the claim would attest an
    # empty outbox while events were queued behind a retry.
    now = utcnow()
    await _seed_row(
        pg_client,
        outbox_table,
        status=OutboxStatus.PENDING,
        created_at=now,
        available_at=now + timedelta(hours=1),
    )

    runtime, outbox_spec = _admin_runtime(pg_client, outbox_table)

    async with runtime.scope():
        ctx = runtime.get_context()
        depth = await ctx.outbox.admin(outbox_spec).depth()
        claimed = await ctx.outbox.query(outbox_spec).claim_pending()

    assert depth.pending == 1
    assert not depth.is_empty
    assert claimed == []  # invisible to the claim path, visible to the depth


@pytest.mark.asyncio
async def test_outbox_admin_is_empty_and_ageless_when_everything_is_published(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    await _seed_row(pg_client, outbox_table, status=OutboxStatus.PUBLISHED, created_at=utcnow())

    runtime, outbox_spec = _admin_runtime(pg_client, outbox_table)

    async with runtime.scope():
        admin = runtime.get_context().outbox.admin(outbox_spec)

        assert await admin.has_undrained() is False
        assert (await admin.depth()).is_empty
        assert await admin.oldest_pending_age() is None


@pytest.mark.asyncio
async def test_outbox_oldest_pending_age_tracks_the_head_of_the_backlog(
    pg_client: PostgresClient,
    outbox_table: str,
) -> None:
    now = utcnow()
    await _seed_row(
        pg_client, outbox_table, status=OutboxStatus.PENDING, created_at=now - timedelta(seconds=30)
    )
    await _seed_row(
        pg_client,
        outbox_table,
        status=OutboxStatus.PENDING,
        created_at=now - timedelta(seconds=120),
    )

    runtime, outbox_spec = _admin_runtime(pg_client, outbox_table)

    async with runtime.scope():
        age = await runtime.get_context().outbox.admin(outbox_spec).oldest_pending_age()

    assert age is not None
    assert timedelta(seconds=110) < age < timedelta(seconds=130)

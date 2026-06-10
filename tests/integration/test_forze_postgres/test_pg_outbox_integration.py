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
from datetime import timedelta
from typing import Any
from uuid import uuid4

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
from forze.application.execution import Deps, DepsRegistry, ExecutionRuntime
from forze.base.primitives import utcnow
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import relay_outbox_to_queue
from forze_kits.integrations.outbox._relay_core import relay_outbox_claims
from forze_mock import MockStateDepKey
from forze_mock.adapters import MockState
from forze_mock.execution.module import ConfigurableMockQueue, MockDepsModule
from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.execution.deps.configs import PostgresOutboxConfig
from forze_postgres.kernel.client import PostgresClient


class _OutboxPayload(BaseModel):
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
                UNIQUE (outbox_route, event_id)
            )
            """
        ).format(table=sql.Identifier(schema, table))
    )

    yield table

    await pg_client.execute(
        sql.SQL("DROP TABLE IF EXISTS {table}").format(
            table=sql.Identifier(schema, table)
        )
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
            sql.SQL("SELECT id FROM {t}").format(
                t=sql.Identifier("public", outbox_table)
            )
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

        result = await relay_outbox_to_queue(
            ctx,
            outbox_spec=outbox_spec,
            queue_spec=queue_spec,
        )

    assert result.published == 1
    state = shared_state

    assert len(state.queues["relay"]["relay"]) == 1


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

        result = await relay_outbox_to_queue(
            ctx,
            outbox_spec=outbox_spec,
            queue_spec=queue_spec,
            reclaim_stale_after=timedelta(minutes=5),
        )

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

        result = await relay_outbox_to_queue(
            ctx,
            outbox_spec=outbox_spec,
            queue_spec=queue_spec,
        )

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
            sql.SQL(
                "SELECT status, attempts, last_error FROM {t} WHERE outbox_route = %s"
            ).format(t=sql.Identifier("public", outbox_table)),
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
                "SELECT status, attempts, available_at, last_error FROM {t} "
                "WHERE id = %s"
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

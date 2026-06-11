"""Integration tests for Mongo outbox adapter.

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
from forze_mongo.execution.deps import MongoDepsModule
from forze_mongo.execution.deps.configs import MongoOutboxConfig
from forze_mongo.kernel.client import MongoClient


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
async def outbox_collection(
    mongo_client_replica: MongoClient,
) -> tuple[str, str]:
    """Create a dedicated outbox collection with indexes; returns (db_name, coll_name)."""

    db = await mongo_client_replica.db()
    db_name = db.name
    coll_name = f"outbox_{uuid4().hex[:8]}"
    coll = await mongo_client_replica.collection(coll_name, db_name=db_name)

    await coll.create_index([("outbox_route", 1), ("event_id", 1)], unique=True)
    await coll.create_index(
        [("outbox_route", 1), ("status", 1), ("available_at", 1), ("created_at", 1)]
    )
    await coll.create_index([("outbox_route", 1), ("status", 1), ("processing_at", 1)])

    yield db_name, coll_name

    await coll.drop()


@pytest.mark.asyncio
async def test_mongo_outbox_flush_commits_with_transaction(
    mongo_client_replica: MongoClient,
    outbox_collection: tuple[str, str],
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    db_name, coll_name = outbox_collection
    mongo_module = MongoDepsModule(
        client=mongo_client_replica,
        tx={"default"},
        outboxes={
            "integration": MongoOutboxConfig(
                collection=(db_name, coll_name),
            ),
        },
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(mongo_module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        async with ctx.tx_ctx.scope("default"):
            outbox = ctx.outbox.command(outbox_spec)
            await outbox.stage("demo.created", _OutboxPayload(label="ok"))
            assert await outbox.flush() == 1

        coll = await mongo_client_replica.collection(coll_name, db_name=db_name)
        rows = await mongo_client_replica.find_many(
            coll,
            {"outbox_route": "integration"},
        )

    assert len(rows) == 1
    assert rows[0]["status"] == OutboxStatus.PENDING.value


@pytest.mark.asyncio
async def test_mongo_outbox_rollback_discards_staged_rows(
    mongo_client_replica: MongoClient,
    outbox_collection: tuple[str, str],
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    db_name, coll_name = outbox_collection
    mongo_module = MongoDepsModule(
        client=mongo_client_replica,
        tx={"default"},
        outboxes={
            "integration": MongoOutboxConfig(
                collection=(db_name, coll_name),
            ),
        },
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(mongo_module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        with pytest.raises(RuntimeError):
            async with ctx.tx_ctx.scope("default"):
                await ctx.outbox.command(outbox_spec).stage(
                    "demo.created",
                    _OutboxPayload(label="rollback"),
                )
                raise RuntimeError("abort")

        coll = await mongo_client_replica.collection(coll_name, db_name=db_name)
        rows = await mongo_client_replica.find_many(coll, {})

    assert rows == []


@pytest.mark.asyncio
async def test_mongo_outbox_bulk_flush(
    mongo_client_replica: MongoClient,
    outbox_collection: tuple[str, str],
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    db_name, coll_name = outbox_collection
    mongo_module = MongoDepsModule(
        client=mongo_client_replica,
        tx={"default"},
        outboxes={
            "integration": MongoOutboxConfig(
                collection=(db_name, coll_name),
            ),
        },
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(mongo_module).freeze())

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

        coll = await mongo_client_replica.collection(coll_name, db_name=db_name)
        rows = await mongo_client_replica.find_many(
            coll,
            {"outbox_route": "integration"},
        )

    assert len(rows) == 3


@pytest.mark.asyncio
async def test_mongo_outbox_duplicate_event_id_flush_is_idempotent(
    mongo_client_replica: MongoClient,
    outbox_collection: tuple[str, str],
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    db_name, coll_name = outbox_collection
    mongo_module = MongoDepsModule(
        client=mongo_client_replica,
        tx={"default"},
        outboxes={
            "integration": MongoOutboxConfig(
                collection=(db_name, coll_name),
            ),
        },
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(mongo_module).freeze())
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

    db_name, coll_name = outbox_collection
    coll = await mongo_client_replica.collection(coll_name, db_name=db_name)
    rows = await mongo_client_replica.find_many(coll, {"event_id": str(event_id)})

    assert len(rows) == 1
    assert rows[0]["payload"]["label"] == "first"


@pytest.mark.asyncio
async def test_mongo_outbox_relay_to_mock_queue(
    mongo_client_replica: MongoClient,
    outbox_collection: tuple[str, str],
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(
        name="integration",
        codec=codec,
        destination=OutboxDestination.queue(route="relay", channel="relay"),
    )
    queue_spec = QueueSpec(name="relay", codec=codec)
    db_name, coll_name = outbox_collection
    mongo_module = MongoDepsModule(
        client=mongo_client_replica,
        tx={"default"},
        outboxes={
            "integration": MongoOutboxConfig(
                collection=(db_name, coll_name),
            ),
        },
    )
    shared_state = MockState()
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(mongo_module)
        .with_deps(_mock_queue_deps(shared_state))
        .freeze(),
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
    assert len(shared_state.queues["relay"]["relay"]) == 1


@pytest.mark.asyncio
async def test_mongo_outbox_relay_reclaims_stale_processing(
    mongo_client_replica: MongoClient,
    outbox_collection: tuple[str, str],
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(
        name="integration",
        codec=codec,
        destination=OutboxDestination.queue(route="relay", channel="relay"),
    )
    queue_spec = QueueSpec(name="relay", codec=codec)
    db_name, coll_name = outbox_collection
    mongo_module = MongoDepsModule(
        client=mongo_client_replica,
        tx={"default"},
        outboxes={
            "integration": MongoOutboxConfig(
                collection=(db_name, coll_name),
            ),
        },
    )
    shared_state = MockState()
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(mongo_module)
        .with_deps(_mock_queue_deps(shared_state))
        .freeze(),
    )
    row_id = uuid4()
    event_id = uuid4()
    stale_at = utcnow() - timedelta(hours=1)

    db_name, coll_name = outbox_collection
    coll = await mongo_client_replica.collection(coll_name, db_name=db_name)
    await mongo_client_replica.insert_one(
        coll,
        {
            "id": str(row_id),
            "outbox_route": "integration",
            "event_id": str(event_id),
            "event_type": "demo.created",
            "occurred_at": utcnow(),
            "payload": {"label": "stale"},
            "status": OutboxStatus.PROCESSING.value,
            "created_at": utcnow(),
            "processing_at": stale_at,
            "published_at": None,
            "last_error": None,
            "tenant_id": None,
            "execution_id": None,
            "correlation_id": None,
            "causation_id": None,
        },
    )

    async with runtime.scope():
        ctx = runtime.get_context()
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
async def test_mongo_outbox_requeue_failed_then_relay(
    mongo_client_replica: MongoClient,
    outbox_collection: tuple[str, str],
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(
        name="integration",
        codec=codec,
        destination=OutboxDestination.queue(route="relay", channel="relay"),
    )
    queue_spec = QueueSpec(name="relay", codec=codec)
    db_name, coll_name = outbox_collection
    mongo_module = MongoDepsModule(
        client=mongo_client_replica,
        tx={"default"},
        outboxes={
            "integration": MongoOutboxConfig(
                collection=(db_name, coll_name),
            ),
        },
    )
    shared_state = MockState()
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(mongo_module)
        .with_deps(_mock_queue_deps(shared_state))
        .freeze(),
    )
    row_id = uuid4()
    event_id = uuid4()

    db_name, coll_name = outbox_collection
    coll = await mongo_client_replica.collection(coll_name, db_name=db_name)
    await mongo_client_replica.insert_one(
        coll,
        {
            "id": str(row_id),
            "outbox_route": "integration",
            "event_id": str(event_id),
            "event_type": "demo.created",
            "occurred_at": utcnow(),
            "payload": {"label": "failed"},
            "status": OutboxStatus.FAILED.value,
            "created_at": utcnow(),
            "processing_at": None,
            "published_at": None,
            "last_error": "boom",
            "tenant_id": None,
            "execution_id": None,
            "correlation_id": None,
            "causation_id": None,
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


def _mongo_runtime(
    mongo_client_replica: MongoClient,
    outbox_collection: tuple[str, str],
) -> ExecutionRuntime:
    db_name, coll_name = outbox_collection
    mongo_module = MongoDepsModule(
        client=mongo_client_replica,
        tx={"default"},
        outboxes={
            "integration": MongoOutboxConfig(
                collection=(db_name, coll_name),
            ),
        },
    )
    return ExecutionRuntime(deps=DepsRegistry.from_modules(mongo_module).freeze())


def _outbox_doc(row_id: Any, **overrides: Any) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "id": str(row_id),
        "outbox_route": "integration",
        "event_id": str(uuid4()),
        "event_type": "demo.created",
        "occurred_at": utcnow(),
        "payload": {"label": "row"},
        "status": OutboxStatus.PENDING.value,
        "created_at": utcnow(),
        "processing_at": None,
        "published_at": None,
        "last_error": None,
        "tenant_id": None,
        "execution_id": None,
        "correlation_id": None,
        "causation_id": None,
        "attempts": 0,
        "available_at": None,
    }
    doc.update(overrides)
    return doc


@pytest.mark.asyncio
async def test_mongo_outbox_retry_cycle_publishes_after_transient_failures(
    mongo_client_replica: MongoClient,
    outbox_collection: tuple[str, str],
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    runtime = _mongo_runtime(mongo_client_replica, outbox_collection)
    db_name, coll_name = outbox_collection

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

        coll = await mongo_client_replica.collection(coll_name, db_name=db_name)
        rows = await mongo_client_replica.find_many(
            coll,
            {"outbox_route": "integration"},
        )

    assert published_total == 3
    assert retried_total == 6  # two reschedules per row
    assert len(rows) == 3

    for row in rows:
        assert row["status"] == OutboxStatus.PUBLISHED.value
        assert row["attempts"] == 2
        assert row["published_at"] is not None


@pytest.mark.asyncio
async def test_mongo_outbox_terminal_failure_after_max_attempts(
    mongo_client_replica: MongoClient,
    outbox_collection: tuple[str, str],
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    runtime = _mongo_runtime(mongo_client_replica, outbox_collection)
    db_name, coll_name = outbox_collection

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

        coll = await mongo_client_replica.collection(coll_name, db_name=db_name)
        rows = await mongo_client_replica.find_many(
            coll,
            {"outbox_route": "integration"},
        )

    assert failed_total == 1
    assert len(rows) == 1
    assert rows[0]["status"] == OutboxStatus.FAILED.value
    assert rows[0]["attempts"] == 1  # one reschedule before the terminal failure
    assert rows[0]["last_error"] is not None
    assert "broker exploded" in rows[0]["last_error"]


@pytest.mark.asyncio
async def test_mongo_outbox_requeue_failed_resets_attempts_then_drains(
    mongo_client_replica: MongoClient,
    outbox_collection: tuple[str, str],
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    runtime = _mongo_runtime(mongo_client_replica, outbox_collection)
    db_name, coll_name = outbox_collection
    row_id = uuid4()

    coll = await mongo_client_replica.collection(coll_name, db_name=db_name)
    await mongo_client_replica.insert_one(
        coll,
        _outbox_doc(
            row_id,
            payload={"label": "revived"},
            status=OutboxStatus.FAILED.value,
            last_error="exhausted",
            attempts=3,
            available_at=utcnow() + timedelta(hours=1),
        ),
    )

    published: list[Any] = []

    async def record_publish(claim: OutboxClaim, payload: Any) -> None:
        published.append(payload)

    async with runtime.scope():
        ctx = runtime.get_context()
        query = ctx.outbox.query(outbox_spec)
        assert await query.requeue_failed([row_id]) == 1

        rows = await mongo_client_replica.find_many(coll, {"id": str(row_id)})
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
async def test_mongo_outbox_future_available_at_invisible_to_claim(
    mongo_client_replica: MongoClient,
    outbox_collection: tuple[str, str],
) -> None:
    codec = PydanticModelCodec(_OutboxPayload)
    outbox_spec = OutboxSpec(name="integration", codec=codec)
    runtime = _mongo_runtime(mongo_client_replica, outbox_collection)
    db_name, coll_name = outbox_collection
    row_id = uuid4()

    coll = await mongo_client_replica.collection(coll_name, db_name=db_name)
    await mongo_client_replica.insert_one(
        coll,
        _outbox_doc(
            row_id,
            payload={"label": "later"},
            attempts=1,
            available_at=utcnow() + timedelta(hours=1),
        ),
    )

    async with runtime.scope():
        ctx = runtime.get_context()
        claims = await ctx.outbox.query(outbox_spec).claim_pending()

        rows = await mongo_client_replica.find_many(coll, {"id": str(row_id)})

    assert list(claims) == []
    assert rows[0]["status"] == OutboxStatus.PENDING.value

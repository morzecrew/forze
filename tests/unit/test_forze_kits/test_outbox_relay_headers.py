"""Relay forwards the staged invocation envelope as transport headers."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.envelope import (
    HEADER_CAUSATION_ID,
    HEADER_CORRELATION_ID,
    HEADER_EVENT_ID,
    HEADER_EXECUTION_ID,
    HEADER_OCCURRED_AT,
    HEADER_TENANT_ID,
)
from forze.application.contracts.outbox import (
    OutboxDestination,
    OutboxSpec,
    OutboxStatus,
)
from forze.application.contracts.pubsub import PubSubSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.contracts.stream import StreamSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import (
    DepsRegistry,
    ExecutionRuntime,
    InvocationMetadata,
)
from forze.base.primitives import utcnow, uuid7
from forze_kits.integrations.outbox import (
    OutboxRelay,
)
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.adapters.outbox import MockOutboxRow

# ----------------------- #


class _EventPayload(BaseModel):
    n: int


def _codec():
    from forze.base.serialization import PydanticModelCodec

    return PydanticModelCodec(_EventPayload)


# ----------------------- #


@pytest.mark.asyncio
async def test_relay_to_queue_forwards_full_envelope_headers() -> None:
    codec = _codec()
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.queue(route="jobs", channel="jobs"),
    )
    queue_spec = QueueSpec(name="jobs", codec=codec)

    event_id = uuid4()
    correlation_id = uuid4()
    causation_id = uuid4()
    execution_id = uuid4()
    tenant_id = uuid4()
    occurred_at = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)
        state.outbox_rows["events"] = [
            MockOutboxRow(
                id=uuid4(),
                outbox_route="events",
                event_id=event_id,
                event_type="job.requested",
                payload={"n": 7},
                status=OutboxStatus.PENDING,
                tenant_id=tenant_id,
                execution_id=execution_id,
                correlation_id=correlation_id,
                causation_id=causation_id,
                occurred_at=occurred_at,
                created_at=utcnow(),
            )
        ]

        result = await OutboxRelay(
            outbox_spec=outbox_spec, reclaim_stale_after=None
        ).to_queue(ctx, queue_spec)

        assert result.published == 1

        message = state.queues["jobs"]["jobs"][0].message
        # type/key stay exactly as before (back-compat).
        assert message.type == "job.requested"
        assert message.key == str(event_id)

        assert dict(message.headers) == {
            HEADER_EVENT_ID: str(event_id),
            HEADER_OCCURRED_AT: occurred_at.isoformat(),
            HEADER_CORRELATION_ID: str(correlation_id),
            HEADER_CAUSATION_ID: str(causation_id),
            HEADER_EXECUTION_ID: str(execution_id),
            HEADER_TENANT_ID: str(tenant_id),
        }


@pytest.mark.asyncio
async def test_relay_omits_unset_envelope_fields() -> None:
    codec = _codec()
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.queue(route="jobs", channel="jobs"),
    )
    queue_spec = QueueSpec(name="jobs", codec=codec)
    event_id = uuid4()

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)
        occurred_at = utcnow()
        state.outbox_rows["events"] = [
            MockOutboxRow(
                id=uuid4(),
                outbox_route="events",
                event_id=event_id,
                event_type="job.requested",
                payload={"n": 7},
                status=OutboxStatus.PENDING,
                tenant_id=None,
                execution_id=None,
                correlation_id=None,
                causation_id=None,
                occurred_at=occurred_at,
                created_at=utcnow(),
            )
        ]

        await OutboxRelay(
            outbox_spec=outbox_spec, reclaim_stale_after=None
        ).to_queue(ctx, queue_spec)

        message = state.queues["jobs"]["jobs"][0].message
        assert dict(message.headers) == {
            HEADER_EVENT_ID: str(event_id),
            HEADER_OCCURRED_AT: occurred_at.isoformat(),
        }


@pytest.mark.asyncio
async def test_staged_event_envelope_reaches_queue_headers_via_inv_ctx() -> None:
    """In-process slice of the tracing flow: bind -> stage -> relay -> headers."""

    codec = _codec()
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.queue(route="jobs", channel="jobs"),
    )
    queue_spec = QueueSpec(name="jobs", codec=codec)

    metadata = InvocationMetadata(
        execution_id=uuid7(),
        correlation_id=uuid7(),
        causation_id=uuid7(),
    )
    tenant = TenantIdentity(tenant_id=uuid4())

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)

        with ctx.inv_ctx.bind(metadata=metadata, tenant=tenant):
            await ctx.outbox.command(outbox_spec).stage(
                "job.requested", _EventPayload(n=1)
            )
            await ctx.outbox.command(outbox_spec).flush()

        await OutboxRelay(
            outbox_spec=outbox_spec, reclaim_stale_after=None
        ).to_queue(ctx, queue_spec)

        message = state.queues["jobs"]["jobs"][0].message
        assert message.headers[HEADER_CORRELATION_ID] == str(metadata.correlation_id)
        assert message.headers[HEADER_CAUSATION_ID] == str(metadata.causation_id)
        assert message.headers[HEADER_EXECUTION_ID] == str(metadata.execution_id)
        assert message.headers[HEADER_TENANT_ID] == str(tenant.tenant_id)
        assert message.headers[HEADER_EVENT_ID] == message.key


@pytest.mark.asyncio
async def test_relay_to_stream_and_pubsub_forward_envelope_headers() -> None:
    codec = _codec()
    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)
        correlation_id = uuid4()

        def _row(route: str, event_id) -> MockOutboxRow:
            return MockOutboxRow(
                id=uuid4(),
                outbox_route=route,
                event_id=event_id,
                event_type="thing.happened",
                payload={"n": 1},
                status=OutboxStatus.PENDING,
                tenant_id=None,
                execution_id=None,
                correlation_id=correlation_id,
                causation_id=None,
                occurred_at=utcnow(),
                created_at=utcnow(),
            )

        stream_event = uuid4()
        pubsub_event = uuid4()
        state.outbox_rows["audit-events"] = [_row("audit-events", stream_event)]
        state.outbox_rows["fanout-events"] = [_row("fanout-events", pubsub_event)]

        stream_outbox = OutboxSpec(
            name="audit-events",
            codec=codec,
            destination=OutboxDestination.stream(route="audit", channel="audit"),
        )
        await OutboxRelay(
            outbox_spec=stream_outbox, reclaim_stale_after=None
        ).to_stream(ctx, StreamSpec(name="audit", codec=codec))

        pubsub_outbox = OutboxSpec(
            name="fanout-events",
            codec=codec,
            destination=OutboxDestination.pubsub(route="fanout", channel="fanout"),
        )
        await OutboxRelay(
            outbox_spec=pubsub_outbox, reclaim_stale_after=None
        ).to_pubsub(ctx, PubSubSpec(name="fanout", codec=codec))

        [stream_message] = state.streams["audit"]["audit"]
        assert stream_message.headers[HEADER_EVENT_ID] == str(stream_event)
        assert stream_message.headers[HEADER_CORRELATION_ID] == str(correlation_id)

        [pubsub_message] = state.pubsub_logs["fanout"]["fanout"]
        assert pubsub_message.headers[HEADER_EVENT_ID] == str(pubsub_event)
        assert pubsub_message.headers[HEADER_CORRELATION_ID] == str(correlation_id)

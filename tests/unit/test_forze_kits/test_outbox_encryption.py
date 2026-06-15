"""End-to-end outbox payload encryption: ciphertext at rest, plaintext after relay."""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import (
    OutboxDestination,
    OutboxSpec,
    OutboxStatus,
)
from forze.application.contracts.queue import QueueSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.application.integrations.outbox import is_encrypted_payload
from forze.base.primitives import utcnow
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import (
    outbox_flush_tx_on_success_factory,
    relay_outbox_to_queue,
)
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.outbox_adapter import MockOutboxRow

# ----------------------- #


class _EventPayload(BaseModel):
    n: int


def _specs(*, encrypt: bool) -> tuple[OutboxSpec[_EventPayload], QueueSpec[_EventPayload]]:
    codec = PydanticModelCodec(_EventPayload)
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.queue(route="jobs", channel="jobs"),
        encrypt=encrypt,
    )
    return outbox_spec, QueueSpec(name="jobs", codec=codec)


# ....................... #


@pytest.mark.asyncio
async def test_payload_encrypted_at_rest_then_decrypted_on_relay() -> None:
    outbox_spec, queue_spec = _specs(encrypt=True)
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()

        await ctx.outbox.command(outbox_spec).stage("job.requested", _EventPayload(n=7))
        await outbox_flush_tx_on_success_factory(outbox_spec)(ctx)(0, 0)

        state = ctx.deps.provide(MockStateDepKey)
        stored = state.outbox_rows["events"][0].payload

        # Ciphertext at rest: the stored payload is an envelope wrapper, not {"n": 7}.
        assert is_encrypted_payload(stored)
        assert "n" not in stored

        result = await relay_outbox_to_queue(
            ctx, outbox_spec=outbox_spec, queue_spec=queue_spec
        )

        # The relay decrypted, decoded, and published the plaintext model.
        assert result.published == 1
        entry = state.queues["jobs"]["jobs"][0]
        assert entry.message.payload == _EventPayload(n=7)


@pytest.mark.asyncio
async def test_relay_tolerates_legacy_plaintext_rows() -> None:
    """A plaintext row written before encryption was enabled still relays."""

    outbox_spec, queue_spec = _specs(encrypt=True)
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)
        state.outbox_rows["events"] = [
            MockOutboxRow(
                id=uuid4(),
                outbox_route="events",
                event_id=uuid4(),
                event_type="job.requested",
                payload={"n": 9},  # legacy plaintext, no envelope wrapper
                status=OutboxStatus.PENDING,
                tenant_id=None,
                execution_id=None,
                correlation_id=None,
                causation_id=None,
                occurred_at=utcnow(),
                created_at=utcnow(),
                processing_at=None,
            )
        ]

        result = await relay_outbox_to_queue(
            ctx, outbox_spec=outbox_spec, queue_spec=queue_spec
        )

        assert result.published == 1
        assert state.queues["jobs"]["jobs"][0].message.payload == _EventPayload(n=9)


@pytest.mark.asyncio
async def test_plaintext_route_unaffected() -> None:
    """encrypt=False keeps payloads plaintext at rest (no regression)."""

    outbox_spec, queue_spec = _specs(encrypt=False)
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        await ctx.outbox.command(outbox_spec).stage("job.requested", _EventPayload(n=3))
        await outbox_flush_tx_on_success_factory(outbox_spec)(ctx)(0, 0)

        state = ctx.deps.provide(MockStateDepKey)
        assert state.outbox_rows["events"][0].payload == {"n": 3}

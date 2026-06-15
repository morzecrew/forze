"""Direct queue publishing with end-to-end encryption: ciphertext at rest on the
broker, plaintext after the consumer decrypts — no outbox involved."""

from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import KeyringDepKey
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.pubsub import PubSubCommandDepKey, PubSubSpec
from forze.application.contracts.queue import (
    QueueCommandDepKey,
    QueueMessage,
    QueueSpec,
)
from forze.application.contracts.stream import StreamCommandDepKey, StreamSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.application.integrations.crypto import (
    MESSAGE_PAYLOAD_DOMAIN,
    decrypt_consumed_payload,
    is_encrypted_payload,
)
from forze.application.integrations.queue import encrypting_queue_command
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.consumer import QueueConsumer
from forze_mock import MockDepsModule, MockStateDepKey

# ----------------------- #


class _Job(BaseModel):
    n: int


def _spec(encryption: str = "end_to_end") -> QueueSpec[_Job]:
    return QueueSpec(name="jobs", codec=PydanticModelCodec(_Job), encryption=encryption)  # type: ignore[arg-type]


# ....................... #


@pytest.mark.asyncio
async def test_direct_publish_seals_payload_then_consumer_decrypts() -> None:
    spec = _spec("end_to_end")
    inbox_spec = InboxSpec(name="jobs")
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)

        command = ctx.deps.resolve_configurable(
            ctx, QueueCommandDepKey, spec, route=spec.name
        )
        await command.enqueue("jobs", _Job(n=42))

        # Ciphertext on the broker: the stored payload is the envelope wrapper.
        stored = state.queues["jobs"]["jobs"][0].message
        assert is_encrypted_payload(stored.payload)
        assert "n" not in stored.payload

        received: list[_Job] = []

        async def _handler(message: QueueMessage[_Job]) -> None:
            received.append(message.payload)

        run = await QueueConsumer(
            queue="jobs",
            queue_spec=spec,
            handler=_handler,
            inbox_spec=inbox_spec,
            tx_route="mock",
        ).run(ctx, timeout=timedelta(milliseconds=250))

        assert run.processed == 1
        assert received == [_Job(n=42)]


@pytest.mark.asyncio
async def test_plaintext_route_publishes_plaintext() -> None:
    spec = _spec("none")
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)

        command = ctx.deps.resolve_configurable(
            ctx, QueueCommandDepKey, spec, route=spec.name
        )
        await command.enqueue("jobs", _Job(n=3))

        assert state.queues["jobs"]["jobs"][0].message.payload == _Job(n=3)


@pytest.mark.asyncio
async def test_stream_append_seals_payload() -> None:
    spec = StreamSpec(name="events", codec=PydanticModelCodec(_Job), encryption="end_to_end")  # type: ignore[arg-type]
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)

        command = ctx.deps.resolve_configurable(
            ctx, StreamCommandDepKey, spec, route=spec.name
        )
        await command.append("events", _Job(n=7))

        [stored] = state.streams["events"]["events"]
        assert is_encrypted_payload(stored.payload)

        # The stored ciphertext decrypts via the consumer helper using the headers the
        # decorator set — proving append-side sealing round-trips for a stream consumer.
        plain = await decrypt_consumed_payload(
            ctx.deps.provide(KeyringDepKey),
            stored.payload,
            domain=MESSAGE_PAYLOAD_DOMAIN,
            codec=spec.codec,
            headers=stored.headers,
        )
        assert plain == _Job(n=7)


@pytest.mark.asyncio
async def test_pubsub_publish_seals_payload() -> None:
    spec = PubSubSpec(name="events", codec=PydanticModelCodec(_Job), encryption="end_to_end")  # type: ignore[arg-type]
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)

        command = ctx.deps.resolve_configurable(
            ctx, PubSubCommandDepKey, spec, route=spec.name
        )
        await command.publish("events", _Job(n=9))

        [stored] = state.pubsub_logs["events"]["events"]
        assert is_encrypted_payload(stored.payload)

        plain = await decrypt_consumed_payload(
            ctx.deps.provide(KeyringDepKey),
            stored.payload,
            domain=MESSAGE_PAYLOAD_DOMAIN,
            codec=spec.codec,
            headers=stored.headers,
        )
        assert plain == _Job(n=9)


def test_encryption_without_keyring_fails_closed() -> None:
    """A route that declares encryption but wires no keyring is refused (fail-closed)."""

    spec = _spec("end_to_end")
    inner = object()  # never touched — the guard fires before delegating

    with pytest.raises(CoreException) as ei:
        encrypting_queue_command(
            inner,  # type: ignore[arg-type]
            spec,
            cipher=None,
            tenant_provider=lambda: None,
        )

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.queue.encryption_wiring"

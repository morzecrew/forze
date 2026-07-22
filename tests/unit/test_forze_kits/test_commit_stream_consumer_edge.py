"""Edge branches of the offset-log consumer: encryption, poison, admin, forever-loop."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import KeyringDepKey
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.stream import (
    CommitStreamGroupAdminPort,
    OffsetReset,
    StreamCommandDepKey,
    StreamMessage,
    StreamSpec,
)
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.serialization import PydanticModelCodec
from forze.testing import context_from_modules
from forze_kits.integrations.consumer import CommitStreamGroupConsumer
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters import MockCommitStreamGroupAdminAdapter, MockStreamAdapter

# ----------------------- #


class _Payload(BaseModel):
    value: str


_CODEC = PydanticModelCodec(_Payload)
_TOPIC = "events"
_INBOX_SPEC = InboxSpec(name="inbox")
_IDLE = timedelta(milliseconds=100)
_ENC_SPEC = StreamSpec(
    name=_TOPIC,
    codec=_CODEC,
    encryption="end_to_end",  # type: ignore[arg-type]
)


def _harness() -> tuple[Any, MockCommitStreamGroupAdminAdapter[_Payload], MockState]:
    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state, strict_tx=True))
    admin = MockCommitStreamGroupAdminAdapter(
        stream=MockStreamAdapter(state=state, namespace=_TOPIC, codec=_CODEC),
        state=state,
    )
    return ctx, admin, state


def _consumer(
    handler: Callable[[StreamMessage[_Payload]], Awaitable[None]],
    **overrides: Any,
) -> CommitStreamGroupConsumer[_Payload]:
    kwargs: dict[str, Any] = dict(
        topics=[_TOPIC],
        group="g",
        consumer="c",
        stream_spec=_ENC_SPEC,
        handler=handler,
        inbox_spec=_INBOX_SPEC,
        tx_route="default",
    )
    kwargs.update(overrides)
    return CommitStreamGroupConsumer(**kwargs)


# ....................... #


@pytest.mark.asyncio
async def test_decrypts_end_to_end_message() -> None:
    ctx, admin, _state = _harness()
    assert ctx.deps.exists(KeyringDepKey)  # keyring wired → decrypt path is live
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)

    command = ctx.deps.resolve_configurable(
        ctx, StreamCommandDepKey, _ENC_SPEC, route=_ENC_SPEC.name
    )
    await command.append(_TOPIC, _Payload(value="secret"))

    seen: list[str] = []

    async def handler(msg: StreamMessage[_Payload]) -> None:
        seen.append(msg.payload.value)

    result = await _consumer(handler).run(ctx, timeout=_IDLE)

    assert (result.processed, seen) == (1, ["secret"])


def _tamper_stored_headers(state: MockState) -> None:
    """Break the AEAD by mutating an AAD-bound header on the stored ciphertext."""

    log = state.streams[_TOPIC][_TOPIC]
    stored = log[0]
    log[0] = attrs.evolve(stored, headers={**stored.headers, "forze_event_id": "tampered-id"})


@pytest.mark.asyncio
async def test_tampered_ciphertext_pauses_without_dlq() -> None:
    ctx, admin, state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    command = ctx.deps.resolve_configurable(
        ctx, StreamCommandDepKey, _ENC_SPEC, route=_ENC_SPEC.name
    )
    await command.append(_TOPIC, _Payload(value="secret"))
    _tamper_stored_headers(state)

    async def handler(_msg: StreamMessage[_Payload]) -> None:  # pragma: no cover
        raise AssertionError("handler must not run on a decrypt-poison message")

    result = await _consumer(handler).run(ctx, timeout=_IDLE)

    assert (result.failed, result.processed) == (1, 0)


@pytest.mark.asyncio
async def test_decrypt_poison_pauses_even_with_dlq() -> None:
    # An undecryptable payload has no typed model to re-produce, so it pauses even
    # when a DLQ stream is configured (unlike a decoded handler-poison message).
    ctx, admin, state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    command = ctx.deps.resolve_configurable(
        ctx, StreamCommandDepKey, _ENC_SPEC, route=_ENC_SPEC.name
    )
    await command.append(_TOPIC, _Payload(value="secret"))
    _tamper_stored_headers(state)

    async def handler(_msg: StreamMessage[_Payload]) -> None:  # pragma: no cover
        raise AssertionError("handler must not run on a decrypt-poison message")

    result = await _consumer(handler, dlq_stream="events.dlq").run(ctx, timeout=_IDLE)

    assert (result.failed, result.dead_lettered) == (1, 0)
    assert "events.dlq" not in state.streams.get(_TOPIC, {})


@pytest.mark.asyncio
async def test_transient_decrypt_failure_raises_for_restart_not_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A KMS blip on a cold data key surfaces from decrypt as a retryable-kind
    # error. That is crash-shaped, not poison: the run must raise (so the
    # supervised lifecycle restarts it with backoff), leaving the offset
    # uncommitted — the message is redelivered once the blip clears, never
    # skipped and never parked behind an operator pause.
    ctx, admin, _state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    command = ctx.deps.resolve_configurable(
        ctx, StreamCommandDepKey, _ENC_SPEC, route=_ENC_SPEC.name
    )
    await command.append(_TOPIC, _Payload(value="secret"))

    from forze_kits.integrations.consumer import commit_stream_runner as _runner

    async def _kms_blip(*_args: Any, **_kwargs: Any) -> Any:
        raise exc.infrastructure("KMS unavailable")

    monkeypatch.setattr(_runner, "decrypt_consumed_payload", _kms_blip)

    async def blocked_handler(_msg: StreamMessage[_Payload]) -> None:  # pragma: no cover
        raise AssertionError("handler must not run on an undecrypted message")

    with pytest.raises(CoreException) as excinfo:
        await _consumer(blocked_handler).run(ctx, timeout=_IDLE)

    assert excinfo.value.kind is ExceptionKind.INFRASTRUCTURE

    # Blip over: the offset was never committed past the message, so a restarted
    # consumer (the lifecycle rewinds to committed first) processes it.
    monkeypatch.undo()

    seen: list[str] = []

    async def handler(msg: StreamMessage[_Payload]) -> None:
        seen.append(msg.payload.value)

    restarted = _consumer(handler)
    await restarted.reset_to_committed(ctx)
    result = await restarted.run(ctx, timeout=_IDLE)

    assert (result.processed, seen) == (1, ["secret"])


@pytest.mark.asyncio
async def test_permanent_decrypt_failure_pauses_and_alerts_instead_of_crash_looping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The counterpart of the blip above: a revoked or deleted CMK (AccessDenied, disabled
    # key) is *permanent*. Classified as retryable it made the run raise, so the supervisor
    # restarted it every few seconds forever — and because totals.failed is only bumped on
    # the pause path, the documented ``failed > 0`` alert could never fire, leaving a
    # consumer that reads as "running" while nothing is ever consumed.
    ctx, admin, _state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    command = ctx.deps.resolve_configurable(
        ctx, StreamCommandDepKey, _ENC_SPEC, route=_ENC_SPEC.name
    )
    await command.append(_TOPIC, _Payload(value="secret"))

    from forze_kits.integrations.consumer import commit_stream_runner as _runner

    async def _revoked_key(*_args: Any, **_kwargs: Any) -> Any:
        # What the KMS adapters now raise for AccessDenied / disabled / deleted keys.
        raise exc.configuration("KMS access denied")

    monkeypatch.setattr(_runner, "decrypt_consumed_payload", _revoked_key)

    async def blocked_handler(_msg: StreamMessage[_Payload]) -> None:  # pragma: no cover
        raise AssertionError("handler must not run on an undecrypted message")

    # Returns instead of raising, so the supervisor stops rather than restarting...
    result = await _consumer(blocked_handler).run(ctx, timeout=_IDLE)

    # ...and the alert the docstring promises can actually fire.
    assert result.failed == 1
    assert result.processed == 0

    # The offset stays uncommitted: once an operator restores the key, a restarted
    # consumer still gets the message. Pausing must never skip it.
    monkeypatch.undo()

    seen: list[str] = []

    async def handler(msg: StreamMessage[_Payload]) -> None:
        seen.append(msg.payload.value)

    restarted = _consumer(handler)
    await restarted.reset_to_committed(ctx)

    assert (await restarted.run(ctx, timeout=_IDLE)).processed == 1
    assert seen == ["secret"]


@pytest.mark.asyncio
async def test_commit_admin_resolves_via_ctx_stream() -> None:
    ctx, _admin, _state = _harness()
    admin_port = ctx.stream.commit_admin(StreamSpec(name=_TOPIC, codec=_CODEC))

    assert isinstance(admin_port, CommitStreamGroupAdminPort)
    await admin_port.ensure_topic(_TOPIC, partitions=2)
    await admin_port.ensure_group("g2", [_TOPIC], start=OffsetReset.EARLIEST)
    lag = await admin_port.lag("g2", _TOPIC)
    assert [row.partition for row in lag] == [0, 1]


@pytest.mark.asyncio
async def test_forever_loop_polls_until_cancelled() -> None:
    ctx, admin, _state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)

    async def handler(_msg: StreamMessage[_Payload]) -> None:  # pragma: no cover
        return None

    # timeout=None runs forever on an empty log (poll-and-sleep); cancel it.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(_consumer(handler).run(ctx, timeout=None), timeout=0.2)

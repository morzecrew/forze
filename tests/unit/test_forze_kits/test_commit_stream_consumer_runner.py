"""Offset-log consumer runner: the commit-after-inbox decision ladder.

The mock offset-log is the primary backend: a per-group committed cursor makes
processed / duplicate / dead-lettered / paused outcomes observable, and the same
in-memory state feeds a producer handle for seeding the log.

``strict_tx=True`` everywhere: a failing handler must roll the inbox mark back
with its transaction, or retry / poison semantics could not be exercised.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any
from uuid import uuid4

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import is_encrypted_payload
from forze.application.contracts.envelope import HEADER_EVENT_ID, HEADER_TENANT_ID
from forze.application.contracts.inbox import InboxDepKey, InboxSpec
from forze.application.contracts.resilience import ResilienceExecutorDepKey
from forze.application.contracts.stream import (
    CommitStreamGroupQueryDepKey,
    OffsetReset,
    StreamCommandDepKey,
    StreamMessage,
    StreamPosition,
    StreamSpec,
    UndecodableStreamPayload,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.primitives import StrKey
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.consumer import (
    CommitStreamGroupConsumer,
    CommitStreamGroupConsumerRunResult,
)
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.adapters import (
    MockCommitStreamGroupAdapter,
    MockCommitStreamGroupAdminAdapter,
    MockState,
    MockStreamAdapter,
)
from forze_mock.execution.module import ConfigurableMockInbox, mock_strict_txmanager
from tests.support.execution_context import context_from_deps, context_from_modules

# ----------------------- #


class _Payload(BaseModel):
    value: str


_CODEC = PydanticModelCodec(_Payload)
_TOPIC = "orders"
_STREAM_SPEC = StreamSpec(name=_TOPIC, codec=_CODEC)
_INBOX_SPEC = InboxSpec(name="events")
_IDLE = timedelta(milliseconds=100)


def _harness() -> tuple[
    ExecutionContext,
    MockStreamAdapter[_Payload],
    MockCommitStreamGroupAdminAdapter[_Payload],
    MockCommitStreamGroupAdapter[_Payload],
    MockState,
]:
    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state, strict_tx=True))
    producer = MockStreamAdapter(state=state, namespace=_TOPIC, codec=_CODEC)
    admin = MockCommitStreamGroupAdminAdapter(stream=producer, state=state)
    query = MockCommitStreamGroupAdapter(stream=producer, state=state, namespace=_TOPIC)
    return ctx, producer, admin, query, state


async def _seed(producer: MockStreamAdapter[_Payload], n: int) -> None:
    for i in range(n):
        await producer.append(_TOPIC, _Payload(value=str(i)), key="k")


def _consumer(
    handler: Callable[[StreamMessage[_Payload]], Awaitable[None]],
    **overrides: Any,
) -> CommitStreamGroupConsumer[_Payload]:
    kwargs: dict[str, Any] = dict(
        topics=[_TOPIC],
        group="g",
        consumer="c",
        stream_spec=_STREAM_SPEC,
        handler=handler,
        inbox_spec=_INBOX_SPEC,
        tx_route="default",
    )
    kwargs.update(overrides)
    return CommitStreamGroupConsumer(**kwargs)


# ....................... #


@pytest.mark.asyncio
async def test_processes_and_commits_all() -> None:
    ctx, producer, admin, query, _state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    await _seed(producer, 3)

    seen: list[str] = []

    async def handler(msg: StreamMessage[_Payload]) -> None:
        seen.append(msg.payload.value)

    result = await _consumer(handler).run(ctx, timeout=_IDLE)

    assert isinstance(result, CommitStreamGroupConsumerRunResult)
    assert result.processed == 3
    assert seen == ["0", "1", "2"]
    # Committed past everything → a fresh read is empty.
    assert await query.read("g", "c", [_TOPIC]) == []


@pytest.mark.asyncio
async def test_redelivery_is_deduped_by_inbox() -> None:
    ctx, producer, admin, query, _state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    await _seed(producer, 2)

    calls = 0

    async def handler(_msg: StreamMessage[_Payload]) -> None:
        nonlocal calls
        calls += 1

    first = await _consumer(handler).run(ctx, timeout=_IDLE)
    assert first.processed == 2

    # Replay the same offsets; the inbox recognizes them as duplicates.
    await admin.reset_offsets("g", _TOPIC, to=OffsetReset.EARLIEST)
    second = await _consumer(handler).run(ctx, timeout=_IDLE)

    assert second.duplicates == 2
    assert second.processed == 0
    assert calls == 2  # handler ran only for the genuinely-new messages


@pytest.mark.asyncio
async def test_poison_dead_letters_and_advances() -> None:
    ctx, producer, admin, query, state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    await _seed(producer, 1)

    async def handler(_msg: StreamMessage[_Payload]) -> None:
        raise ValueError("boom")

    result = await _consumer(handler, dlq_stream="orders.dlq").run(ctx, timeout=_IDLE)

    assert result.dead_lettered == 1
    assert result.failed == 0
    # Advanced past the poison message.
    assert await query.read("g", "c", [_TOPIC]) == []
    # Produced to the dead-letter stream.
    assert len(state.streams[_TOPIC]["orders.dlq"]) == 1


@pytest.mark.asyncio
async def test_dead_letter_forwards_sealed_envelope_round_trip() -> None:
    """End-to-end-encrypted route: the dead-letter copy is the envelope as received —
    same ciphertext, same event-id/tenant headers — so a consumer on the DLQ topic
    with the same keyring decrypts it and correlates it back to the source event."""

    e2e_spec = StreamSpec(name=_TOPIC, codec=_CODEC, encryption="end_to_end")  # type: ignore[arg-type]
    ctx, _producer, admin, _query, state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)

    # Seed one sealed message the way the governed path produces it: through the
    # encrypting command port, under a bound tenant.
    command = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, e2e_spec, route=e2e_spec.name)
    tenant_id = uuid4()
    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant_id)):
        await command.append(_TOPIC, _Payload(value="0"), key="k")

    [source] = state.streams[_TOPIC][_TOPIC]
    assert is_encrypted_payload(source.payload)
    source_event_id = source.headers[HEADER_EVENT_ID]
    assert source.headers[HEADER_TENANT_ID] == str(tenant_id)

    async def poison(_msg: StreamMessage[_Payload]) -> None:
        raise ValueError("boom")

    # The consumer runs tenant-unbound (a background drain), like production.
    result = await _consumer(poison, stream_spec=e2e_spec, dlq_stream="orders.dlq").run(
        ctx, timeout=_IDLE
    )
    assert result.dead_lettered == 1

    # The DLQ copy is the source envelope untouched: ciphertext and headers intact.
    [copy] = state.streams[_TOPIC]["orders.dlq"]
    assert copy.payload == source.payload
    assert copy.headers[HEADER_EVENT_ID] == source_event_id
    assert copy.headers[HEADER_TENANT_ID] == str(tenant_id)

    # Round-trip: a consumer on the DLQ topic decrypts the copy with the same
    # keyring and sees the original event id (the dedup/correlation identity).
    await admin.ensure_group("dlq-g", ["orders.dlq"], start=OffsetReset.EARLIEST)

    seen: list[tuple[str, str | None]] = []

    async def dlq_handler(msg: StreamMessage[_Payload]) -> None:
        seen.append((msg.payload.value, msg.headers.get(HEADER_EVENT_ID)))

    dlq_result = await CommitStreamGroupConsumer(
        topics=["orders.dlq"],
        group="dlq-g",
        consumer="c",
        stream_spec=e2e_spec,
        handler=dlq_handler,
        inbox_spec=InboxSpec(name="dlq-events"),
        tx_route="default",
    ).run(ctx, timeout=_IDLE)

    assert dlq_result.failed == 0  # not classified as decrypt-poison
    assert dlq_result.processed == 1
    assert seen == [("0", source_event_id)]


@pytest.mark.asyncio
async def test_poison_without_dlq_pauses_and_alerts() -> None:
    ctx, producer, admin, query, _state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    await _seed(producer, 2)

    async def handler(msg: StreamMessage[_Payload]) -> None:
        if msg.payload.value == "0":
            raise ValueError("boom")

    result = await _consumer(handler).run(ctx, timeout=_IDLE)

    assert result.failed == 1
    assert result.processed == 0
    # Offset left uncommitted → the poison message is redelivered on the next read.
    remaining = await query.read("g", "c", [_TOPIC])
    assert [m.offset for m in remaining] == [0, 1]


@pytest.mark.asyncio
async def test_max_attempts_retries_then_succeeds() -> None:
    ctx, producer, admin, _query, _state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    await _seed(producer, 1)

    attempts = 0

    async def handler(_msg: StreamMessage[_Payload]) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError("transient")

    result = await _consumer(handler, max_attempts=3).run(ctx, timeout=_IDLE)

    assert result.processed == 1
    assert attempts == 3


@pytest.mark.asyncio
async def test_requires_transactions_fails_closed() -> None:
    ctx, producer, admin, _query, _state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    await _seed(producer, 1)

    spec = StreamSpec(name=_TOPIC, codec=_CODEC, requires_transactions=True)

    async def handler(_msg: StreamMessage[_Payload]) -> None:
        return None

    with pytest.raises(CoreException) as ei:
        await _consumer(handler, stream_spec=spec).run(ctx, timeout=_IDLE)

    assert ei.value.code == "stream.transactions_unsupported"


@attrs.define(slots=True)
class _RetryOnceExecutor:
    """Resilience executor double: records the policy and retries the call once."""

    policies_used: list[str] = attrs.field(factory=list)

    async def run[T](
        self,
        fn: Callable[[], Awaitable[T]],
        *,
        policy: StrKey,
        route: StrKey | None = None,
        fallback: Callable[[BaseException], Awaitable[T]] | None = None,
    ) -> T:
        del route, fallback
        self.policies_used.append(str(policy))
        try:
            return await fn()
        except Exception:
            return await fn()


@pytest.mark.asyncio
async def test_retry_policy_wraps_each_process_attempt() -> None:
    state = MockState()
    producer = MockStreamAdapter(state=state, namespace=_TOPIC, codec=_CODEC)
    admin = MockCommitStreamGroupAdminAdapter(stream=producer, state=state)
    query = MockCommitStreamGroupAdapter(stream=producer, state=state, namespace=_TOPIC)

    executor = _RetryOnceExecutor()
    ctx = context_from_deps(
        Deps.plain(
            {
                MockStateDepKey: state,
                InboxDepKey: ConfigurableMockInbox(module=MockDepsModule(state=state)),
                TransactionManagerDepKey: mock_strict_txmanager,
                CommitStreamGroupQueryDepKey: (lambda _ctx, _spec: query),
                ResilienceExecutorDepKey: executor,
            }
        )
    )

    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    await _seed(producer, 1)

    attempts = 0

    async def handler(_msg: StreamMessage[_Payload]) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ValueError("transient")

    result = await _consumer(handler, retry_policy="flaky").run(ctx, timeout=_IDLE)

    assert result.processed == 1
    assert attempts == 2  # the executor retried the failing attempt once
    assert executor.policies_used == ["flaky"]


@attrs.define(slots=True)
class _PoisonThenEmptyPort:
    """Fake commit-stream port: serves one undecodable-marker batch, then drains.

    Records commits and seek-to-committed calls so the runner's decode-poison
    pause path (surface marker → pause → rewind to committed) is observable
    without a real Kafka adapter.
    """

    seek_calls: list[tuple[str, list[str]]] = attrs.field(factory=list)
    commits: list[list[StreamPosition]] = attrs.field(factory=list)
    served: bool = False

    async def read(
        self,
        group: str,
        consumer: str,
        topics: Any,
        *,
        limit: Any = None,
        timeout: Any = None,
    ) -> list[StreamMessage[_Payload]]:
        del group, consumer, topics, limit, timeout
        if self.served:
            return []
        self.served = True
        return [
            StreamMessage(
                stream=_TOPIC,
                id=f"{_TOPIC}:0:0",
                payload=UndecodableStreamPayload(raw=b"x", error="boom"),  # type: ignore[arg-type]
                partition=0,
                offset=0,
            )
        ]

    async def tail(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
        raise NotImplementedError

    async def commit(self, group: str, positions: Any) -> None:
        del group
        self.commits.append(list(positions))

    async def seek_to_committed(self, group: str, topics: Any) -> None:
        self.seek_calls.append((group, list(topics)))


@pytest.mark.asyncio
async def test_undecodable_marker_pauses_and_seeks_to_committed() -> None:
    # BUG 1: an UndecodableStreamPayload surfaced by the read path pauses the run
    # (offset left uncommitted) and rewinds to committed, never committing past it.
    port = _PoisonThenEmptyPort()
    ctx = context_from_deps(Deps.plain({CommitStreamGroupQueryDepKey: (lambda _ctx, _spec: port)}))

    async def handler(_msg: StreamMessage[_Payload]) -> None:  # pragma: no cover
        raise AssertionError("handler must not run on a decode-poison marker")

    result = await _consumer(handler).run(ctx, timeout=_IDLE)

    assert (result.failed, result.processed) == (1, 0)
    assert port.commits == []  # nothing good before the poison → no offset committed
    assert port.seek_calls == [("g", [_TOPIC])]


@pytest.mark.asyncio
async def test_rejects_bad_config() -> None:
    async def handler(_msg: StreamMessage[_Payload]) -> None:
        return None

    with pytest.raises(CoreException):
        _consumer(handler, max_attempts=0)

    with pytest.raises(CoreException):
        _consumer(handler, topics=[])


@pytest.mark.asyncio
async def test_draining_refusal_stops_without_dead_lettering() -> None:
    # A rolling deploy flips the drain gate before the loop's stop event: the
    # handler's dispatch is refused with THROTTLED/code="draining". That is a
    # shutdown artifact, not a handler defect — burning max_attempts on the one-way
    # gate and dead-lettering would park a HEALTHY message as poison and commit the
    # offset past an effect that never ran. The runner must stop instead, leaving
    # the offset uncommitted for redelivery (the queue twin requeues uncounted for
    # exactly this case).
    from forze.base.exceptions import exc

    ctx, producer, admin, query, state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    await _seed(producer, 3)

    calls: list[str] = []

    async def handler(msg: StreamMessage[_Payload]) -> None:
        calls.append(msg.payload.value)

        if msg.payload.value == "1":
            raise exc.throttled("Runtime is draining", code="draining")

    result = await _consumer(handler, dlq_stream="orders.dlq", max_attempts=3).run(
        ctx, timeout=_IDLE
    )

    # message 0 processed and committed; the refusal stopped the run
    assert result.processed == 1
    assert result.dead_lettered == 0
    assert "orders.dlq" not in state.streams.get(_TOPIC, {})

    # not a delivery attempt: the refusal did not burn retries
    assert calls == ["0", "1"]

    # the refused message and the tail stay uncommitted — a fresh run redelivers
    redelivered = await query.read("g", "c", [_TOPIC])
    assert [m.payload.value for m in redelivered] == ["1", "2"]


@pytest.mark.asyncio
async def test_non_draining_core_exception_retries_then_dead_letters() -> None:
    # A CoreException with any other code is an ordinary handler failure: it burns
    # attempts and follows the poison ladder — only the draining code short-circuits.
    from forze.base.exceptions import exc

    ctx, producer, admin, query, state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    await _seed(producer, 1)

    attempts = 0

    async def handler(_msg: StreamMessage[_Payload]) -> None:
        nonlocal attempts
        attempts += 1
        raise exc.conflict("busy", code="not_draining")

    result = await _consumer(handler, dlq_stream="orders.dlq", max_attempts=2).run(
        ctx, timeout=_IDLE
    )

    assert attempts == 2  # retried, unlike a draining refusal
    assert result.dead_lettered == 1
    assert len(state.streams[_TOPIC]["orders.dlq"]) == 1


@pytest.mark.asyncio
async def test_core_exception_escaping_process_one_propagates_unless_draining() -> None:
    # The batch loop's draining handler must NOT absorb other CoreExceptions that
    # escape _process_one — anything else is crash-shaped and belongs to the
    # supervised restart, not to the stop-and-rewind path.
    from unittest.mock import AsyncMock, patch

    from forze.base.exceptions import exc
    from forze_kits.integrations.consumer.commit_stream_runner import (
        CommitStreamGroupConsumer as _Consumer,
    )

    ctx, producer, admin, query, _state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    await _seed(producer, 1)

    async def handler(_msg: StreamMessage[_Payload]) -> None:  # pragma: no cover — patched out
        return None

    boom = exc.infrastructure("store down", code="not_draining")

    with (
        patch.object(_Consumer, "_process_one", AsyncMock(side_effect=boom)),
        pytest.raises(CoreException, match="store down"),
    ):
        await _consumer(handler).run(ctx, timeout=_IDLE)

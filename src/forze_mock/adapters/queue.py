"""In-memory queue adapter."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Mapping, Sequence
from datetime import datetime, timedelta
from typing import (
    Any,
    cast,
    final,
)

import attrs

from forze.application.contracts.queue import (
    QueueCommandPort,
    QueueMessage,
    QueueQueryPort,
    resolve_delivery_delay,
)
from forze.base.exceptions import exc
from forze.base.primitives import monotonic, utcnow
from forze.base.serialization import (
    ModelCodec,
)
from forze_mock.query._types import M
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin

# ----------------------- #


def _sleep_interval(  # pyright: ignore[reportUnusedFunction]
    timeout: timedelta | None,
) -> float:
    if timeout is None:
        return 0.05
    return max(timeout.total_seconds(), 0.001)


# ....................... #

_CONSUME_POLL_INTERVAL: float = 0.01
"""Polling interval (seconds) used by :meth:`MockQueueAdapter.consume`."""

_DEAD_LETTER_PREFIX: str = "__dead_letter__:"
"""Reserved key prefix (within the per-namespace queue store) for dead-letter lists."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _MockQueueEntry[M]:
    """In-memory queue slot with visibility time."""

    visible_at: datetime
    message: QueueMessage[M]
    delivery_count: int = 0
    """How many times the message has already been delivered to a consumer."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _MockPendingEntry[M]:
    """In-flight (received, unacked) message with its redelivery deadline."""

    message: QueueMessage[M]
    redeliver_at: datetime
    """Instant after which the unacked message becomes receivable again."""

    delivery_count: int
    """How many times the message has been delivered (including this one)."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockQueueAdapter(MockTenancyMixin, QueueQueryPort[M], QueueCommandPort[M]):
    """In-memory queue adapter with ack/nack support.

    Mirrors the broker redelivery model of the real adapters:

    - **Visibility timeout** — :meth:`receive` parks messages as in-flight for
      :attr:`visibility_timeout`; messages not acked within that window become
      receivable again with the *same* stable id (SQS-style redelivery). Lapsed
      deadlines are checked lazily on the next :meth:`receive`/:meth:`consume`
      — no background task runs.
    - **Idle-timeout consume** — :meth:`consume` follows the port contract:
      ``timeout=None`` consumes forever; a finite value is an *idle* timeout
      after which the generator terminates cleanly, with every yielded message
      resetting the idle window.
    - **Dead-lettering** — ``nack(requeue=False)`` moves the message to an
      inspectable per-queue dead-letter list (see :meth:`dead_letters`), the
      mock's stand-in for the broker-specific terminal disposition (RabbitMQ
      DLX / SQS redrive policy).
    """

    state: MockState
    namespace: str
    codec: ModelCodec[M, Any]

    visibility_timeout: timedelta = timedelta(seconds=30)
    """How long a received-but-unacked message stays invisible before redelivery."""

    # ....................... #

    def _ns(self) -> str:
        return self._partitioned_namespace(self.namespace)

    def _queue_store(self) -> dict[str, list[_MockQueueEntry[M]]]:
        return cast(
            dict[str, list[_MockQueueEntry[M]]],
            self.state.queues.setdefault(self._ns(), {}),
        )

    # ....................... #

    def _pending_store(self) -> dict[str, dict[str, _MockPendingEntry[M]]]:
        return cast(
            dict[str, dict[str, _MockPendingEntry[M]]],
            self.state.queue_pending.setdefault(self._ns(), {}),
        )

    # ....................... #

    def _dead_letter_list(self, queue: str) -> list[QueueMessage[M]]:
        ns_store = self.state.queues.setdefault(self._ns(), {})
        return cast(
            list[QueueMessage[M]],
            ns_store.setdefault(_DEAD_LETTER_PREFIX + queue, []),
        )

    # ....................... #

    def dead_letters(self, queue: str) -> list[QueueMessage[M]]:
        """Return the dead-letter list for *queue* (``nack(requeue=False)`` sink)."""

        with self.state.lock:
            return list(self._dead_letter_list(queue))

    # ....................... #

    def _release_expired(self, queue: str, now: datetime) -> None:
        """Lazily move unacked messages past their visibility deadline back to the queue."""

        pending = self._pending_store().setdefault(queue, {})
        queued = self._queue_store().setdefault(queue, [])
        expired_ids = [pid for pid, entry in pending.items() if entry.redeliver_at <= now]
        for pid in expired_ids:
            entry = pending.pop(pid)
            queued.append(
                _MockQueueEntry(
                    visible_at=now,
                    message=entry.message,
                    delivery_count=entry.delivery_count,
                )
            )

    # ....................... #

    async def enqueue(
        self,
        queue: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        delay: timedelta | None = None,
        not_before: datetime | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        now = utcnow()
        resolved_delay = resolve_delivery_delay(delay=delay, not_before=not_before)
        visible_at = now if resolved_delay is None else now + resolved_delay
        message_id = self.state.next_id("queue")
        message = QueueMessage(
            queue=queue,
            id=message_id,
            payload=payload,
            type=type,
            key=key,
            enqueued_at=enqueued_at or now,
            headers=dict(headers) if headers else {},
        )
        entry = _MockQueueEntry(visible_at=visible_at, message=message)
        with self.state.lock:
            self._queue_store().setdefault(queue, []).append(entry)
        return message_id

    # ....................... #

    async def enqueue_many(
        self,
        queue: str,
        payloads: Sequence[M],
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        delay: timedelta | None = None,
        not_before: datetime | None = None,
        headers: Mapping[str, str] | None = None,
        message_headers: Sequence[Mapping[str, str]] | None = None,
    ) -> list[str]:
        if message_headers is not None and len(message_headers) != len(payloads):
            raise exc.precondition("message_headers length must match payloads length.")

        out: list[str] = []
        for i, payload in enumerate(payloads):
            out.append(
                await self.enqueue(
                    queue,
                    payload,
                    type=type,
                    key=key,
                    enqueued_at=enqueued_at,
                    delay=delay,
                    not_before=not_before,
                    headers=(
                        {**(headers or {}), **message_headers[i]}
                        if message_headers is not None
                        else headers
                    ),
                )
            )
        return out

    # ....................... #

    async def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[QueueMessage[M]]:
        del timeout
        now = utcnow()
        with self.state.lock:
            self._release_expired(queue, now)
            entries = self._queue_store().setdefault(queue, [])
            pending = self._pending_store().setdefault(queue, {})
            ready: list[_MockQueueEntry[M]] = []
            deferred: list[_MockQueueEntry[M]] = []
            for entry in entries:
                if entry.visible_at <= now:
                    ready.append(entry)
                else:
                    deferred.append(entry)
            count = limit if limit is not None else len(ready)
            batch_entries = ready[:count]
            remaining_ready = ready[count:]
            self._queue_store()[queue] = remaining_ready + deferred
            redeliver_at = now + self.visibility_timeout
            delivered: list[QueueMessage[M]] = []
            for entry in batch_entries:
                pending[entry.message.id] = _MockPendingEntry(
                    message=entry.message,
                    redeliver_at=redeliver_at,
                    delivery_count=entry.delivery_count + 1,
                )
                # Surface the exact per-delivery count (including this one)
                # on the returned message, mirroring SQS's
                # ApproximateReceiveCount.
                delivered.append(
                    attrs.evolve(
                        entry.message,
                        delivery_count=entry.delivery_count + 1,
                    )
                )
            return delivered

    # ....................... #

    async def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[QueueMessage[M]]:
        # The idle window is tracked on the monotonic clock: consume genuinely
        # waits in real time (small polling sleeps), so a frozen ``TimeSource``
        # must not be able to wedge a finite idle timeout into an endless loop.
        idle_seconds = None if timeout is None else timeout.total_seconds()
        idle_deadline = None if idle_seconds is None else monotonic() + idle_seconds
        while True:
            batch = await self.receive(queue, limit=1, timeout=timeout)
            if batch:
                yield batch[0]
                if idle_seconds is not None:
                    idle_deadline = monotonic() + idle_seconds
                continue
            if idle_deadline is not None and monotonic() >= idle_deadline:
                return
            await asyncio.sleep(_CONSUME_POLL_INTERVAL)

    # ....................... #

    async def ack(self, queue: str, ids: Sequence[str]) -> int:
        with self.state.lock:
            pending = self._pending_store().setdefault(queue, {})
            acked = 0
            for item_id in ids:
                if item_id in pending:
                    pending.pop(item_id, None)
                    acked += 1
            return acked

    # ....................... #

    async def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
        count: bool = True,
    ) -> int:
        # ``count`` is accepted for port parity; the mock requeue already preserves
        # ``delivery_count`` verbatim (it never advances it on requeue), so nothing to gate.
        _ = count

        with self.state.lock:
            pending = self._pending_store().setdefault(queue, {})
            queued = self._queue_store().setdefault(queue, [])
            nacked = 0
            now = utcnow()
            for item_id in ids:
                entry = pending.pop(item_id, None)
                if entry is None:
                    continue
                nacked += 1
                if requeue:
                    queued.append(
                        _MockQueueEntry(
                            visible_at=now,
                            message=entry.message,
                            delivery_count=entry.delivery_count,
                        )
                    )
                else:
                    self._dead_letter_list(queue).append(entry.message)
            return nacked

"""In-memory queue adapter."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import (
    Any,
    AsyncGenerator,
    Sequence,
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
from forze.base.primitives import utcnow
from forze.base.serialization import (
    RecordMappingCodec,
)
from forze_mock.query._types import M
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace


def _sleep_interval(timeout: timedelta | None) -> float:
    if timeout is None:
        return 0.05
    return max(timeout.total_seconds(), 0.001)


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _MockQueueEntry[M]:
    """In-memory queue slot with visibility time."""

    visible_at: datetime
    message: QueueMessage[M]


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockQueueAdapter(MockTenancyMixin, QueueQueryPort[M], QueueCommandPort[M]):
    """In-memory queue adapter with ack/nack support."""

    state: MockState
    namespace: str
    codec: RecordMappingCodec[M, Any]

    # ....................... #

    def _ns(self) -> str:
        return partition_namespace(self.require_tenant_if_aware(), self.namespace)

    def _queue_store(self) -> dict[str, list[_MockQueueEntry[M]]]:
        return cast(
            dict[str, list[_MockQueueEntry[M]]],
            self.state.queues.setdefault(self._ns(), {}),
        )

    # ....................... #

    def _pending_store(self) -> dict[str, dict[str, QueueMessage[M]]]:
        return cast(
            dict[str, dict[str, QueueMessage[M]]],
            self.state.queue_pending.setdefault(self._ns(), {}),
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
    ) -> list[str]:
        out: list[str] = []
        for payload in payloads:
            out.append(
                await self.enqueue(
                    queue,
                    payload,
                    type=type,
                    key=key,
                    enqueued_at=enqueued_at,
                    delay=delay,
                    not_before=not_before,
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
            batch = [entry.message for entry in batch_entries]
            for msg in batch:
                pending[msg.id] = msg
            return list(batch)

    # ....................... #

    async def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[QueueMessage[M]]:
        while True:
            batch = await self.receive(queue, limit=1, timeout=timeout)
            if batch:
                yield batch[0]
                continue
            await asyncio.sleep(_sleep_interval(timeout))

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
    ) -> int:
        with self.state.lock:
            pending = self._pending_store().setdefault(queue, {})
            queued = self._queue_store().setdefault(queue, [])
            nacked = 0
            now = utcnow()
            for item_id in ids:
                msg = pending.pop(item_id, None)
                if msg is None:
                    continue
                nacked += 1
                if requeue:
                    queued.append(_MockQueueEntry(visible_at=now, message=msg))
            return nacked

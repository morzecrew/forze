"""In-memory pub/sub adapter."""

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

from forze.application.contracts.pubsub import (
    PubSubCommandPort,
    PubSubMessage,
    PubSubQueryPort,
)
from forze.base.primitives import utcnow
from forze.base.serialization import (
    RecordMappingCodec,
)
from forze_mock.adapters.queue import (
    _sleep_interval,  # type: ignore[reportPrivateUsage]
)
from forze_mock.query._types import M
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockPubSubAdapter(MockTenancyMixin, PubSubCommandPort[M], PubSubQueryPort[M]):
    """In-memory pub/sub adapter backed by append-only topic logs."""

    state: MockState
    namespace: str
    codec: RecordMappingCodec[M, Any]

    # ....................... #

    def _ns(self) -> str:
        return partition_namespace(self.require_tenant_if_aware(), self.namespace)

    def _topic_store(self) -> dict[str, list[PubSubMessage[M]]]:
        return cast(
            dict[str, list[PubSubMessage[M]]],
            self.state.pubsub_logs.setdefault(self._ns(), {}),
        )

    # ....................... #

    async def publish(
        self,
        topic: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        published_at: datetime | None = None,
    ) -> None:
        message = PubSubMessage(
            topic=topic,
            payload=payload,
            type=type,
            key=key,
            published_at=published_at or utcnow(),
        )
        with self.state.lock:
            self._topic_store().setdefault(topic, []).append(message)

    # ....................... #

    async def subscribe(
        self,
        topics: Sequence[str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[PubSubMessage[M]]:
        with self.state.lock:
            cursors = {
                topic: len(self._topic_store().get(topic, [])) for topic in topics
            }

        while True:
            emitted = False
            pending: list[PubSubMessage[M]] = []
            with self.state.lock:
                for topic in topics:
                    log = self._topic_store().setdefault(topic, [])
                    cur = cursors.get(topic, 0)
                    if cur >= len(log):
                        continue
                    pending.extend(log[cur:])
                    emitted = True
                    cursors[topic] = len(log)

            for msg in pending:
                yield msg

            if emitted:
                continue
            await asyncio.sleep(_sleep_interval(timeout))

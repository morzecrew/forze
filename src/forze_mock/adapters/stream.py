"""In-memory stream adapters."""

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
from pydantic import BaseModel

from forze.application.contracts.stream import (
    StreamCommandPort,
    StreamGroupQueryPort,
    StreamMessage,
    StreamQueryPort,
)
from forze.base.primitives import utcnow
from forze.base.serialization import (
    ModelCodec,
)
from forze_mock.adapters.queue import (
    _sleep_interval,  # type: ignore[reportPrivateUsage]
)
from forze_mock.query._types import M
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockStreamAdapter(MockTenancyMixin, StreamQueryPort[M], StreamCommandPort[M]):
    """In-memory stream adapter with monotonic message identifiers."""

    state: MockState
    namespace: str
    codec: ModelCodec[M, Any]

    # ....................... #

    def _ns(self) -> str:
        return partition_namespace(self.require_tenant_if_aware(), self.namespace)

    def _stream_store(self) -> dict[str, list[StreamMessage[M]]]:
        return cast(
            dict[str, list[StreamMessage[M]]],
            self.state.streams.setdefault(self._ns(), {}),
        )

    # ....................... #

    def _id_to_int(self, value: str) -> int:
        suffix = value.rsplit("-", 1)[-1]
        try:
            return int(suffix)
        except ValueError:
            return 0

    # ....................... #

    async def append(
        self,
        stream: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        timestamp: datetime | None = None,
    ) -> str:
        message_id = self.state.next_id("stream")
        message = StreamMessage(
            stream=stream,
            id=message_id,
            payload=payload,
            type=type,
            key=key,
            timestamp=timestamp or utcnow(),
        )
        with self.state.lock:
            self._stream_store().setdefault(stream, []).append(message)
        return message_id

    # ....................... #

    async def read(
        self,
        stream_mapping: dict[str, str],
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[StreamMessage[M]]:
        del timeout
        out: list[StreamMessage[M]] = []
        with self.state.lock:
            for stream, last_id in stream_mapping.items():
                log = self._stream_store().setdefault(stream, [])
                last_num = self._id_to_int(last_id)
                for msg in log:
                    if self._id_to_int(msg.id) > last_num:
                        out.append(msg)
                        if limit is not None and len(out) >= limit:
                            return out
        return out

    # ....................... #

    async def tail(
        self,
        stream_mapping: dict[str, str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[StreamMessage[M]]:
        cursor = dict(stream_mapping)
        while True:
            messages = await self.read(cursor, timeout=timeout)
            for message in messages:
                cursor[message.stream] = message.id
                yield message
            if not messages:
                await asyncio.sleep(_sleep_interval(timeout))


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockStreamGroupAdapter[M: BaseModel](StreamGroupQueryPort[M]):
    """In-memory stream group adapter."""

    stream: MockStreamAdapter[M]
    state: MockState
    namespace: str

    # ....................... #

    async def read(
        self,
        group: str,
        consumer: str,
        stream_mapping: dict[str, str],
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[StreamMessage[M]]:
        del consumer
        return await self.stream.read(
            stream_mapping,
            limit=limit,
            timeout=timeout,
        )

    # ....................... #

    async def tail(
        self,
        group: str,
        consumer: str,
        stream_mapping: dict[str, str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[StreamMessage[M]]:
        del group, consumer
        async for item in self.stream.tail(stream_mapping, timeout=timeout):
            yield item

    # ....................... #

    async def ack(self, group: str, stream: str, ids: Sequence[str]) -> int:
        key = (self.stream._ns(), group, stream)  # type: ignore[reportPrivateUsage]
        with self.state.lock:
            ack_set = self.state.stream_ack.setdefault(key, set())
            before = len(ack_set)
            ack_set.update(ids)
            return len(ack_set) - before

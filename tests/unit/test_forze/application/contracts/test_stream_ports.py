"""Tests for forze.application.contracts.stream.ports."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from typing import Sequence

from pydantic import BaseModel

from forze.application.contracts.stream import StreamCommandPort, StreamGroupQueryPort, StreamQueryPort
from forze.application.contracts.stream.types import StreamMessage


class _Msg(BaseModel):
    v: int


class _StubStreamQuery:
    async def read(
        self,
        stream_mapping: dict[str, str],
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[StreamMessage[_Msg]]:
        return [
            {
                "stream": "s",
                "id": "1",
                "payload": _Msg(v=1),
            }
        ]

    async def tail(
        self, stream_mapping: dict[str, str], *, timeout: timedelta | None = None
    ) -> AsyncIterator[StreamMessage[_Msg]]:
        _ = stream_mapping, timeout
        yield {
            "stream": "s",
            "id": "t1",
            "payload": _Msg(v=2),
        }


class _StubStreamGroupQuery:
    async def read(
        self,
        group: str,
        consumer: str,
        stream_mapping: dict[str, str],
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[StreamMessage[_Msg]]:
        return []

    async def tail(
        self,
        group: str,
        consumer: str,
        stream_mapping: dict[str, str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[StreamMessage[_Msg]]:
        _ = group, consumer, stream_mapping, timeout
        yield {
            "stream": "s",
            "id": "gt1",
            "payload": _Msg(v=3),
        }

    async def ack(self, group: str, stream: str, ids: Sequence[str]) -> int:
        return len(ids)


class _StubStreamCommand:
    async def append(
        self,
        stream: str,
        payload: _Msg,
        *,
        type: str | None = None,
        key: str | None = None,
        timestamp: datetime | None = None,
    ) -> str:
        return "mid"


class TestStreamQueryPort:
    def test_runtime_checkable(self) -> None:
        assert isinstance(_StubStreamQuery(), StreamQueryPort)

    async def test_read_and_tail(self) -> None:
        stub = _StubStreamQuery()
        batch = await stub.read({"x": "0"}, limit=2, timeout=timedelta(seconds=1))
        assert len(batch) == 1
        rows: list[StreamMessage[_Msg]] = []
        async for msg in stub.tail({"x": "0"}):
            rows.append(msg)
        assert len(rows) == 1
        assert rows[0]["payload"].v == 2


class TestStreamGroupQueryPort:
    def test_runtime_checkable(self) -> None:
        assert isinstance(_StubStreamGroupQuery(), StreamGroupQueryPort)

    async def test_read_and_ack(self) -> None:
        stub = _StubStreamGroupQuery()
        assert await stub.read("g", "c", {"a": "0"}) == []
        assert await stub.ack("g", "s", ("1", "2")) == 2

    async def test_tail_yields_messages(self) -> None:
        stub = _StubStreamGroupQuery()
        rows: list[StreamMessage[_Msg]] = []
        async for msg in stub.tail("g", "c", {"a": "0"}):
            rows.append(msg)
        assert len(rows) == 1
        assert rows[0]["payload"].v == 3


class TestStreamCommandPort:
    def test_runtime_checkable(self) -> None:
        assert isinstance(_StubStreamCommand(), StreamCommandPort)

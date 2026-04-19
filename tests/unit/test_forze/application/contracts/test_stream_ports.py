"""Tests for forze.application.contracts.stream.ports."""

from __future__ import annotations

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

    async def tail(self, stream_mapping: dict[str, str], *, timeout: timedelta | None = None):
        if False:
            yield {
                "stream": "s",
                "id": "",
                "payload": _Msg(v=0),
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
    ):
        if False:
            yield {
                "stream": "s",
                "id": "",
                "payload": _Msg(v=0),
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


class TestStreamGroupQueryPort:
    def test_runtime_checkable(self) -> None:
        assert isinstance(_StubStreamGroupQuery(), StreamGroupQueryPort)

    async def test_read_and_ack(self) -> None:
        stub = _StubStreamGroupQuery()
        assert await stub.read("g", "c", {"a": "0"}) == []
        assert await stub.ack("g", "s", ("1", "2")) == 2


class TestStreamCommandPort:
    def test_runtime_checkable(self) -> None:
        assert isinstance(_StubStreamCommand(), StreamCommandPort)

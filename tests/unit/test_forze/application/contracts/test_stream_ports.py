"""Tests for forze.application.contracts.stream.ports."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Sequence
from datetime import datetime, timedelta

from pydantic import BaseModel

from forze.application.contracts.stream import (
    AckGroupDepth,
    AckStreamGroupAdminPort,
    AckStreamGroupQueryPort,
    PendingEntry,
    StreamCommandPort,
    StreamMessage,
    StreamQueryPort,
)


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
            StreamMessage(
                stream="s",
                id="1",
                payload=_Msg(v=1),
            )
        ]

    async def tail(
        self, stream_mapping: dict[str, str], *, timeout: timedelta | None = None
    ) -> AsyncGenerator[StreamMessage[_Msg]]:
        _ = stream_mapping, timeout
        yield StreamMessage(
            stream="s",
            id="t1",
            payload=_Msg(v=2),
        )


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
    ) -> AsyncGenerator[StreamMessage[_Msg]]:
        _ = group, consumer, stream_mapping, timeout
        yield StreamMessage(
            stream="s",
            id="gt1",
            payload=_Msg(v=3),
        )

    async def ack(self, group: str, stream: str, ids: Sequence[str]) -> int:
        return len(ids)

    async def claim(
        self,
        group: str,
        consumer: str,
        stream: str,
        *,
        idle: timedelta,
        limit: int | None = None,
    ) -> list[StreamMessage[_Msg]]:
        return [
            StreamMessage(
                stream=stream,
                id="c1",
                payload=_Msg(v=4),
            )
        ]

    async def pending(
        self,
        group: str,
        stream: str,
        *,
        limit: int | None = None,
    ) -> list[PendingEntry]:
        return [
            PendingEntry(
                id="c1",
                consumer="c",
                idle=timedelta(seconds=5),
                delivery_count=2,
            )
        ]


class _StubStreamGroupAdmin:
    async def ensure_group(self, group: str, stream: str, *, start_id: str = "$") -> None:
        return None

    async def depth(self, group: str, stream: str) -> AckGroupDepth:
        return AckGroupDepth(backlog=0, pending=0, oldest_pending_idle=None)

    async def trim_acknowledged(self, stream: str) -> int:
        return 0


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
        assert rows[0].payload.v == 2


class TestAckStreamGroupQueryPort:
    def test_runtime_checkable(self) -> None:
        assert isinstance(_StubStreamGroupQuery(), AckStreamGroupQueryPort)

    def test_query_port_excludes_control_plane(self) -> None:
        # ensure_group is control-plane (AckStreamGroupAdminPort), not on the data port
        assert not hasattr(_StubStreamGroupQuery(), "ensure_group")


class TestAckStreamGroupAdminPort:
    def test_runtime_checkable(self) -> None:
        assert isinstance(_StubStreamGroupAdmin(), AckStreamGroupAdminPort)

    async def test_ensure_group(self) -> None:
        assert await _StubStreamGroupAdmin().ensure_group("g", "s") is None

    async def test_depth_at_rest_semantics(self) -> None:
        assert (await _StubStreamGroupAdmin().depth("g", "s")).at_rest
        # an unknown backlog is never at rest — quiesce must fail closed on it
        assert not AckGroupDepth(backlog=None, pending=0, oldest_pending_idle=None).at_rest
        assert not AckGroupDepth(
            backlog=0, pending=1, oldest_pending_idle=timedelta(seconds=1)
        ).at_rest

    async def test_read_and_ack(self) -> None:
        stub = _StubStreamGroupQuery()
        assert await stub.read("g", "c", {"a": "0"}) == []
        assert await stub.ack("g", "s", ("1", "2")) == 2

    async def test_claim_and_pending(self) -> None:
        stub = _StubStreamGroupQuery()

        claimed = await stub.claim("g", "c", "s", idle=timedelta(seconds=30), limit=5)
        assert [m.id for m in claimed] == ["c1"]

        rows = await stub.pending("g", "s", limit=5)
        assert rows == [
            PendingEntry(
                id="c1",
                consumer="c",
                idle=timedelta(seconds=5),
                delivery_count=2,
            )
        ]

    async def test_tail_yields_messages(self) -> None:
        stub = _StubStreamGroupQuery()
        rows: list[StreamMessage[_Msg]] = []
        async for msg in stub.tail("g", "c", {"a": "0"}):
            rows.append(msg)
        assert len(rows) == 1
        assert rows[0].payload.v == 3


class TestStreamCommandPort:
    def test_runtime_checkable(self) -> None:
        assert isinstance(_StubStreamCommand(), StreamCommandPort)

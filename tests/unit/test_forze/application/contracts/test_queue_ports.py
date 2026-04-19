"""Tests for forze.application.contracts.queue.ports."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Sequence

from pydantic import BaseModel

from forze.application.contracts.queue import QueueCommandPort, QueueQueryPort
from forze.application.contracts.queue.types import QueueMessage


class _Msg(BaseModel):
    body: str


class _StubQueueQuery:
    async def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[QueueMessage[_Msg]]:
        return [
            {
                "queue": queue,
                "id": "1",
                "payload": _Msg(body="hi"),
            }
        ]

    async def consume(self, queue: str, *, timeout: timedelta | None = None):
        if False:
            yield {
                "queue": queue,
                "id": "",
                "payload": _Msg(body=""),
            }

    async def ack(self, queue: str, ids: Sequence[str]) -> int:
        return len(ids)

    async def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
    ) -> int:
        return len(ids)


class _StubQueueCommand:
    async def enqueue(
        self,
        queue: str,
        payload: _Msg,
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
    ) -> str:
        return "id-1"

    async def enqueue_many(
        self,
        queue: str,
        payloads: Sequence[_Msg],
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
    ) -> list[str]:
        return [f"id-{i}" for i in range(len(payloads))]


class TestQueueQueryPort:
    def test_is_runtime_checkable(self) -> None:
        stub = _StubQueueQuery()
        assert isinstance(stub, QueueQueryPort)

    @staticmethod
    async def test_receive_ack_nack() -> None:
        stub = _StubQueueQuery()
        msgs = await stub.receive("q", limit=5, timeout=timedelta(seconds=1))
        assert len(msgs) == 1
        assert await stub.ack("q", ("1",)) == 1
        assert await stub.nack("q", ("1",), requeue=False) == 1

    def test_non_conforming_not_instance(self) -> None:
        class Bad:
            pass

        assert not isinstance(Bad(), QueueQueryPort)


class TestQueueCommandPort:
    def test_is_runtime_checkable(self) -> None:
        assert isinstance(_StubQueueCommand(), QueueCommandPort)

    @staticmethod
    async def test_enqueue_and_many() -> None:
        stub = _StubQueueCommand()
        one = await stub.enqueue("q", _Msg(body="a"), type="t", key="k")
        assert one == "id-1"
        many = await stub.enqueue_many(
            "q",
            (_Msg(body="a"), _Msg(body="b")),
            enqueued_at=datetime(2020, 1, 1),
        )
        assert many == ["id-0", "id-1"]

"""Unit tests for queue contracts (QueueSpec and dep keys)."""

from datetime import timedelta
from typing import AsyncIterator

from pydantic import BaseModel

from forze.application.contracts.queue import (
    QueueReadDepKey,
    QueueReadPort,
    QueueSpec,
    QueueWriteDepKey,
    QueueWritePort,
)

# ----------------------- #


class _QueuePayload(BaseModel):
    value: str


class _StubQueue(QueueReadPort[_QueuePayload], QueueWritePort[_QueuePayload]):
    async def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ):
        return [{"queue": queue, "id": "1", "payload": _QueuePayload(value="x")}]

    async def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator:
        yield {"queue": queue, "id": "1", "payload": _QueuePayload(value="x")}

    async def ack(self, queue: str, ids: list[str]) -> int:
        return len(ids)

    async def nack(self, queue: str, ids: list[str], *, requeue: bool = True) -> int:
        return len(ids)

    async def enqueue(
        self,
        queue: str,
        payload: _QueuePayload,
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at=None,
    ) -> str:
        return "1"


class TestQueueSpec:
    def test_spec_contains_name_and_model(self) -> None:
        spec = QueueSpec(name="jobs", model=_QueuePayload)

        assert spec.name == "jobs"
        assert spec.model is _QueuePayload


class TestQueueDepKeys:
    def test_queue_read_dep_key_name(self) -> None:
        assert QueueReadDepKey.name == "queue_read"

    def test_queue_write_dep_key_name(self) -> None:
        assert QueueWriteDepKey.name == "queue_write"

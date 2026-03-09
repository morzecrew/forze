"""Unit tests for queue contracts (QueueSpec and dep keys)."""

from datetime import timedelta
from typing import AsyncIterator

import pytest
from pydantic import BaseModel

from forze.application.contracts.queue import (
    QueueConformity,
    QueueDepConformity,
    QueueReadDepKey,
    QueueReadDepPort,
    QueueReadPort,
    QueueSpec,
    QueueWriteDepKey,
    QueueWriteDepPort,
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
    def test_spec_contains_namespace_and_model(self) -> None:
        spec = QueueSpec(namespace="jobs", model=_QueuePayload)

        assert spec.namespace == "jobs"
        assert spec.model is _QueuePayload


class TestQueueDepKeys:
    def test_queue_read_dep_key_name(self) -> None:
        assert QueueReadDepKey.name == "queue_read"

    def test_queue_write_dep_key_name(self) -> None:
        assert QueueWriteDepKey.name == "queue_write"


class TestQueuePorts:
    @pytest.mark.asyncio
    async def test_stub_conforms_to_read_and_write_ports(self) -> None:
        queue = _StubQueue()

        assert isinstance(queue, QueueReadPort)
        assert isinstance(queue, QueueWritePort)

    def test_stub_conforms_to_queue_conformity(self) -> None:
        queue = _StubQueue()
        assert isinstance(queue, QueueConformity)


class _StubQueueDep(QueueReadDepPort, QueueWriteDepPort):
    def __call__(self, context, spec):
        return _StubQueue()


class TestQueueDeps:
    def test_stub_conforms_to_read_and_write_dep_ports(self) -> None:
        dep = _StubQueueDep()

        assert isinstance(dep, QueueReadDepPort)
        assert isinstance(dep, QueueWriteDepPort)

    def test_stub_conforms_to_queue_dep_conformity(self) -> None:
        dep = _StubQueueDep()
        assert isinstance(dep, QueueDepConformity)

"""Unit tests for queue contracts (QueueSpec and dep keys)."""

from collections.abc import AsyncGenerator
from datetime import timedelta

from pydantic import BaseModel

from forze.application.contracts.queue import (
    QueueCommandDepKey,
    QueueCommandPort,
    QueueQueryDepKey,
    QueueQueryPort,
    QueueSpec,
)
from forze.base.serialization import PydanticModelCodec

# ----------------------- #


class _QueuePayload(BaseModel):
    value: str


class _StubQueue(QueueQueryPort[_QueuePayload], QueueCommandPort[_QueuePayload]):
    async def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ):
        from forze.application.contracts.queue import QueueMessage

        return [
            QueueMessage(
                queue=queue,
                id="1",
                payload=_QueuePayload(value="x"),
            )
        ]

    async def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator:
        from forze.application.contracts.queue import QueueMessage

        yield QueueMessage(
            queue=queue,
            id="1",
            payload=_QueuePayload(value="x"),
        )

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
    def test_spec_contains_name_and_codec(self) -> None:
        spec = QueueSpec(
            name="jobs", codec=PydanticModelCodec(model_type=_QueuePayload)
        )

        assert spec.name == "jobs"
        assert spec.model_type is _QueuePayload
        assert spec.codec.model_type is _QueuePayload


class TestQueueDepKeys:
    def test_queue_query_dep_key_name(self) -> None:
        assert QueueQueryDepKey.name == "queue_query"

    def test_queue_command_dep_key_name(self) -> None:
        assert QueueCommandDepKey.name == "queue_command"

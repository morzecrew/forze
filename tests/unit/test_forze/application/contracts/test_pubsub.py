"""Unit tests for pubsub contract (PubSubSpec and dep keys)."""

from collections.abc import AsyncGenerator

from pydantic import BaseModel

from forze.application.contracts.pubsub import (
    PubSubCommandDepKey,
    PubSubCommandPort,
    PubSubQueryDepKey,
    PubSubQueryPort,
    PubSubSpec,
)
from forze.base.serialization import PydanticModelCodec

# ----------------------- #


class _PubSubPayload(BaseModel):
    value: str


class _StubPubSub(PubSubCommandPort[_PubSubPayload], PubSubQueryPort[_PubSubPayload]):
    async def publish(
        self,
        topic: str,
        payload: _PubSubPayload,
        *,
        type: str | None = None,
        key: str | None = None,
        published_at=None,
    ) -> None:
        return None

    async def subscribe(self, topics: list[str]) -> AsyncGenerator:
        if not topics:
            return

        from forze.application.contracts.pubsub import PubSubMessage

        yield PubSubMessage(
            topic=topics[0],
            payload=_PubSubPayload(value="x"),
        )


class TestPubSubSpec:
    def test_spec_contains_name_and_codec(self) -> None:
        spec = PubSubSpec(
            name="events", codec=PydanticModelCodec(model_type=_PubSubPayload)
        )

        assert spec.name == "events"
        assert spec.model_type is _PubSubPayload
        assert spec.codec.model_type is _PubSubPayload


class TestPubSubDepKeys:
    def test_pubsub_command_dep_key_name(self) -> None:
        assert PubSubCommandDepKey.name == "pubsub_command"

    def test_pubsub_query_dep_key_name(self) -> None:
        assert PubSubQueryDepKey.name == "pubsub_query"

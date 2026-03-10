"""Unit tests for pubsub contract (PubSubSpec and dep keys)."""

from typing import AsyncIterator

from pydantic import BaseModel

from forze.application.contracts.pubsub import (
    PubSubPublishDepKey,
    PubSubPublishPort,
    PubSubSpec,
    PubSubSubscribeDepKey,
    PubSubSubscribePort,
)

# ----------------------- #


class _PubSubPayload(BaseModel):
    value: str


class _StubPubSub(
    PubSubPublishPort[_PubSubPayload], PubSubSubscribePort[_PubSubPayload]
):
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

    async def subscribe(self, topics: list[str]) -> AsyncIterator:
        if not topics:
            return

        yield {
            "topic": topics[0],
            "payload": _PubSubPayload(value="x"),
        }


class TestPubSubSpec:
    def test_spec_contains_namespace_and_model(self) -> None:
        spec = PubSubSpec(namespace="events", model=_PubSubPayload)

        assert spec.namespace == "events"
        assert spec.model is _PubSubPayload


class TestPubSubDepKeys:
    def test_pubsub_publish_dep_key_name(self) -> None:
        assert PubSubPublishDepKey.name == "pubsub_publish"

    def test_pubsub_subscribe_dep_key_name(self) -> None:
        assert PubSubSubscribeDepKey.name == "pubsub_subscribe"

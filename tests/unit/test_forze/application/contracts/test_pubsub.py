"""Unit tests for pubsub contract (PubSubSpec and dep keys)."""

from typing import AsyncIterator

import pytest
from pydantic import BaseModel

from forze.application.contracts.pubsub import (
    PubSubPublishDepKey,
    PubSubPublishPort,
    PubSubSpec,
    PubSubSubscribeDepKey,
    PubSubSubscribePort,
    PubSubConformity,
    PubSubDepConformity,
    PubSubPublishDepPort,
    PubSubSubscribeDepPort,
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


class TestPubSubPorts:
    @pytest.mark.asyncio
    async def test_stub_conforms_to_publish_and_subscribe_ports(self) -> None:
        pubsub = _StubPubSub()

        assert isinstance(pubsub, PubSubPublishPort)
        assert isinstance(pubsub, PubSubSubscribePort)

    def test_stub_conforms_to_pubsub_conformity(self) -> None:
        pubsub = _StubPubSub()
        assert isinstance(pubsub, PubSubConformity)


class _StubPubSubDep(PubSubPublishDepPort, PubSubSubscribeDepPort):
    def __call__(self, context, spec):
        return _StubPubSub()


class TestPubSubDeps:
    def test_stub_conforms_to_publish_and_subscribe_dep_ports(self) -> None:
        dep = _StubPubSubDep()
        assert isinstance(dep, PubSubPublishDepPort)
        assert isinstance(dep, PubSubSubscribeDepPort)

    def test_stub_conforms_to_pubsub_dep_conformity(self) -> None:
        dep = _StubPubSubDep()
        assert isinstance(dep, PubSubDepConformity)

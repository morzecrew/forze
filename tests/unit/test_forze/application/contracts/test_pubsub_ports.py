"""Tests for forze.application.contracts.pubsub.ports."""

from __future__ import annotations

from datetime import datetime, timedelta

from pydantic import BaseModel

from forze.application.contracts.pubsub import PubSubCommandPort, PubSubQueryPort


class _Evt(BaseModel):
    n: int


class _StubPubSubCommand:
    async def publish(
        self,
        topic: str,
        payload: _Evt,
        *,
        type: str | None = None,
        key: str | None = None,
        published_at: datetime | None = None,
    ) -> None:
        return None


class _StubPubSubQuery:
    async def subscribe(self, topics: tuple[str, ...], *, timeout: timedelta | None = None):
        if False:
            yield {
                "topic": topics[0] if topics else "t",
                "payload": _Evt(n=0),
            }


class TestPubSubPorts:
    def test_command_runtime_checkable(self) -> None:
        assert isinstance(_StubPubSubCommand(), PubSubCommandPort)

    def test_query_runtime_checkable(self) -> None:
        assert isinstance(_StubPubSubQuery(), PubSubQueryPort)

"""Unit tests for :mod:`forze_socketio.emitter`."""

from typing import Any

import pytest
from pydantic import BaseModel

from forze_socketio.emitter import (
    SocketIOEventEmitter,
    SocketIOServerEvent,
)


# ----------------------- #


class StubSocketIOServer:
    """Minimal Socket.IO server stub for emit assertions."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def emit(
        self,
        event: str,
        data: Any = None,
        *,
        namespace: str | None = None,
        to: str | None = None,
        room: str | None = None,
        skip_sid: str | list[str] | None = None,
    ) -> None:
        self.calls.append(
            {
                "event": event,
                "data": data,
                "namespace": namespace,
                "to": to,
                "room": room,
                "skip_sid": skip_sid,
            }
        )


class ProgressPayload(BaseModel):
    """Typed payload for progress events."""

    done: int
    total: int


class TestSocketIOEventEmitter:
    """Tests for typed outbound event emission."""

    @pytest.mark.asyncio
    async def test_emit_validates_and_serializes_payload(self) -> None:
        sio = StubSocketIOServer()
        emitter = SocketIOEventEmitter(sio=sio)  # pyright: ignore[reportArgumentType]
        event = SocketIOServerEvent[ProgressPayload](
            event="progress",
            payload_type=ProgressPayload,
            namespace="/jobs",
        )

        await emitter.emit(
            event,
            ProgressPayload(done=1, total=3),
            to="sid-1",
            room="room-1",
            skip_sid="sid-2",
        )

        assert sio.calls == [
            {
                "event": "progress",
                "data": {"done": 1, "total": 3},
                "namespace": "/jobs",
                "to": "sid-1",
                "room": "room-1",
                "skip_sid": "sid-2",
            }
        ]

    @pytest.mark.asyncio
    async def test_namespace_emitter_overrides_event_namespace(self) -> None:
        sio = StubSocketIOServer()
        emitter = SocketIOEventEmitter(sio=sio)  # pyright: ignore[reportArgumentType]
        ns_emitter = emitter.for_namespace("/tenant")
        event = SocketIOServerEvent[ProgressPayload](
            event="progress",
            payload_type=ProgressPayload,
            namespace="/jobs",
        )

        await ns_emitter.emit(event, {"done": 2, "total": 5})

        assert sio.calls == [
            {
                "event": "progress",
                "data": {"done": 2, "total": 5},
                "namespace": "/tenant",
                "to": None,
                "room": None,
                "skip_sid": None,
            }
        ]

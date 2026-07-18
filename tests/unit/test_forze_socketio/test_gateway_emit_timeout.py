"""Gateway bounded emit — a stuck delivery can't wedge the consume loop."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any, cast

import pytest

from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.execution import ExecutionContext
from forze_socketio import RealtimeGateway, RealtimeSignalSource, SignalHandler

# ----------------------- #


class _NullSource(RealtimeSignalSource):
    async def run(
        self,
        ctx: ExecutionContext,
        handler: SignalHandler,
        *,
        stop: asyncio.Event | None = None,
    ) -> None:  # pragma: no cover
        await asyncio.Event().wait()


class _HangingSio:
    async def emit(self, *args: Any, **kwargs: Any) -> None:
        await asyncio.Event().wait()  # never completes


class _OkSio:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str | None]] = []

    async def emit(
        self, event: str, data: Any = None, *, room: str | None = None,
        namespace: str | None = None, **_: Any,
    ) -> None:
        self.calls.append((event, room))


def _gateway(sio: Any, *, emit_timeout: timedelta | None) -> RealtimeGateway:
    return RealtimeGateway(sio=cast(Any, sio), source=_NullSource(), emit_timeout=emit_timeout)


# ----------------------- #


async def test_emit_times_out_when_delivery_hangs() -> None:
    gw = _gateway(_HangingSio(), emit_timeout=timedelta(seconds=0.05))
    signal = RealtimeSignal.of(Audience.topic("c"), "typing", {"x": 1})

    with pytest.raises(asyncio.TimeoutError):
        await gw._emit(signal, None)


async def test_emit_within_timeout_delivers_normally() -> None:
    sio = _OkSio()
    gw = _gateway(sio, emit_timeout=timedelta(seconds=1))
    signal = RealtimeSignal.of(Audience.principal("u"), "order.shipped", {"x": 1})

    await gw._emit(signal, None)

    assert sio.calls == [("order.shipped", "principal:u")]


async def test_no_timeout_configured_awaits_emit() -> None:
    sio = _OkSio()
    gw = _gateway(sio, emit_timeout=None)
    signal = RealtimeSignal.of(Audience.topic("room"), "ping", {})

    await gw._emit(signal, None)

    assert sio.calls == [("ping", "topic:room")]

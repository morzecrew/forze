"""Tests for the recording realtime port and its ``ctx.realtime()`` wiring."""

from __future__ import annotations

from pydantic import BaseModel

from forze.application.contracts.realtime import Audience, RealtimePort
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze_mock import MockDepsModule, RecordingRealtimePort

# ----------------------- #


class _Msg(BaseModel):
    text: str


# ....................... #


async def test_recording_port_records_emits() -> None:
    port = RecordingRealtimePort()

    await port.emit(Audience.principal("u-1"), "message.new", _Msg(text="hi"))

    assert len(port.emits) == 1
    recorded = port.emits[0]
    assert recorded.audience == Audience.principal("u-1")
    assert recorded.event == "message.new"
    assert recorded.payload == _Msg(text="hi")


# ....................... #


async def test_events_for_filters_by_audience() -> None:
    port = RecordingRealtimePort()
    a, b = Audience.principal("u-1"), Audience.topic("chat")

    await port.emit(a, "e1", _Msg(text="a"))
    await port.emit(b, "e2", _Msg(text="b"))
    await port.emit(a, "e3", _Msg(text="c"))

    assert [e.event for e in port.events_for(a)] == ["e1", "e3"]


# ....................... #


async def test_clear_forgets_everything() -> None:
    port = RecordingRealtimePort()
    await port.emit(Audience.principal("u-1"), "e", _Msg(text="x"))

    port.clear()

    assert port.emits == []


# ....................... #


async def test_ctx_realtime_resolves_provided_recorder() -> None:
    recorder = RecordingRealtimePort()
    module = MockDepsModule(realtime=recorder)
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        port = ctx.realtime()

        assert isinstance(port, RealtimePort)
        await port.emit(Audience.topic("chat"), "ping", _Msg(text="hi"))

    assert len(recorder.emits) == 1
    assert recorder.emits[0].event == "ping"


# ....................... #


async def test_ctx_realtime_defaults_to_a_working_recorder() -> None:
    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        port = ctx.realtime()

        assert isinstance(port, RecordingRealtimePort)
        await port.emit(Audience.tenant(), "broadcast", _Msg(text="hi"))
        assert len(port.emits) == 1

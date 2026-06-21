"""Unit tests for :class:`ResolutionContext`."""

import pytest

from forze.application.contracts.deps import DepKey
from forze.application.execution.deps.resolution import (
    RecordingResolutionTracer,
    ResolutionContext,
    frame_for,
    resolution_tracer_from_flag,
)
from forze.base.exceptions import CoreException

_A = DepKey[str]("a")
_B = DepKey[str]("b")


class TestResolutionContextStack:
    def test_push_pop(self) -> None:
        ctx = ResolutionContext(resolution_tracer_from_flag(False))
        frame = frame_for(_A, None)
        token = ctx.push(frame)
        assert ctx.stack() == (frame,)
        ctx.pop(token)
        assert ctx.stack() == ()

    def test_push_cycle_raises(self) -> None:
        ctx = ResolutionContext(resolution_tracer_from_flag(False))
        frame = frame_for(_A, None)
        token = ctx.push(frame)

        try:
            with pytest.raises(CoreException, match="Cyclic dependency resolution"):
                ctx.push(frame)
        finally:
            ctx.pop(token)

    def test_assert_not_active(self) -> None:
        ctx = ResolutionContext(resolution_tracer_from_flag(False))
        frame = frame_for(_A, None)
        token = ctx.push(frame)

        try:
            with pytest.raises(CoreException, match="Cyclic dependency resolution"):
                ctx.assert_not_active(frame)
        finally:
            ctx.pop(token)


class TestResolutionContextTracing:
    def test_push_records_edge(self) -> None:
        tracer = RecordingResolutionTracer()
        ctx = ResolutionContext(tracer)
        frame_a = frame_for(_A, None)
        frame_b = frame_for(_B, None)

        token_a = ctx.push(frame_a)
        ctx.push(frame_b)
        ctx.pop(token_a)

        trace = tracer.snapshot()

        assert trace is not None
        assert (frame_a, frame_b) in trace.edges

    def test_provide_edge_without_push(self) -> None:
        tracer = RecordingResolutionTracer()
        ctx = ResolutionContext(tracer)
        frame_a = frame_for(_A, None)
        frame_b = frame_for(_B, None)

        token = ctx.push(frame_a)
        ctx.record_provide_edge(frame_b)
        ctx.pop(token)

        trace = tracer.snapshot()

        assert trace is not None
        assert (frame_a, frame_b) in trace.edges

    def test_two_contexts_isolated(self) -> None:
        tracer_a = RecordingResolutionTracer()
        tracer_b = RecordingResolutionTracer()
        ctx_a = ResolutionContext(tracer_a)
        _ctx_b = ResolutionContext(tracer_b)
        frame_a = frame_for(_A, None)
        frame_b = frame_for(_B, None)

        token = ctx_a.push(frame_a)
        ctx_a.record_provide_edge(frame_b)
        ctx_a.pop(token)

        assert tracer_a.snapshot() is not None
        assert tracer_b.snapshot() is None
        assert _ctx_b.stack() == ()

"""Subsequence matching for golden runtime traces."""

from __future__ import annotations

from collections.abc import Sequence
from typing import final

import attrs

from forze.base.errors import CoreError

from .buffer import RuntimeTrace
from .events import TracingEvent

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TraceExpectation:
    """Expected event subset for ordered trace matching."""

    domain: str
    op: str
    surface: str | None = None
    route: str | None = None
    phase: str | None = None
    tx_depth: int | None = None
    """When set, ``TracingEvent.tx_depth`` must equal this value."""


# ....................... #


def _event_matches(event: TracingEvent, expectation: TraceExpectation) -> bool:
    if event.domain != expectation.domain or event.op != expectation.op:
        return False

    if expectation.surface is not None and event.surface != expectation.surface:
        return False

    if expectation.route is not None and event.route != expectation.route:
        return False

    if expectation.phase is not None and event.phase != expectation.phase:
        return False

    if expectation.tx_depth is not None and event.tx_depth != expectation.tx_depth:
        return False

    return True


def _events_from(trace: RuntimeTrace | Sequence[TracingEvent]) -> Sequence[TracingEvent]:
    if isinstance(trace, RuntimeTrace):
        return trace.events
    return trace


def assert_trace_contains(
    trace: RuntimeTrace | Sequence[TracingEvent] | None,
    expectations: Sequence[TraceExpectation],
) -> None:
    """Assert *expectations* appear in order within *trace* (not necessarily adjacent)."""

    if trace is None:
        events: Sequence[TracingEvent] = ()
    else:
        events = _events_from(trace)

    index = 0

    for expectation in expectations:
        matched = False

        while index < len(events):
            if _event_matches(events[index], expectation):
                matched = True
                index += 1
                break
            index += 1

        if not matched:
            msg = (
                f"Expected trace to contain {expectation!r} "
                f"after index {index - 1}; trace has {len(events)} event(s)"
            )
            raise CoreError(msg)


def assert_trace_equals(
    trace: RuntimeTrace | Sequence[TracingEvent] | None,
    expectations: Sequence[TraceExpectation],
) -> None:
    """Assert *trace* has exactly len(expectations) events matching in order."""

    if trace is None:
        events: list[TracingEvent] = []
    else:
        events = list(_events_from(trace))

    if len(events) != len(expectations):
        msg = (
            f"Expected {len(expectations)} trace event(s), got {len(events)}"
        )
        raise CoreError(msg)

    for event, expectation in zip(events, expectations, strict=True):
        if not _event_matches(event, expectation):
            msg = f"Event {event!r} does not match expectation {expectation!r}"
            raise CoreError(msg)

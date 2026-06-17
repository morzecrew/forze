"""Recorder: a structured, virtual-time-stamped history of a simulation run.

The oracle's substrate. Code under simulation calls :func:`record_event` to log domain
facts (an effect applied, a lock acquired, an id minted); the recorder stamps each with
the virtual monotonic clock and a sequence number. Invariants (see
:mod:`forze_dst.invariants`) then assert over the resulting :class:`History` — *DST only
finds the bugs you assert*. The recorder is ambient (a ContextVar, like the time/entropy
seams), so deep handler code can record without threading a history object through.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator, Mapping, final

import attrs

from forze.base.primitives import monotonic

# ----------------------- #


@final
@attrs.define(frozen=True, kw_only=True)
class Event:
    """One recorded fact: a monotonically increasing seq, a kind, the virtual time, fields."""

    seq: int
    kind: str
    at: float
    """Virtual monotonic time (seconds) the event was recorded at."""
    fields: Mapping[str, Any]


@final
@attrs.define(frozen=True, kw_only=True)
class History:
    """An immutable, ordered history of recorded events for one seeded run."""

    seed: int
    events: tuple[Event, ...]

    def of_kind(self, kind: str) -> list[Event]:
        """All events of *kind*, in recorded order."""

        return [event for event in self.events if event.kind == kind]


# ....................... #


@final
@attrs.define
class Recorder:
    """Accumulates recorded events for a single run; freeze with :attr:`history`."""

    seed: int
    _events: list[Event] = attrs.field(factory=list, init=False)
    _seq: int = attrs.field(default=0, init=False)

    def record(self, kind: str, **fields: Any) -> None:
        self._events.append(
            Event(seq=self._seq, kind=kind, at=monotonic(), fields=dict(fields))
        )
        self._seq += 1

    @property
    def history(self) -> History:
        return History(seed=self.seed, events=tuple(self._events))


# ....................... #

_RECORDER: ContextVar[Recorder | None] = ContextVar("dst_recorder", default=None)


def current_recorder() -> Recorder | None:
    """The recorder active in the current context, or ``None`` when not recording."""

    return _RECORDER.get()


@contextmanager
def bind_recorder(recorder: Recorder) -> Iterator[None]:
    """Bind *recorder* as the active recorder for the duration of the block."""

    token = _RECORDER.set(recorder)

    try:
        yield

    finally:
        _RECORDER.reset(token)


def record_event(kind: str, **fields: Any) -> None:
    """Record a domain fact into the active recorder — a no-op when none is bound.

    Cheap and safe to leave in application/handler code: outside a recorded simulation
    it does nothing.
    """

    recorder = _RECORDER.get()

    if recorder is not None:
        recorder.record(kind, **fields)

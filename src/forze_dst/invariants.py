"""Invariants over a recorded :class:`History` — the oracle's assertions.

*DST only finds the bugs you assert.* An :data:`Invariant` is any callable from a
history to the violations it found (empty == holds). This module ships a few reusable
built-ins and a generic :func:`expect`; apps add their own. The checker runs them all
and collects every violation.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable, Sequence, final

import attrs

from forze_dst.recorder import Event, History

# ----------------------- #


@final
@attrs.define(frozen=True, kw_only=True)
class Violation:
    """A single invariant breach: which invariant, a message, and the implicating events."""

    invariant: str
    message: str
    events: tuple[Event, ...] = ()


Invariant = Callable[[History], list[Violation]]
"""A check from a history to the violations it found (empty == satisfied)."""


def check(history: History, invariants: Sequence[Invariant]) -> list[Violation]:
    """Run every invariant over *history* and collect all violations."""

    violations: list[Violation] = []

    for invariant in invariants:
        violations.extend(invariant(history))

    return violations


# ....................... #
# Built-in invariants (factories returning an Invariant).


def no_duplicate_effect(kind: str, *, by: str) -> Invariant:
    """Each ``kind`` event must be unique on ``fields[by]`` — exactly-once effect.

    Catches a non-idempotent consumer applying a redelivered/duplicate message twice.
    """

    def _check(history: History) -> list[Violation]:
        seen: dict[object, Event] = {}
        violations: list[Violation] = []

        for event in history.of_kind(kind):
            identifier = event.fields.get(by)

            if identifier in seen:
                violations.append(
                    Violation(
                        invariant="no_duplicate_effect",
                        message=f"{kind!r} effect for {by}={identifier!r} occurred more than once",
                        events=(seen[identifier], event),
                    )
                )
            else:
                seen[identifier] = event

        return violations

    return _check


def monotonic_per(kind: str, value: str, *, actor: str) -> Invariant:
    """``fields[value]`` must never decrease within each ``fields[actor]`` group.

    For HLC/sequence monotonicity per replica/actor.
    """

    def _check(history: History) -> list[Violation]:
        last: dict[object, object] = {}
        violations: list[Violation] = []

        for event in history.of_kind(kind):
            who = event.fields.get(actor)
            current = event.fields[value]

            if who in last and current < last[who]:  # type: ignore[operator]
                violations.append(
                    Violation(
                        invariant="monotonic_per",
                        message=f"{value} went backwards for {actor}={who!r}: {last[who]} -> {current}",
                        events=(event,),
                    )
                )

            last[who] = current

        return violations

    return _check


def mutual_exclusion(
    kind: str,
    *,
    resource: str,
    start: str,
    end: str,
) -> Invariant:
    """No two ``kind`` holds may overlap in ``[start, end)`` for the same ``resource``.

    For validating a distributed lock / critical section across replicas: a correct
    lock serializes holders, so their intervals never overlap; concurrent entry shows
    up here as an overlap.
    """

    def _check(history: History) -> list[Violation]:
        by_resource: dict[Any, list[tuple[float, float, Event]]] = defaultdict(list)
        for event in history.of_kind(kind):
            by_resource[event.fields[resource]].append(
                (event.fields[start], event.fields[end], event)
            )

        violations: list[Violation] = []
        for res, holds in by_resource.items():
            holds.sort(key=lambda hold: hold[0])
            max_end: float | None = None
            max_event: Event | None = None

            for hold_start, hold_end, event in holds:
                if max_end is not None and hold_start < max_end:
                    violations.append(
                        Violation(
                            invariant="mutual_exclusion",
                            message=f"overlapping holds on resource {res!r}",
                            events=(max_event, event) if max_event else (event,),
                        )
                    )
                if max_end is None or hold_end > max_end:
                    max_end, max_event = hold_end, event

        return violations

    return _check


def expect(
    kind: str,
    predicate: Callable[[Event], bool],
    *,
    message: str,
) -> Invariant:
    """Generic app invariant: every ``kind`` event must satisfy *predicate*."""

    def _check(history: History) -> list[Violation]:
        return [
            Violation(invariant="expect", message=message, events=(event,))
            for event in history.of_kind(kind)
            if not predicate(event)
        ]

    return _check

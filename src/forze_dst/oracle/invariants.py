"""Invariants over a recorded :class:`History` — the oracle's assertions.

*DST only finds the bugs you assert.* An :data:`Invariant` is any callable from a
history to the violations it found (empty == holds). This module ships a few reusable
built-ins and a generic :func:`expect`; apps add their own. The checker runs them all
and collects every violation.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable, Mapping, Sequence, final

import attrs

from forze_dst.oracle.recorder import Event, History

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


def no_unexpected_error() -> Invariant:
    """No operation raised an *unexpected* exception (a bug — ``KeyError``, ``TypeError``, …).

    Domain failures (``CoreException``) are expected outcomes and pass; anything else is a
    latent bug surfaced under the explored interleavings and injected faults. This is the
    zero-instrumentation safety net — it holds against any operation registry without a
    single app-written invariant, so ``forze dst run`` is useful on an uninstrumented app.

    Reads the operation outcome from the engine trace (the single source of truth): the engine
    classifies each failed operation ``failed`` (a declared ``CoreException`` — expected) or
    ``error`` (an unhandled exception — a bug); this flags only the latter.
    """

    def _check(history: History) -> list[Violation]:
        return [
            Violation(
                invariant="no_unexpected_error",
                message=(
                    f"operation {event.fields.get('op')!r} raised an unexpected "
                    f"{event.fields.get('error')}"
                ),
                events=(event,),
            )
            for event in history.of_kind("operation")
            if event.fields.get("outcome") == "error"
        ]

    return _check


def operation_succeeds(*ops: str) -> Invariant:
    """Each named operation must always reach a successful (``ok``) outcome.

    Stricter than :func:`no_unexpected_error`: a *declared* domain failure (``failed``) on these
    operations is flagged too — for operations a scenario guarantees should succeed (e.g. a
    confirm after a valid arrange). With no *ops* it applies to every operation. Reads the
    operation outcome the engine trace records (the single source), projected into the history.
    """

    wanted = frozenset(ops)

    def _check(history: History) -> list[Violation]:
        return [
            Violation(
                invariant="operation_succeeds",
                message=(
                    f"operation {event.fields.get('op')!r} did not succeed "
                    f"(outcome={event.fields.get('outcome')})"
                ),
                events=(event,),
            )
            for event in history.of_kind("operation")
            if (not wanted or event.fields.get("op") in wanted)
            and event.fields.get("outcome") != "ok"
        ]

    return _check


def completes_within(op: str, seconds: float) -> Invariant:
    """Every ``op`` operation must finish within *seconds* of **virtual** time (invoke→return).

    Reads the operation span the engine trace records (projected into the history). A slow
    downstream — modeled by a seeded ``LatencyProfile`` — that pushes an operation past its
    deadline is caught here with zero handler instrumentation: the time twin of a concurrency
    invariant, surfaced from the converged trace alone.
    """

    def _check(history: History) -> list[Violation]:
        violations: list[Violation] = []

        for event in history.of_kind("operation"):
            if event.fields.get("op") != op:
                continue

            elapsed = float(event.fields.get("returned_at", event.at)) - float(
                event.fields.get("invoked_at", event.at)
            )

            if elapsed > seconds:
                violations.append(
                    Violation(
                        invariant="completes_within",
                        message=f"operation {op!r} took {elapsed:.6f}s > {seconds}s",
                        events=(event,),
                    )
                )

        return violations

    return _check


def single_key_per_operation(
    op: str, *, surface: str = "document_command"
) -> Invariant:
    """Each ``op`` execution must touch at most one entity **key** on *surface*.

    Reads the entity key the engine trace records for every keyed port call and attributes each
    call to the operation span that contains it (in the trace's own sequence space — true
    execution order). An operation that writes to two different keys — the classic *wrong-entity*
    bug (charged the wrong account) — is caught from the trace alone: no payload capture, no
    handler instrumentation. Each call is credited to its innermost (most-recently-started) span,
    so overlapping same-op spans under concurrency are not double-counted.
    """

    def _check(history: History) -> list[Violation]:
        spans = [
            (
                int(event.fields.get("start_seq", -1)),
                int(event.fields.get("end_seq", -1)),
                event,
            )
            for event in history.of_kind("operation")
            if event.fields.get("op") == op
        ]

        if not spans:
            return []

        keys_by_span: dict[int, set[Any]] = defaultdict(set)

        for event in history.of_kind("trace"):
            if (
                event.fields.get("surface") != surface
                or event.fields.get("key") is None
            ):
                continue

            seq = int(event.fields.get("trace_seq", -1))
            covering = [span for span in spans if span[0] <= seq <= span[1]]

            if not covering:
                continue

            owner = max(covering, key=lambda span: span[0])  # innermost (latest start)
            keys_by_span[owner[2].seq].add(event.fields["key"])

        return [
            Violation(
                invariant="single_key_per_operation",
                message=(
                    f"operation {op!r} touched multiple keys on {surface}: "
                    f"{sorted(str(key) for key in keys_by_span[event.seq])}"
                ),
                events=(event,),
            )
            for _, _, event in spans
            if len(keys_by_span[event.seq]) > 1
        ]

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


# ....................... #
# Value-level invariants — read the redaction-applied call values captured under
# ``SimulationConfig.capture_values`` (off by default; see E3.2). They assert on *what* was
# written / read, not just which key — the class of correctness bugs (stale reads, wrong values)
# that an id-only trace cannot see.


def read_your_writes(
    surface: str,
    *,
    value_field: str,
) -> Invariant:
    """On *surface*, a keyed read must observe the most recently written value for its key.

    Walks the captured trace in order, per entity key tracking the last value written
    (``payload[value_field]``) and checking every read's observed value (``result[value_field]``)
    against it. A read that returns a *stale* value — an earlier write, or none of the writes —
    is the read-your-writes violation: a cache that wasn't invalidated, a replica that lagged, a
    read off the wrong snapshot. Requires ``capture_values`` (else there are no payloads to read,
    and it holds vacuously). Writes with no key (creates that mint their own id) seed nothing —
    the check covers keyed write→read flows (updates).
    """

    def _check(history: History) -> list[Violation]:
        last_written: dict[Any, Any] = {}
        violations: list[Violation] = []

        for event in history.of_kind("trace"):
            fields = event.fields
            if fields.get("surface") != surface:
                continue

            key = fields.get("key")
            if key is None:
                continue

            payload = fields.get("payload")
            if payload is not None and value_field in payload:
                last_written[key] = payload[value_field]

            result = fields.get("result")
            if result is not None and value_field in result and key in last_written:
                observed = result[value_field]
                if observed != last_written[key]:
                    violations.append(
                        Violation(
                            invariant="read_your_writes",
                            message=(
                                f"read on {surface!r} key={key!r} observed "
                                f"{value_field}={observed!r}, but the last write was "
                                f"{last_written[key]!r}"
                            ),
                            events=(event,),
                        )
                    )

        return violations

    return _check


def expect_value(
    surface: str,
    predicate: Callable[[Mapping[str, Any]], bool],
    *,
    on: str = "payload",
    message: str,
) -> Invariant:
    """Every captured value on *surface* must satisfy *predicate* — the value-level :func:`expect`.

    Reads the ``payload`` (a write, the default) or ``result`` (a read, ``on="result"``) value map
    each call captured under ``capture_values`` and asserts *predicate* over it — the wrong-value
    guard (a write whose value isn't what the contract requires) and any other value constraint.
    Vacuous without ``capture_values`` (no values to check).
    """

    def _check(history: History) -> list[Violation]:
        violations: list[Violation] = []

        for event in history.of_kind("trace"):
            if event.fields.get("surface") != surface:
                continue

            value = event.fields.get(on)
            if value is not None and not predicate(value):
                violations.append(
                    Violation(
                        invariant="expect_value", message=message, events=(event,)
                    )
                )

        return violations

    return _check

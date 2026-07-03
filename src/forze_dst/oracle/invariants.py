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


_DEADLOCK_KIND = "deadlock"


def _deadlock_violations(history: History) -> list[Violation]:
    """A recorded ``deadlock`` event is always a violation — the workload could not make progress.

    The loop records one when it goes quiescent (no ready callbacks, no pending timer) instead of
    hanging; the run substrate folds that into the history so it lands here rather than aborting
    the sweep, and a normal find → minimize → report follows.
    """

    return [
        Violation(
            invariant="no_deadlock",
            message="the workload deadlocked — "
            + str(event.fields.get("detail", "no ready work and no pending timer")),
            events=(event,),
        )
        for event in history.of_kind(_DEADLOCK_KIND)
    ]


def check(history: History, invariants: Sequence[Invariant]) -> list[Violation]:
    """Run every invariant over *history* and collect all violations.

    A recorded deadlock is reported regardless of the declared invariants — a workload that
    cannot make progress is a bug on any app — so it surfaces here even uninstrumented, the way a
    sweep catches an unexpected crash.
    """

    violations: list[Violation] = _deadlock_violations(history)

    for invariant in invariants:
        violations.extend(invariant(history))

    return violations


# ....................... #
# Built-in invariants (factories returning an Invariant).


def no_resource_leak(
    *,
    open_op: str,
    close_op: str,
    domain: str | None = None,
    surface: str | None = None,
    by: str | None = None,
) -> Invariant:
    """Every resource opened during the run must be closed by its end — pairs *open_op* against
    *close_op* on the trace and flags any imbalance.

    Match the traced calls by *domain* (the trace domain, e.g. ``"tx"``) and/or *surface* (a port
    name), then count ``open_op``-named operations against ``close_op``-named ones, grouped by
    ``fields[by]`` (e.g. the transaction ``route``) when given. A group left with more opens than
    closes leaked a resource — a scope entered but never exited, a handle taken but never returned.
    Reads only the id-only engine trace (no values), so it costs nothing extra.

    This catches a bug class nobody writes assertions for. Pair it with normal and error runs, not
    a :class:`~forze_dst.faults.CrashPolicy` — a crash *legitimately* abandons an open scope (the
    process dies mid-flight), which would read as a leak.
    """

    def _check(history: History) -> list[Violation]:
        opened: dict[object, int] = defaultdict(int)
        closed: dict[object, int] = defaultdict(int)
        first_open: dict[object, Event] = {}

        for event in history.of_kind("trace"):
            fields = event.fields

            if domain is not None and fields.get("trace_domain") != domain:
                continue
            if surface is not None and fields.get("surface") != surface:
                continue

            group = fields.get(by) if by is not None else None
            op = fields.get("op")

            if op == open_op:
                opened[group] += 1
                first_open.setdefault(group, event)
            elif op == close_op:
                closed[group] += 1

        violations: list[Violation] = []

        for group, count in opened.items():
            leaked = count - closed.get(group, 0)

            if leaked > 0:
                where = f" for {by}={group!r}" if by is not None else ""
                violations.append(
                    Violation(
                        invariant="no_resource_leak",
                        message=f"{leaked} {open_op!r} not matched by {close_op!r}{where} "
                        "— a resource was left open at end of run",
                        events=(first_open[group],),
                    )
                )

        return violations

    return _check


def no_unclosed_transaction() -> Invariant:
    """Every transaction scope entered must exit by end of run.

    A tx ``enter`` with no matching ``exit`` is an unclosed transaction — a scope abandoned
    without commit or rollback (a bug in an error/cleanup path that skipped the scope teardown).
    Reads the ``tx`` boundaries the engine traces, grouped by route. Like
    :func:`no_resource_leak`, do not pair this with a crash policy — a crash legitimately abandons
    a transaction.
    """

    return no_resource_leak(domain="tx", open_op="enter", close_op="exit", by="route")


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


def no_duplicate_trace_effect(
    *,
    domain: str,
    op: str,
    outcome: str,
    by: Sequence[str],
    name: str | None = None,
) -> Invariant:
    """Each runtime-trace effect matching ``(domain, op, outcome)`` occurs at most once per group.

    Reads the folded ``"trace"`` events (``fields``: ``trace_domain`` / ``op`` / ``outcome`` /
    ``route`` / ``key`` / …) and groups them by the ``by`` field names — so an effect a component
    records once per logical action is asserted to fire exactly once even **across a crash +
    recovery**. E.g. a durable step's ``executed`` event keyed by run id + step id: a completed
    step must replay from its journal on recovery, never re-execute.
    """

    invariant_name = name or f"no_duplicate_{domain}_{op}"

    def _check(history: History) -> list[Violation]:
        seen: dict[tuple[object, ...], Event] = {}
        violations: list[Violation] = []

        for event in history.of_kind("trace"):
            fields = event.fields

            if (
                fields.get("trace_domain") != domain
                or fields.get("op") != op
                or fields.get("outcome") != outcome
            ):
                continue

            identifier = tuple(fields.get(field) for field in by)

            if identifier in seen:
                violations.append(
                    Violation(
                        invariant=invariant_name,
                        message=(
                            f"{domain}.{op} effect {identifier!r} (outcome {outcome!r}) "
                            "occurred more than once"
                        ),
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
        # Values are recorded fields (dynamically typed), compared with ``<`` for monotonicity;
        # the key is the actor field. Both are opaque to the type system, hence ``Any`` for the
        # compared value rather than a suppressed operator error.
        last: dict[object, Any] = {}
        violations: list[Violation] = []

        for event in history.of_kind(kind):
            who = event.fields.get(actor)
            current = event.fields[value]

            if who in last and current < last[who]:
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

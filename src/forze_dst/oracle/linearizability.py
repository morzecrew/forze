"""Linearizability checking — the strongest correctness oracle for concurrent objects.

Given a recorded history of operations (each with a real-time interval ``[invoked,
returned]`` and the result it observed) and a *sequential specification*, decide whether
some total order, consistent with the real-time partial order, reproduces every observed
result. If none does, the object is not linearizable — a read saw a value no legal
ordering can justify.

The search is Wing & Gong's recursive linearize/lift with Lowe's memoization, and it is
**P-compositional**: a history over independent keys is linearizable iff each per-key
sub-history is, so we partition by key and check each in isolation (the optimization that
makes this tractable). Operations are recorded with :func:`record_operation`; the result
is the :func:`linearizable` invariant, which plugs into the oracle like any other.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any, Protocol, final, runtime_checkable

import attrs

from forze.base.primitives import monotonic
from forze_dst.oracle.invariants import Invariant, Violation
from forze_dst.oracle.recorder import History, record_event

# ----------------------- #


@runtime_checkable
class SequentialSpec(Protocol):
    """A deterministic sequential model of an object.

    ``initial()`` returns the start state (must be *hashable* — it keys the memo).
    ``apply(state, op, args)`` returns ``(next_state, result)``; the checker accepts an
    operation in a candidate order only when this ``result`` equals the one recorded.
    """

    def initial(self) -> Any: ...  # pragma: no cover

    def apply(
        self, state: Any, op: str, args: tuple[Any, ...]
    ) -> tuple[Any, Any]: ...  # pragma: no cover


@final
@attrs.define(frozen=True, kw_only=True)
class RegisterSpec:
    """Sequential spec for a read/write register — ``write(v)`` then ``read()`` sees ``v``."""

    initial_value: Any = 0

    def initial(self) -> Any:
        return self.initial_value

    def apply(self, state: Any, op: str, args: tuple[Any, ...]) -> tuple[Any, Any]:
        if op == "write":
            return args[0], None
        if op == "read":
            return state, state
        raise ValueError(f"unknown register op {op!r}")


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class _Op:
    key: Any
    op: str
    args: tuple[Any, ...]
    result: Any
    invoked_at: float
    returned_at: float
    session: Any = None
    """The session/process that issued the op — the program-order axis for the weaker models
    (``sequential`` / ``monotonic_reads``). ``None`` when not recorded."""


@final
@attrs.define
class _Call:
    """Mutable cell a recorded operation sets its result on."""

    result: Any = None


@asynccontextmanager
async def record_operation(
    key: Any,
    op: str,
    args: tuple[Any, ...] = (),
    *,
    session: Any = None,
) -> AsyncIterator[_Call]:
    """Record one operation's real-time interval + result for the consistency oracles.

    Usage::

        async with record_operation("reg", "read", session=node_id) as call:
            call.result = await register.read()

    The invoke time is captured on enter and the return time on (normal) exit; an operation whose
    body raises is not recorded. *session* tags the issuing process/client — pass it to make the
    weaker, program-order models (:func:`sequential`, :func:`monotonic_reads`) meaningful;
    :func:`linearizable` ignores it (it orders by real time alone).
    """

    invoked_at = monotonic()
    call = _Call()
    yield call
    record_event(
        "operation",
        key=key,
        op=op,
        args=tuple(args),
        result=call.result,
        invoked_at=invoked_at,
        returned_at=monotonic(),
        session=session,
    )


# ....................... #


def _linearizable_one(ops: Sequence[_Op], spec: SequentialSpec) -> bool:
    """Wing-Gong recursive search with Lowe memoization, for a single key's history."""

    ordered = tuple(sorted(ops, key=lambda o: (o.invoked_at, o.returned_at)))
    failed: set[tuple[frozenset[int], Any]] = set()

    def search(remaining: frozenset[int], state: Any) -> bool:
        if not remaining:
            return True

        memo_key = (remaining, state)
        if memo_key in failed:
            return False

        # A minimal operation can be linearized next: one that no other remaining
        # operation must precede (nothing returned strictly before it was invoked).
        # Equivalently its invoke is at or before the earliest remaining return — which
        # also makes mutually-concurrent operations (shared timestamps) all candidates.
        earliest_return = min(ordered[i].returned_at for i in remaining)

        for i in remaining:
            candidate = ordered[i]
            if candidate.invoked_at <= earliest_return:
                next_state, result = spec.apply(state, candidate.op, candidate.args)
                if result == candidate.result and search(remaining - {i}, next_state):
                    return True

        failed.add(memo_key)
        return False

    return search(frozenset(range(len(ordered))), spec.initial())


def is_linearizable(ops: Sequence[_Op], spec: SequentialSpec) -> bool:
    """True iff every per-key sub-history of *ops* is linearizable under *spec*."""

    by_key: dict[Any, list[_Op]] = defaultdict(list)
    for operation in ops:
        by_key[operation.key].append(operation)

    return all(_linearizable_one(group, spec) for group in by_key.values())


def _ops_from(history: History, op_kind: str) -> list[_Op]:
    # Only events emitted by ``record_operation`` carry the linearizability fields (``key`` /
    # ``args`` / ``result`` / interval). The default ``op_kind="operation"`` collides with the
    # operation-boundary events the harness projects for every op, which have no ``key`` — skip
    # those so ``linearizable()`` over a normal ``Simulation`` history checks the recorded
    # register operations instead of raising ``KeyError``.
    return [
        _Op(
            key=event.fields["key"],
            op=event.fields["op"],
            args=tuple(event.fields.get("args", ())),
            result=event.fields.get("result"),
            invoked_at=event.fields["invoked_at"],
            returned_at=event.fields["returned_at"],
            session=event.fields.get("session"),
        )
        for event in history.of_kind(op_kind)
        if "key" in event.fields and "invoked_at" in event.fields
    ]


def linearizable(spec: SequentialSpec, *, op_kind: str = "operation") -> Invariant:
    """An invariant: every key's recorded operation history must be linearizable.

    Reads the operations recorded by :func:`record_operation` (events of *op_kind*),
    partitions them by key, and flags each key whose sub-history admits no valid
    linearization.
    """

    def _check(history: History) -> list[Violation]:
        by_key: dict[Any, list[_Op]] = defaultdict(list)
        for operation in _ops_from(history, op_kind):
            by_key[operation.key].append(operation)

        return [
            Violation(
                invariant="linearizable",
                message=f"operations on key {key!r} are not linearizable",
            )
            for key, group in by_key.items()
            if not _linearizable_one(group, spec)
        ]

    return _check


# ....................... #
# Weaker consistency models — they layer on the SAME recorded history (register ops with a
# session), differing only in the order they must respect: linearizable obeys real time;
# sequential obeys per-session program order; monotonic-reads is a per-session guarantee.


def _sequential_one(ops: Sequence[_Op], spec: SequentialSpec) -> bool:
    """Per-key sequential consistency: a total order respecting each session's program order
    (NOT real time) that reproduces every recorded read.

    The same recursive/memoized search as linearizability, but a candidate may be ordered next
    unless some *other* remaining op from the **same session** was issued before it — program
    order replaces the real-time constraint. With no sessions recorded the constraint is empty,
    so this asks only whether *some* sequential execution reproduces the reads.
    """

    ordered = tuple(sorted(ops, key=lambda o: (o.invoked_at, o.returned_at)))
    failed: set[tuple[frozenset[int], Any]] = set()

    def precedes(earlier: _Op, later: _Op) -> bool:
        return (
            earlier.session is not None
            and earlier.session == later.session
            and earlier.invoked_at < later.invoked_at
        )

    def search(remaining: frozenset[int], state: Any) -> bool:
        if not remaining:
            return True

        memo_key = (remaining, state)
        if memo_key in failed:
            return False

        for i in remaining:
            candidate = ordered[i]
            # i can be next only if nothing else remaining must precede it (program order).
            if any(precedes(ordered[j], candidate) for j in remaining if j != i):
                continue

            next_state, result = spec.apply(state, candidate.op, candidate.args)
            if result == candidate.result and search(remaining - {i}, next_state):
                return True

        failed.add(memo_key)
        return False

    return search(frozenset(range(len(ordered))), spec.initial())


def sequential(spec: SequentialSpec, *, op_kind: str = "operation") -> Invariant:
    """An invariant: every key's recorded history must be **sequentially consistent**.

    Weaker than :func:`linearizable` — it drops the real-time constraint and keeps only per-session
    program order, so a register that reorders across sessions (a stale but program-order-respecting
    read) passes here yet fails ``linearizable``. Pass ``session=`` to :func:`record_operation` for
    this to bite; without sessions it only checks that *some* sequential execution explains the reads.
    """

    def _check(history: History) -> list[Violation]:
        by_key: dict[Any, list[_Op]] = defaultdict(list)
        for operation in _ops_from(history, op_kind):
            by_key[operation.key].append(operation)

        return [
            Violation(
                invariant="sequential",
                message=f"operations on key {key!r} are not sequentially consistent",
            )
            for key, group in by_key.items()
            if not _sequential_one(group, spec)
        ]

    return _check


def _unique_write(writes: dict[Any, list[_Op]], value: Any) -> _Op | None:
    """The single write that produced *value*, or ``None`` when ambiguous (zero or many writes)."""

    matched = writes.get(value)
    return matched[0] if matched is not None and len(matched) == 1 else None


def _reads_regress(reads: Sequence[_Op], writes: dict[Any, list[_Op]]) -> bool:
    """Whether *reads* (one session/key, in program order) ever go backward in version.

    Sound + incomplete: only a *definitive* regression counts — the later read's write returned
    before the earlier read's write was invoked. Ambiguous (concurrent / non-unique) pairs skip.
    """

    for earlier_idx, earlier in enumerate(reads):
        seen_earlier = _unique_write(writes, earlier.result)
        if seen_earlier is None:
            continue

        for later in reads[earlier_idx + 1 :]:
            seen_later = _unique_write(writes, later.result)
            if seen_later is None or seen_later is seen_earlier:
                continue

            if seen_later.returned_at < seen_earlier.invoked_at:
                return True

    return False


def monotonic_reads(*, op_kind: str = "operation") -> Invariant:
    """An invariant: within a session, successive reads of a key never go **backward** in version.

    A session that reads a fresh value and then an older one has seen time run backwards — a classic
    replica-consistency bug (a read served by a lagging replica). This is **sound and incomplete**:
    it flags a pair only when the version order is unambiguous — the write the later read saw
    *definitively* preceded (returned before) the write the earlier read saw — so it never cries
    wolf on concurrent writes. Assumes uniquely-valued writes per key (the standard version-recovery
    assumption); a value written more than once is skipped. Needs ``session=`` on the recorded ops.
    """

    def _check(history: History) -> list[Violation]:
        ops = _ops_from(history, op_kind)

        writes: dict[Any, dict[Any, list[_Op]]] = defaultdict(lambda: defaultdict(list))
        reads: dict[tuple[Any, Any], list[_Op]] = defaultdict(list)

        for operation in ops:
            if operation.op == "write" and operation.args:
                writes[operation.key][operation.args[0]].append(operation)
            elif operation.op == "read" and operation.session is not None:
                reads[(operation.session, operation.key)].append(operation)

        violations: list[Violation] = []

        for (session, key), session_reads in reads.items():
            session_reads.sort(key=lambda r: r.invoked_at)
            by_value = writes.get(key, {})

            if _reads_regress(session_reads, by_value):
                violations.append(
                    Violation(
                        invariant="monotonic_reads",
                        message=f"session {session!r} read key {key!r} backward — a later read "
                        "observed an older write than an earlier read",
                    )
                )

        return violations

    return _check

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
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Protocol, Sequence, final, runtime_checkable

import attrs

from forze.base.primitives import monotonic
from forze_dst.invariants import Invariant, Violation
from forze_dst.recorder import History, record_event

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
) -> AsyncIterator[_Call]:
    """Record one operation's real-time interval + result for linearizability checking.

    Usage::

        async with record_operation("reg", "read") as call:
            call.result = await register.read()

    The invoke time is captured on enter and the return time on (normal) exit; an
    operation whose body raises is not recorded.
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

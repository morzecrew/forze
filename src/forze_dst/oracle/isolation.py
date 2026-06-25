"""Transactional-isolation oracles — snapshot-isolation and serializability over read/write sets.

The register-history checkers ([linearizability.py](linearizability.py)) reason about a *single*
object. The transactional anomalies — lost update, write skew — are defined over a transaction's
**read set** and **write set**, so they need a per-transaction view the id-only register history
does not give.

This module is the **sound kernel** of that check: pure functions over explicit
:class:`TxRecord`s. Two transactions are **concurrent** when their sequence spans overlap; a
transaction **committed** when its outcome is success. Over the concurrent, committed transactions:

- :func:`find_snapshot_isolation_violations` flags a **write-write** conflict (two concurrent
  committers wrote a common key) — the lost update snapshot isolation forbids;
- :func:`find_serializable_violations` additionally flags a **write-skew** anti-dependency (each
  read a key the other wrote) — so its violations are a superset (``serializable ⊋ snapshot``).

Both are **sound** (a flagged set genuinely violates the level) and **incomplete**: they catch the
canonical two-transaction key-level anomalies, not predicate phantoms (which need scan tracking) or
anti-dependency cycles spanning three or more transactions (which need version order) — mirroring
the sound-incomplete posture of :func:`~forze_dst.oracle.linearizability.monotonic_reads`.

**Feeding the kernel.** A correct per-transaction read/write set requires attributing each port call
to the *transaction* that issued it — which operation spans cannot give, since concurrent operations
interleave at every ``await`` and a span-based attribution misattributes an outer call that lands
inside an inner span. So the trace stamps a run-global ``tx_id`` on every event (RFC 0004 C.1):
:func:`transactions_from_history` groups by it (sound, exact), and :func:`snapshot_isolation` /
:func:`serializable` wrap the kernel as ``History``-reading :data:`Invariant`s for a sweep. A
transaction **committed** iff its scope emitted a ``tx`` ``exit`` event (a clean commit; a rollback
raises and emits none); its concurrency window is the span of trace sequences carrying its ``tx_id``.
"""

from __future__ import annotations

from typing import Any, Sequence, final

import attrs

from forze_dst.oracle.invariants import Invariant, Violation
from forze_dst.oracle.recorder import History

# ----------------------- #


@final
@attrs.define(frozen=True, kw_only=True)
class TxRecord:
    """One transaction's read/write sets + commit outcome — the kernel's unit of analysis.

    ``start``/``end`` are a sequence interval defining the transaction's concurrency window (two
    transactions are concurrent iff their intervals overlap); ``reads``/``writes`` the entity keys
    it read/wrote; ``committed`` whether it succeeded.
    """

    name: str
    start: int
    end: int
    committed: bool
    reads: frozenset[Any]
    writes: frozenset[Any]


# ....................... #


def _concurrent(a: TxRecord, b: TxRecord) -> bool:
    """Whether two transactions' concurrency intervals overlap (they interleaved)."""

    return a.start <= b.end and b.start <= a.end


def _committed_concurrent_pairs(
    txns: Sequence[TxRecord],
) -> list[tuple[TxRecord, TxRecord]]:
    committed = [tx for tx in txns if tx.committed]

    return [
        (committed[i], committed[j])
        for i in range(len(committed))
        for j in range(i + 1, len(committed))
        if _concurrent(committed[i], committed[j])
    ]


def _keys(keys: frozenset[Any]) -> str:
    return ", ".join(sorted(str(key) for key in keys))


# ....................... #


def find_snapshot_isolation_violations(txns: Sequence[TxRecord]) -> list[Violation]:
    """No two concurrent committed transactions wrote a common key (no lost update).

    Snapshot isolation aborts the second committer on a write-write conflict (first-committer-wins),
    so two concurrent transactions that both committed a write to the same key is a lost update SI
    forbids. Sound and incomplete: catches key-level write-write conflicts, not predicate phantoms.
    Disjoint-write anomalies (write skew) are *permitted* under SI and are not flagged here — see
    :func:`find_serializable_violations`.
    """

    return [
        Violation(
            invariant="snapshot_isolation",
            message=(
                f"concurrent committed transactions {a.name!r} and {b.name!r} both wrote "
                f"key(s) {{{_keys(a.writes & b.writes)}}} — a lost update snapshot isolation forbids"
            ),
        )
        for a, b in _committed_concurrent_pairs(txns)
        if a.writes & b.writes
    ]


def find_serializable_violations(txns: Sequence[TxRecord]) -> list[Violation]:
    """The committed transactions admit a serial order (no lost update, no write skew).

    Over each concurrent committed pair, flags either a **write-write** conflict (a lost update —
    also non-serializable) or a **write-skew** anti-dependency, where each transaction read a key
    the other wrote (the canonical snapshot-permits / serializable-forbids anomaly). Strictly
    stronger than :func:`find_snapshot_isolation_violations` (its violations are a superset). Sound
    and incomplete: catches the two-transaction anomalies, not predicate phantoms or anti-dependency
    cycles spanning three or more transactions.
    """

    violations: list[Violation] = []

    for a, b in _committed_concurrent_pairs(txns):
        if write_write := a.writes & b.writes:
            violations.append(
                Violation(
                    invariant="serializable",
                    message=(
                        f"concurrent committed transactions {a.name!r} and {b.name!r} both wrote "
                        f"key(s) {{{_keys(write_write)}}} — a lost update, not serializable"
                    ),
                )
            )
        elif (a.reads & b.writes) and (b.reads & a.writes):
            violations.append(
                Violation(
                    invariant="serializable",
                    message=(
                        f"concurrent committed transactions {a.name!r} and {b.name!r} form a "
                        f"write-skew anti-dependency: each read a key the other wrote "
                        f"({a.name!r} read {{{_keys(a.reads & b.writes)}}}, "
                        f"{b.name!r} read {{{_keys(b.reads & a.writes)}}}) — not serializable"
                    ),
                )
            )

    return violations


# ....................... #
# Feeding the kernel from a recorded DST history (sound, via the per-event tx_id seam).


@final
@attrs.define
class _TxBuilder:
    """Mutable accumulator for one transaction's trace facts, frozen into a :class:`TxRecord`."""

    start: int
    end: int
    committed: bool = False
    reads: set[Any] = attrs.field(factory=set)
    writes: set[Any] = attrs.field(factory=set)


def transactions_from_history(
    history: History,
    *,
    read_phase: str = "query",
    write_phase: str = "command",
) -> list[TxRecord]:
    """Derive per-transaction read/write sets from a recorded history, grouped by ``tx_id``.

    Sound because grouping is by the run-global transaction id the trace stamps on every event, not
    by operation span (which misattributes interleaved calls). A transaction's concurrency window is
    the span of trace sequences carrying its id; it ``committed`` iff a ``tx`` ``exit`` event was
    recorded under it (a clean commit). Keyed calls are classified by ``phase``. Events without a
    ``tx_id`` (outside any transaction, or an untraced/production run) are ignored.
    """

    builders: dict[int, _TxBuilder] = {}

    for event in history.of_kind("trace"):
        fields = event.fields
        tx_id = fields.get("tx_id")

        if tx_id is None:
            continue

        seq = int(fields.get("trace_seq", -1))
        builder = builders.get(tx_id)

        if builder is None:
            builders[tx_id] = builder = _TxBuilder(start=seq, end=seq)
        else:
            builder.start = min(builder.start, seq)
            builder.end = max(builder.end, seq)

        if fields.get("trace_domain") == "tx" and fields.get("op") == "exit":
            builder.committed = True

        key = fields.get("key")
        phase = fields.get("phase")

        if key is not None and phase == read_phase:
            builder.reads.add(key)
        elif key is not None and phase == write_phase:
            builder.writes.add(key)

    return [
        TxRecord(
            name=f"tx{tx_id}",
            start=builder.start,
            end=builder.end,
            committed=builder.committed,
            reads=frozenset(builder.reads),
            writes=frozenset(builder.writes),
        )
        for tx_id, builder in sorted(builders.items())
    ]


def snapshot_isolation(
    *, read_phase: str = "query", write_phase: str = "command"
) -> Invariant:
    """An :data:`Invariant`: the run's committed transactions are snapshot-isolated (no lost update).

    Derives per-transaction read/write sets from the trace (by ``tx_id``) and applies
    :func:`find_snapshot_isolation_violations`. Declarable in any sweep config.
    """

    def _check(history: History) -> list[Violation]:
        return find_snapshot_isolation_violations(
            transactions_from_history(
                history, read_phase=read_phase, write_phase=write_phase
            )
        )

    return _check


def serializable(
    *, read_phase: str = "query", write_phase: str = "command"
) -> Invariant:
    """An :data:`Invariant`: the run's committed transactions are serializable (no lost update / write skew).

    Derives per-transaction read/write sets from the trace (by ``tx_id``) and applies
    :func:`find_serializable_violations`. Declarable in any sweep config.
    """

    def _check(history: History) -> list[Violation]:
        return find_serializable_violations(
            transactions_from_history(
                history, read_phase=read_phase, write_phase=write_phase
            )
        )

    return _check

"""Weaker consistency models on the recorded register history — sequential + monotonic reads.

They layer on the same operations ``linearizable`` reads, differing only in the order they must
respect: ``linearizable`` obeys real time; ``sequential`` obeys per-session program order;
``monotonic_reads`` is a per-session guarantee. Driven on synthetic histories so each model's
exact discriminating case is pinned.
"""

from __future__ import annotations

from typing import Any

from forze_dst.invariants import RegisterSpec, check, linearizable, monotonic_reads, sequential
from forze_dst.oracle.recorder import Event, History

# ----------------------- #


def _op(
    seq: int,
    op: str,
    *,
    result: Any = None,
    args: tuple[Any, ...] = (),
    invoked: float,
    returned: float,
    session: Any = None,
    key: str = "r",
) -> Event:
    return Event(
        seq=seq,
        kind="operation",
        at=invoked,
        fields={
            "key": key,
            "op": op,
            "args": args,
            "result": result,
            "invoked_at": invoked,
            "returned_at": returned,
            "session": session,
        },
    )


# A history that is sequentially consistent but NOT linearizable: session B writes 2 (after A's
# write 1 returns) then reads 1. Real time forces write(2) before the read, so a linearization must
# show the read seeing 2 — it saw 1. But program order (B: write 2, then read) admits the order
# [write 2, write 1, read→1], which respects every session and reproduces the read.
_SEQ_NOT_LIN = History(
    seed=0,
    events=(
        _op(0, "write", args=(1,), invoked=0, returned=1, session="A"),
        _op(1, "write", args=(2,), invoked=2, returned=3, session="B"),
        _op(2, "read", result=1, invoked=4, returned=5, session="B"),
    ),
)


class TestSequential:
    def test_sequential_admits_what_linearizable_rejects(self) -> None:
        assert [v.invariant for v in check(_SEQ_NOT_LIN, [linearizable(RegisterSpec())])] == [
            "linearizable"
        ]
        assert check(_SEQ_NOT_LIN, [sequential(RegisterSpec())]) == []

    def test_sequential_rejects_a_program_order_violation(self) -> None:
        # One session writes 1 then 2 (program order), then reads 1 — no order respecting that
        # session's program order can have the final read see the superseded value.
        history = History(
            seed=0,
            events=(
                _op(0, "write", args=(1,), invoked=0, returned=1, session="S"),
                _op(1, "write", args=(2,), invoked=2, returned=3, session="S"),
                _op(2, "read", result=1, invoked=4, returned=5, session="S"),
            ),
        )
        assert [v.invariant for v in check(history, [sequential(RegisterSpec())])] == ["sequential"]


class TestMonotonicReads:
    def test_backward_read_is_flagged(self) -> None:
        # Session S reads 2 (newer write) then 1 (older write) — time ran backward for it.
        history = History(
            seed=0,
            events=(
                _op(0, "write", args=(1,), invoked=0, returned=1),
                _op(1, "write", args=(2,), invoked=2, returned=3),
                _op(2, "read", result=2, invoked=4, returned=5, session="S"),
                _op(3, "read", result=1, invoked=6, returned=7, session="S"),
            ),
        )
        violations = check(history, [monotonic_reads()])
        assert [v.invariant for v in violations] == ["monotonic_reads"]

    def test_forward_reads_hold(self) -> None:
        history = History(
            seed=0,
            events=(
                _op(0, "write", args=(1,), invoked=0, returned=1),
                _op(1, "write", args=(2,), invoked=2, returned=3),
                _op(2, "read", result=1, invoked=4, returned=5, session="S"),
                _op(3, "read", result=2, invoked=6, returned=7, session="S"),
            ),
        )
        assert check(history, [monotonic_reads()]) == []

    def test_other_sessions_do_not_interfere(self) -> None:
        # S1 reads 1 then 2 (forward); S2 reads 2 then 1 (backward). Only S2 is flagged.
        history = History(
            seed=0,
            events=(
                _op(0, "write", args=(1,), invoked=0, returned=1),
                _op(1, "write", args=(2,), invoked=2, returned=3),
                _op(2, "read", result=1, invoked=4, returned=5, session="S1"),
                _op(3, "read", result=2, invoked=5, returned=6, session="S1"),
                _op(4, "read", result=2, invoked=4, returned=5, session="S2"),
                _op(5, "read", result=1, invoked=6, returned=7, session="S2"),
            ),
        )
        violations = check(history, [monotonic_reads()])
        assert len(violations) == 1
        assert "'S2'" in violations[0].message

    def test_concurrent_writes_are_not_flagged(self) -> None:
        # The two writes overlap in time → no definitive version order → sound checker stays quiet.
        history = History(
            seed=0,
            events=(
                _op(0, "write", args=(1,), invoked=0, returned=3),
                _op(1, "write", args=(2,), invoked=1, returned=4),
                _op(2, "read", result=2, invoked=5, returned=6, session="S"),
                _op(3, "read", result=1, invoked=7, returned=8, session="S"),
            ),
        )
        assert check(history, [monotonic_reads()]) == []

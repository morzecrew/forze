"""DST P5b: the linearizability oracle — Wing-Gong checker, per-key, via the recorder.

Unit-checks the algorithm on constructed histories (a legal one, a real-time-order
violation, concurrent writes in either order, per-key partitioning), then exercises it
end-to-end: a correct register records a linearizable history, and a register with a
stale-read bug is found by `explore` and minimized to a minimal counterexample.
"""

from __future__ import annotations

import asyncio
from typing import Sequence

from forze_dst.invariants import RegisterSpec, check, linearizable, record_operation
from forze_dst.oracle import explore, run_recorded
from forze_dst.oracle.linearizability import _Op, is_linearizable
from forze_dst.oracle.recorder import Event, History

# ----------------------- #

_SPEC = RegisterSpec()


def test_ignores_harness_operation_events_without_linearizability_fields() -> None:
    # The harness projects ``operation`` events (the default ``op_kind``) that carry no ``key``;
    # the checker must skip them — not ``KeyError`` — and evaluate the recorded register ops.
    history = History(
        seed=0,
        events=(
            Event(seq=0, kind="operation", at=0.0, fields={"op": "pay", "outcome": "ok"}),
            Event(
                seq=1,
                kind="operation",
                at=1.0,
                fields={
                    "key": "r", "op": "write", "args": (1,), "result": None,
                    "invoked_at": 0.0, "returned_at": 1.0,
                },
            ),
            Event(
                seq=2,
                kind="operation",
                at=2.0,
                fields={
                    "key": "r", "op": "read", "args": (), "result": 0,  # stale → not linearizable
                    "invoked_at": 2.0, "returned_at": 3.0,
                },
            ),
        ),
    )

    violations = check(history, [linearizable(_SPEC)])  # must not raise KeyError
    assert [v.invariant for v in violations] == ["linearizable"]


def _op(op: str, args: tuple, result: object, invoked: float, returned: float) -> _Op:
    return _Op(
        key="r",
        op=op,
        args=args,
        result=result,
        invoked_at=invoked,
        returned_at=returned,
    )


class TestChecker:
    def test_sequential_consistent_history_is_linearizable(self) -> None:
        ops = [
            _op("write", (1,), None, 0.0, 1.0),
            _op("read", (), 1, 2.0, 3.0),
            _op("write", (2,), None, 4.0, 5.0),
            _op("read", (), 2, 6.0, 7.0),
        ]
        assert is_linearizable(ops, _SPEC)

    def test_stale_read_after_completed_write_is_not_linearizable(self) -> None:
        # write(2) completes (t=3) before the read starts (t=4); a read of 1 has no
        # valid linearization — it must see the last write.
        ops = [
            _op("write", (1,), None, 0.0, 1.0),
            _op("write", (2,), None, 2.0, 3.0),
            _op("read", (), 1, 4.0, 5.0),
        ]
        assert not is_linearizable(ops, _SPEC)

    def test_concurrent_writes_allow_either_order(self) -> None:
        # W(1) and W(2) overlap; a later read of *either* is linearizable.
        overlapping = [
            _op("write", (1,), None, 0.0, 2.0),
            _op("write", (2,), None, 1.0, 3.0),
        ]
        assert is_linearizable([*overlapping, _op("read", (), 1, 4.0, 5.0)], _SPEC)
        assert is_linearizable([*overlapping, _op("read", (), 2, 4.0, 5.0)], _SPEC)

    def test_memoization_prunes_repeated_failed_states(self) -> None:
        # Four concurrent writes then a read of a value never written. Different write
        # orderings reach the same (remaining, state), so the memo is hit and prunes.
        writes = [_op("write", (i,), None, 0.0, 10.0) for i in range(1, 5)]
        read = _op("read", (), 99, 11.0, 12.0)
        assert not is_linearizable([*writes, read], _SPEC)

    def test_register_spec_rejects_unknown_op(self) -> None:
        import pytest

        with pytest.raises(ValueError):
            _SPEC.apply(0, "increment", ())

    def test_partition_per_key(self) -> None:
        good = _Op(
            key="a", op="write", args=(1,), result=None, invoked_at=0.0, returned_at=1.0
        )
        good_read = _Op(
            key="a", op="read", args=(), result=1, invoked_at=2.0, returned_at=3.0
        )
        # key "b": stale read → that partition is not linearizable, failing the whole.
        bad_write = _Op(
            key="b", op="write", args=(9,), result=None, invoked_at=0.0, returned_at=1.0
        )
        bad_read = _Op(
            key="b", op="read", args=(), result=0, invoked_at=2.0, returned_at=3.0
        )
        assert is_linearizable([good, good_read], _SPEC)
        assert not is_linearizable([good, good_read, bad_write, bad_read], _SPEC)


class _Register:
    def __init__(self) -> None:
        self.value = 0

    async def write(self, v: int) -> None:
        self.value = v

    async def read(self) -> int:
        return self.value


class _StaleReadRegister(_Register):
    async def read(self) -> int:
        return 0  # BUG: ignores writes, always returns the initial value


def _build(register_factory):
    def build(ops: Sequence[tuple]):
        async def scenario() -> None:
            register = register_factory()
            for spec in ops:
                if spec[0] == "write":
                    async with record_operation("r", "write", (spec[1],)):
                        await register.write(spec[1])
                else:
                    async with record_operation("r", "read") as call:
                        call.result = await register.read()
                await asyncio.sleep(
                    1
                )  # separate intervals: a sequential real-time order

        return scenario

    return build


class TestLinearizabilityOracle:
    def test_correct_register_history_is_linearizable(self) -> None:
        ops = [("write", 1), ("read",), ("write", 2), ("read",)]
        history = run_recorded(_build(_Register)(ops), seed=0)
        assert check(history, [linearizable(_SPEC)]) == []

    def test_stale_read_bug_is_found_and_minimized(self) -> None:
        items = [("write", 1), ("read",), ("write", 2), ("read",)]
        report = explore(
            _build(_StaleReadRegister), items, [linearizable(_SPEC)], seeds=range(3)
        )
        assert report is not None
        assert report.violations[0].invariant == "linearizable"
        # Minimal counterexample: a non-zero write followed by a read that returns 0.
        assert len(report.workload) < len(items)
        assert len(report.workload) == 2

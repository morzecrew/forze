"""DST P5 exit gate: a violated invariant yields a reproducible seed + a minimized case.

The recorder captures domain facts under simulation; invariants assert over the history;
the oracle searches seeds for a violation and shrinks the workload to a minimal
counterexample that still fails and replays identically. A correct implementation
yields no violation across seeds (no false positives).
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

from forze_dst.invariants import check, expect, monotonic_per, no_duplicate_effect
from forze_dst.markers import record_event
from forze_dst.oracle import Event, History, explore, minimize, run_recorded

# ----------------------- #


def _build_lost_update(workers: Sequence[str]):
    async def scenario() -> None:
        counter = {"value": 0}

        async def increment() -> None:
            current = counter["value"]
            await asyncio.sleep(
                0
            )  # yield: a peer can interleave between read and write
            counter["value"] = current + 1

        semaphore = asyncio.Semaphore(max(1, len(workers)))

        async def one() -> None:
            async with semaphore:
                await increment()

        await asyncio.gather(*(one() for _ in workers))
        record_event("result", final=counter["value"], expected=len(workers))

    return scenario


def _build_atomic(workers: Sequence[str]):
    async def scenario() -> None:
        counter = {"value": 0}

        async def increment() -> None:
            counter["value"] += 1  # atomic on the single-threaded loop (no yield)
            await asyncio.sleep(0)

        semaphore = asyncio.Semaphore(max(1, len(workers)))

        async def one() -> None:
            async with semaphore:
                await increment()

        await asyncio.gather(*(one() for _ in workers))
        record_event("result", final=counter["value"], expected=len(workers))

    return scenario


_NO_LOST_UPDATE = expect(
    "result",
    lambda event: event.fields["final"] == event.fields["expected"],
    message="lost update: final != expected",
)


# ....................... #


class TestOracle:
    def test_violation_found_minimized_and_reproducible(self) -> None:
        items = [f"w{i}" for i in range(8)]
        report = explore(_build_lost_update, items, [_NO_LOST_UPDATE], seeds=range(10))

        assert report is not None
        assert report.violations[0].invariant == "expect"
        # Minimized: a lost update needs exactly two concurrent writers — no fewer.
        assert len(report.workload) < len(items)
        assert len(report.workload) == 2

        # Reproducible: re-running the minimal workload at the reported seed re-violates.
        replay = run_recorded(
            _build_lost_update(list(report.workload)),
            seed=report.seed,
            schedule_seed=report.schedule_seed,
        )
        assert check(replay, [_NO_LOST_UPDATE])

    def test_no_false_positive_for_correct_code(self) -> None:
        items = [f"w{i}" for i in range(8)]
        report = explore(_build_atomic, items, [_NO_LOST_UPDATE], seeds=range(20))
        assert report is None


class TestInvariants:
    def test_no_duplicate_effect_catches_a_duplicate(self) -> None:
        history = History(
            seed=0,
            events=(
                Event(seq=0, kind="effect", at=0.0, fields={"id": "a"}),
                Event(seq=1, kind="effect", at=1.0, fields={"id": "b"}),
                Event(seq=2, kind="effect", at=2.0, fields={"id": "a"}),
            ),
        )
        violations = check(history, [no_duplicate_effect("effect", by="id")])
        assert len(violations) == 1
        assert violations[0].invariant == "no_duplicate_effect"

    def test_no_duplicate_effect_passes_when_unique(self) -> None:
        history = History(
            seed=0,
            events=(Event(seq=0, kind="effect", at=0.0, fields={"id": "a"}),),
        )
        assert check(history, [no_duplicate_effect("effect", by="id")]) == []

    def test_monotonic_per_catches_a_regression(self) -> None:
        history = History(
            seed=0,
            events=(
                Event(seq=0, kind="hlc", at=0.0, fields={"actor": "a", "ts": 5}),
                Event(seq=1, kind="hlc", at=1.0, fields={"actor": "a", "ts": 3}),
            ),
        )
        violations = check(history, [monotonic_per("hlc", "ts", actor="actor")])
        assert len(violations) == 1


class TestRecorderAndMinimize:
    def test_recorder_stamps_virtual_time(self) -> None:
        async def scenario() -> None:
            record_event("tick", n=1)
            await asyncio.sleep(5)
            record_event("tick", n=2)

        history = run_recorded(scenario, seed=0)
        ticks = history.of_kind("tick")
        assert [event.fields["n"] for event in ticks] == [1, 2]
        assert ticks[0].at == 0.0
        assert ticks[1].at == 5.0  # advanced by virtual time

    def test_record_event_is_a_noop_without_a_recorder(self) -> None:
        record_event("orphan", value=1)  # no recorder bound — must not raise

    def test_current_recorder_reflects_binding(self) -> None:
        from forze_dst.oracle import Recorder, bind_recorder, current_recorder

        assert current_recorder() is None
        recorder = Recorder(seed=0)
        with bind_recorder(recorder):
            assert current_recorder() is recorder
        assert current_recorder() is None

    def test_minimize_finds_a_minimal_failing_subset(self) -> None:
        def fails(items: Sequence[str]) -> bool:
            return "x" in items and "y" in items

        result = minimize(["a", "x", "b", "y", "c"], fails)
        assert set(result) == {"x", "y"}
        assert fails(result)

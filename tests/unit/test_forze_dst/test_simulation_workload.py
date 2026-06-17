"""DST P3 exit gate: seeded workloads + scheduler perturbation surface concurrency bugs.

A non-atomic read-modify-write loses updates under concurrent interleaving. The
perturbing loop reaches orderings FIFO never would, the invariant violation is found,
and because every run is seeded it reproduces exactly. The atomic version holds across
all seeds — no false positives.
"""

from __future__ import annotations

import asyncio

import pytest

from forze_dst import (
    OpSpec,
    generate_workload,
    run_simulation,
    run_workload,
    simulate_workload,
)

# ----------------------- #

_N = 25
_CONCURRENCY = 5


class _Counter:
    def __init__(self) -> None:
        self.value = 0

    async def incr_racy(self) -> None:
        # Read, yield (lets a peer interleave), then write — the classic lost update.
        current = self.value
        await asyncio.sleep(0)
        self.value = current + 1

    async def incr_atomic(self) -> None:
        # No await between read and write → atomic on the single-threaded loop.
        self.value += 1
        await asyncio.sleep(0)


async def _drive(counter: _Counter, method: str, n: int, concurrency: int) -> int:
    semaphore = asyncio.Semaphore(concurrency)

    async def one() -> None:
        async with semaphore:
            await getattr(counter, method)()

    await asyncio.gather(*(one() for _ in range(n)))
    return counter.value


def _run(method: str, *, schedule_seed: int | None) -> int:
    counter = _Counter()
    return run_simulation(
        lambda: _drive(counter, method, _N, _CONCURRENCY),
        seed=0,
        schedule_seed=schedule_seed,
    )


async def _record_order(n: int) -> tuple[int, ...]:
    # Each task yields once, then records itself — the recorded order is exactly the
    # order the loop resumed the continuations, i.e. what perturbation controls.
    log: list[int] = []

    async def one(tag: int) -> None:
        await asyncio.sleep(0)
        log.append(tag)

    await asyncio.gather(*(one(i) for i in range(n)))
    return tuple(log)


def _run_order(*, schedule_seed: int | None) -> tuple[int, ...]:
    return run_simulation(
        lambda: _record_order(8), seed=0, schedule_seed=schedule_seed
    )


# ....................... #


class TestConcurrencyBugDetection:
    def test_lost_update_is_found_and_reproduces(self) -> None:
        final = _run("incr_racy", schedule_seed=1)
        assert final < _N  # updates were lost under concurrency — the bug
        assert _run("incr_racy", schedule_seed=1) == final  # same seed → same result

    def test_perturbation_explores_interleavings(self) -> None:
        # Plain FIFO resumes continuations in insertion order; perturbation reaches
        # other orderings. Each run is deterministic, so this set is fixed (not flaky).
        fifo = _run_order(schedule_seed=None)
        assert fifo == tuple(range(8))  # unperturbed: insertion order
        orders = {_run_order(schedule_seed=s) for s in range(16)}
        assert len(orders) > 1  # perturbation reaches multiple interleavings
        assert any(order != fifo for order in orders)
        # ...and a fixed schedule seed reproduces its ordering exactly.
        assert _run_order(schedule_seed=3) == _run_order(schedule_seed=3)

    def test_atomic_is_correct_across_all_seeds(self) -> None:
        # No false positives: the correct implementation holds under every interleaving.
        for s in range(16):
            assert _run("incr_atomic", schedule_seed=s) == _N
        assert _run("incr_atomic", schedule_seed=None) == _N  # and under plain FIFO


class TestWorkloadGenerator:
    @staticmethod
    def _catalog(counter: _Counter) -> list[OpSpec]:
        return [
            OpSpec(name="incr", make=counter.incr_atomic, weight=3.0),
            OpSpec(name="noop", make=lambda: asyncio.sleep(0), weight=1.0),
        ]

    def test_generation_is_deterministic(self) -> None:
        catalog = self._catalog(_Counter())
        a = [op.name for op in generate_workload(catalog, seed=5, count=40)]
        b = [op.name for op in generate_workload(catalog, seed=5, count=40)]
        c = [op.name for op in generate_workload(catalog, seed=6, count=40)]
        assert a == b
        assert a != c  # a different seed draws a different workload
        assert a.count("incr") > a.count("noop")  # weight 3:1 reflected

    def test_simulate_workload_runs_the_catalog(self) -> None:
        counter = _Counter()
        catalog = self._catalog(counter)
        results = simulate_workload(catalog, seed=2, count=30, concurrency=4)
        assert len(results) == 30
        # atomic ops are safe under perturbation: value == number of "incr" draws.
        expected_incr = sum(
            1 for op in generate_workload(catalog, seed=2, count=30) if op.name == "incr"
        )
        assert counter.value == expected_incr

    def test_invalid_inputs_are_rejected(self) -> None:
        op = OpSpec(name="x", make=lambda: asyncio.sleep(0))
        with pytest.raises(ValueError):
            generate_workload([], seed=0, count=5)
        with pytest.raises(ValueError):
            generate_workload([op], seed=0, count=-1)
        with pytest.raises(ValueError):
            run_simulation(lambda: run_workload([op], concurrency=0), seed=0)

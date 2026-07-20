"""DST in CI: a seed corpus run every build, plus a fuzz-marked extended sweep.

The corpus asserts the DST machinery stays honest on a fixed band of seeds each CI run:
correct components never trip an invariant (no false positives, determinism intact), and
a known bug stays caught (the oracle doesn't go blind). ``just fuzz`` runs the same
properties over a much wider band — intended for a nightly job. Add any seed that once
found a bug to ``_REGRESSION_SEEDS`` so it is re-checked forever.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import pytest

from forze_dst.invariants import RegisterSpec, expect, linearizable, record_operation
from forze_dst.oracle import explore
from forze_dst.oracle.recorder import record_event

# ----------------------- #

_REGRESSION_SEEDS: tuple[int, ...] = ()  # seeds that once found a bug — checked forever
_CORPUS = tuple(range(32)) + _REGRESSION_SEEDS
_EXTENDED = tuple(range(500))

_NO_LOST_UPDATE = expect(
    "result",
    lambda event: event.fields["final"] == event.fields["expected"],
    message="lost update",
)
_LINEARIZABLE = linearizable(RegisterSpec())


def _atomic_counter(workers: Sequence[int]):
    async def scenario() -> None:
        counter = {"value": 0}

        async def increment() -> None:
            counter["value"] += 1  # atomic: no await between read and write
            await asyncio.sleep(0)

        await asyncio.gather(*(increment() for _ in workers))
        record_event("result", final=counter["value"], expected=len(workers))

    return scenario


def _lossy_counter(workers: Sequence[int]):
    async def scenario() -> None:
        counter = {"value": 0}

        async def increment() -> None:
            current = counter["value"]
            await asyncio.sleep(0)  # yield: a peer interleaves between read and write
            counter["value"] = current + 1

        await asyncio.gather(*(increment() for _ in workers))
        record_event("result", final=counter["value"], expected=len(workers))

    return scenario


def _linearizable_register(ops: Sequence[tuple]):
    async def scenario() -> None:
        register = {"value": 0}
        for spec in ops:
            if spec[0] == "write":
                async with record_operation("r", "write", (spec[1],)):
                    register["value"] = spec[1]
            else:
                async with record_operation("r", "read") as call:
                    call.result = register["value"]
            await asyncio.sleep(1)

    return scenario


# (build, items, invariants) for components that must NEVER trip an invariant.
_CORRECT: list[tuple] = [
    (_atomic_counter, list(range(6)), [_NO_LOST_UPDATE]),
    (
        _linearizable_register,
        [("write", 1), ("read",), ("write", 2), ("read",)],
        [_LINEARIZABLE],
    ),
]


def _assert_corpus_clean(seeds: Sequence[int]) -> None:
    for build, items, invariants in _CORRECT:
        report = explore(build, items, invariants, seeds=seeds)
        assert report is None, f"DST regression — correct component violated: {report}"


# ....................... #


class TestDstCorpus:
    def test_correct_components_pass_the_corpus(self) -> None:
        _assert_corpus_clean(_CORPUS)

    def test_known_bug_stays_caught(self) -> None:
        # Guards the oracle's sensitivity: the lost-update bug must still be found.
        report = explore(
            _lossy_counter, list(range(6)), [_NO_LOST_UPDATE], seeds=_CORPUS
        )
        assert report is not None

    @pytest.mark.fuzz
    def test_extended_fuzz(self) -> None:
        _assert_corpus_clean(_EXTENDED)

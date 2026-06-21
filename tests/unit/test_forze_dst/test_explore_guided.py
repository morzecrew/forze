"""Coverage-guided MUTATION engine (E4) — feedback-directed input generation.

The engine is substrate-free: it is handed a ``run`` that maps a :class:`Genome` to a
:class:`History`, so these tests drive it with a synthetic AFL-style oracle — coverage equals the
length of the genome's matched prefix against a hidden target sequence. A behavior gated behind a
long prefix is exponentially unlikely for an independent uniform sweep, but the mutation loop keeps
each prefix-extending input in its corpus and builds on it, so it climbs. The tests pin: every
mutation operator, full reproducibility from the master seed, that guided beats uniform at equal
budget, and that a planted violation is caught.
"""

from __future__ import annotations

import asyncio
import random

import attrs
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze_dst import OperationCase, Simulation, SimulationConfig
from forze_dst.markers import record_event
from forze_dst.explore_guided import Genome, coverage_guided_search, mutate
from forze_dst.invariants import expect
from forze_dst.oracle.coverage import behavioral_coverage
from forze_dst.oracle.recorder import Event, History
from forze_mock import MockDepsModule

# ----------------------- #

_TARGET = (3, 1, 4, 1, 5, 2)  # the hidden "interesting" op sequence
_CATALOG = 6  # op indices 0.._CATALOG-1
_MAX_OPS = len(_TARGET)  # fixed-length workload for the climb demo (no position dilution)


def _matched_prefix(ops: tuple[int, ...]) -> int:
    """How many leading ops equal the target — the depth this input reached."""

    depth = 0
    for index, token in enumerate(_TARGET):
        if index < len(ops) and ops[index] == token:
            depth += 1
        else:
            break
    return depth


def _synthetic_run(genome: Genome) -> History:
    """A run whose behavioral coverage is one ``depth-k`` behavior per matched prefix level."""

    depth = _matched_prefix(genome.ops)
    events = (Event(seq=0, kind="operation", at=0.0, fields={"op": "root", "outcome": "ok"}),)
    events += tuple(
        Event(seq=k, kind="operation", at=float(k), fields={"op": f"depth-{k}", "outcome": "ok"})
        for k in range(1, depth + 1)
    )
    return History(seed=genome.seed, events=events)


def _uniform_coverage(master_seed: int, *, budget: int) -> int:
    """Behaviors a uniform sweep covers at *budget* — independent random genomes, no feedback."""

    rng = random.Random(master_seed)
    behaviors: set[object] = set()
    for _ in range(budget):
        ops = tuple(rng.randrange(_CATALOG) for _ in range(len(_TARGET)))
        behaviors |= behavioral_coverage(_synthetic_run(Genome(ops=ops, seed=rng.getrandbits(32))))
    return len(behaviors)


def _guided(master_seed: int, *, budget: int, is_violation=lambda _h: False):  # type: ignore[no-untyped-def]
    seed_genome = Genome(ops=(0,) * len(_TARGET), seed=master_seed)
    return coverage_guided_search(
        seed_genome=seed_genome,
        run=_synthetic_run,
        is_violation=is_violation,
        on_violation=lambda genome: None,
        master_seed=master_seed,
        budget=budget,
        catalog_size=_CATALOG,
        max_ops=_MAX_OPS,
    )


# ....................... #


class TestMutate:
    def test_each_operator_is_reachable_and_bounded(self) -> None:
        seen: set[str] = set()
        rng = random.Random(1)
        genome = Genome(ops=(0, 1, 2), seed=99)

        for _ in range(400):
            child = mutate(genome, rng, catalog_size=_CATALOG, max_ops=4)
            assert 1 <= len(child.ops) <= 4
            assert all(0 <= op < _CATALOG for op in child.ops)

            if len(child.ops) > len(genome.ops):
                seen.add("insert")
            elif len(child.ops) < len(genome.ops):
                seen.add("delete")
            elif child.seed != genome.seed:
                seen.add("reseed")
            elif child.ops != genome.ops:
                seen.add("point")

        assert seen == {"insert", "delete", "reseed", "point"}

    def test_reseed_holds_the_op_sequence(self) -> None:
        rng = random.Random(0)
        genome = Genome(ops=(5,), seed=1)  # len 1 → no delete; force toward reseed/point/insert
        # A reseed keeps ops; a point keeps length. Over many draws ops stay valid indices.
        for _ in range(50):
            child = mutate(genome, rng, catalog_size=_CATALOG, max_ops=12)
            assert all(0 <= op < _CATALOG for op in child.ops)


class TestReproducible:
    def test_same_master_seed_is_identical(self) -> None:
        a = _guided(2024, budget=120)
        b = _guided(2024, budget=120)
        assert a.behaviors == b.behaviors
        assert a.new_by_run == b.new_by_run
        assert a.corpus_size == b.corpus_size

    def test_different_seed_explores_differently(self) -> None:
        a = _guided(1, budget=120)
        b = _guided(2, budget=120)
        # Same machinery, different lineage — the run order of discoveries differs.
        assert a.new_by_run != b.new_by_run


class TestBeatsUniform:
    def test_guided_reaches_deeper_than_uniform_at_equal_budget(self) -> None:
        budget = 2000
        guided = _guided(7, budget=budget)
        uniform = _uniform_coverage(7, budget=budget)

        # Each depth level is one behavior; the ceiling is len(_TARGET) + 1 (incl. the root).
        # The guided loop climbs nearly the whole ladder by reusing each prefix it discovers;
        # uniform, needing the whole prefix at once, stalls exponentially short.
        assert guided.size > uniform, (guided.size, uniform)
        assert guided.size >= len(_TARGET)  # within one rung of the top — far past uniform
        assert guided.runs == budget
        assert guided.corpus_size >= len(_TARGET) - 1  # ~one corpus entry per level unlocked


class TestFindsViolation:
    def test_planted_violation_is_caught_and_reported(self) -> None:
        # The bug fires once the input reaches depth >= 4 — a deep state uniform rarely hits but
        # the guided loop climbs to. on_violation returns a sentinel report so we can detect it.
        sentinel = object()

        def is_violation(history: History) -> bool:
            return ("op", "depth-4", "ok") in behavioral_coverage(history)

        result = coverage_guided_search(
            seed_genome=Genome(ops=(0,) * len(_TARGET), seed=11),
            run=_synthetic_run,
            is_violation=is_violation,
            on_violation=lambda genome: sentinel,  # type: ignore[arg-type,return-value]
            master_seed=11,
            budget=600,
            catalog_size=_CATALOG,
            max_ops=_MAX_OPS,
        )

        assert result.violation is sentinel  # the deep state was reached and reported
        assert result.runs <= 600


# ....................... #
# Integration: the real Simulation.coverage_guided over an op catalog.


class DepositDTO(BaseModel):
    amount: int


@attrs.define(slots=True, kw_only=True)
class _Deposit(Handler[DepositDTO, None]):
    """A non-atomic deposit — concurrent calls race on read-modify-write (lost update)."""

    ledger: dict[str, int]

    async def __call__(self, args: DepositDTO) -> None:
        self.ledger["expected"] += args.amount
        current = self.ledger["balance"]
        await asyncio.sleep(0)  # yield → concurrent deposits race here
        self.ledger["balance"] = current + args.amount


def _racy_sim() -> Simulation:
    ledger = {"balance": 0, "expected": 0}
    registry = OperationRegistry(
        handlers={"deposit": lambda _c: _Deposit(ledger=ledger)},
        descriptors={
            "deposit": OperationDescriptor(
                input_type=DepositDTO, output_type=None, description="x"
            )
        },
    ).freeze()

    async def reset(_ctx: ExecutionContext) -> None:
        ledger["balance"] = ledger["expected"] = 0

    async def observe(_ctx: ExecutionContext) -> None:
        record_event("balance", final=ledger["balance"], expected=ledger["expected"])

    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(),
        setup=reset,
        observe=observe,
        invariants=[
            expect("balance", lambda e: e.fields["final"] == e.fields["expected"],
                   message="lost deposit")
        ],
    )


_CASES = [OperationCase(op="deposit", inputs=lambda _rng: DepositDTO(amount=1))]


class TestCoverageGuidedIntegration:
    def _config(self) -> SimulationConfig:
        return SimulationConfig(
            seeds=range(1), count=4, concurrency=4, guided_budget=64
        )

    def test_finds_the_lost_update_and_minimizes(self) -> None:
        stats = _racy_sim().coverage_guided(self._config(), cases=_CASES)

        assert stats.violation is not None
        assert stats.violation.violations[0].message == "lost deposit"
        # Minimized to the few concurrent deposits that still lose an update, with a real
        # reproducible history + fingerprint — the whole genome→run→minimize wiring end to end.
        assert 0 < len(stats.violation.workload) <= 4
        assert stats.violation.history.events
        assert stats.violation.registry_fingerprint is not None

    def test_reproducible_from_the_master_seed(self) -> None:
        a = _racy_sim().coverage_guided(self._config(), cases=_CASES)
        b = _racy_sim().coverage_guided(self._config(), cases=_CASES)

        assert a.violation is not None and b.violation is not None
        assert a.violation.seed == b.violation.seed
        assert a.runs == b.runs
        assert a.behaviors == b.behaviors

    def test_requires_cases(self) -> None:
        import pytest

        with pytest.raises(ValueError):
            _racy_sim().coverage_guided(self._config(), cases=[])

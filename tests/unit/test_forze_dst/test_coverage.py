"""Coverage-guided exploration (S7) — a behavioral coverage signal + a self-right-sizing sweep.

:func:`behavioral_coverage` distills a run into the distinct, PII-free behaviors it exercised;
:meth:`Simulation.coverage` sweeps seeds while coverage grows and stops once it saturates,
reporting how much behavior was covered and which seeds mattered (and surfacing a violation if
the sweep hits one).
"""

from __future__ import annotations

import asyncio

import attrs
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, PCTScheduler, Rule, Scenario, Simulation, SimulationConfig, Strategy
from forze_dst.markers import record_event
from forze_dst.invariants import expect, operation_succeeds
from forze_dst.oracle import behavioral_coverage
from forze_dst.oracle.coverage import CoverageStats, behavioral_fingerprint
from forze_dst.oracle.confidence import ConfidenceReport
from forze_dst.oracle.reachability import ReachabilityReport
from forze_dst.oracle.recorder import Event, History
from forze_mock import MockDepsModule

from types import MappingProxyType

# ----------------------- #


def _ev(seq: int, kind: str, **fields: object) -> Event:
    return Event(seq=seq, kind=kind, at=float(seq), fields=fields)


class TestBehavioralCoverage:
    def test_distills_operations_edges_and_faults(self) -> None:
        history = History(
            seed=0,
            events=(
                _ev(0, "op_start", call_id=0, op="pay"),  # structural — ignored
                _ev(1, "operation", op="pay", outcome="ok"),
                _ev(2, "trace", trace_domain="document", surface="document_command",
                    route="orders", op="create", phase="command", outcome=None),
                _ev(3, "fault", fault="error", surface="document_command", op="update"),
            ),
        )
        cov = behavioral_coverage(history)

        assert ("op", "pay", "ok") in cov
        assert ("edge", "document", "document_command", "orders", "create", "command", None) in cov
        assert ("env", "fault", "error", "document_command", "update") in cov
        assert len(cov) == 3  # op_start is not a behavior

    def test_is_id_independent(self) -> None:
        # The same shapes of behavior with different entity keys → identical coverage.
        def history(key: str) -> History:
            return History(
                seed=0,
                events=(
                    _ev(0, "operation", op="pay", outcome="ok"),
                    _ev(1, "trace", trace_domain="document", surface="document_command",
                        route="orders", op="update", phase="command", key=key, outcome=None),
                ),
            )

        assert behavioral_coverage(history("a")) == behavioral_coverage(history("b"))


# ....................... #
# A clean sim that exercises one fixed behavior — coverage saturates fast.


class Thing(Document):
    pass


class ThingCreate(CreateDocumentCmd):
    pass


class ThingRead(ReadDocument):
    pass


THING_SPEC = DocumentSpec(
    name="things",
    read=ThingRead,
    write=DocumentWriteTypes(domain=Thing, create_cmd=ThingCreate),
)


@attrs.define(slots=True, kw_only=True)
class _Make(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        await self.ctx.document.command(THING_SPEC).create(ThingCreate())


def _clean_sim() -> Simulation:
    registry = OperationRegistry(
        handlers={"make": lambda ctx: _Make(ctx=ctx)},
        descriptors={
            "make": OperationDescriptor(input_type=None, output_type=None, description="x")
        },
    ).freeze()
    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(),
        invariants=[operation_succeeds("make")],
    )


_MAKE_SCENARIO = Scenario(state=ModelState, act=(Rule(op="make"),))


# A racy ledger (lost update under concurrency) — coverage finds the violating seed.


class DepositDTO(BaseModel):
    amount: int


@attrs.define(slots=True, kw_only=True)
class _Deposit(Handler[DepositDTO, None]):
    ledger: dict[str, int]

    async def __call__(self, args: DepositDTO) -> None:
        self.ledger["expected"] += args.amount
        current = self.ledger["balance"]
        await asyncio.sleep(0)  # yield: concurrent deposits race here
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


# ....................... #


class TestCoverageGuidedSweep:
    def test_honors_pct_scheduler(self, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        # coverage() with a PCTScheduler scheduler must build a PCT scheduler, not silently shuffle.
        from forze_dst import scheduler

        calls: list[object] = []
        real = scheduler.pct_reorderer_factory

        def spy(**kwargs: object) -> object:
            calls.append(kwargs)
            return real(**kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(scheduler, "pct_reorderer_factory", spy)

        _clean_sim().coverage(
            SimulationConfig(
                strategy=Strategy.SCENARIO,
                scheduler=PCTScheduler(),
                seeds=range(2),
                act_count=2,
                concurrency=2,
                coverage_plateau=0,
            ),
            scenario=_MAKE_SCENARIO,
        )

        assert calls, "coverage() with a PCTScheduler scheduler built no PCT scheduler"

    def test_saturates_and_stops_early(self) -> None:
        stats = _clean_sim().coverage(
            SimulationConfig(
                strategy=Strategy.SCENARIO,
                seeds=range(100),
                act_count=2,
                concurrency=1,
                coverage_plateau=3,
            ),
            scenario=_MAKE_SCENARIO,
        )
        # One fixed behavior shape → coverage saturates and the sweep stops far short of 100.
        assert stats.size > 0
        assert stats.plateaued
        assert stats.seeds_run < 100
        assert stats.violation is None
        # The first seed did all the covering; later seeds added nothing.
        assert stats.productive_seeds == (0,)

    def test_full_sweep_when_plateau_disabled(self) -> None:
        stats = _clean_sim().coverage(
            SimulationConfig(
                strategy=Strategy.SCENARIO,
                seeds=range(5),
                act_count=1,
                concurrency=1,
                coverage_plateau=0,  # disabled → run the whole pool
            ),
            scenario=_MAKE_SCENARIO,
        )
        assert stats.seeds_run == 5
        assert not stats.plateaued

    def test_surfaces_a_violation_and_stops(self) -> None:
        stats = _racy_sim().coverage(
            SimulationConfig(
                strategy=Strategy.SCENARIO, seeds=range(20), act_count=6, concurrency=6
            ),
            scenario=Scenario(
                state=ModelState,
                act=(Rule(op="deposit", arg=lambda _s, _rng: DepositDTO(amount=1)),),
            ),
        )

        assert stats.violation is not None
        assert "lost deposit" in stats.violation.format()
        # The report renders the coverage summary too.
        assert "coverage report" in stats.format()


# ....................... #


class TestBehavioralFingerprint:
    """The ordered, PII-free signature of a run's path (vs. the unordered coverage set)."""

    def test_folds_operations_and_edges_excluding_other_kinds(self) -> None:
        history = History(
            seed=0,
            events=(
                _ev(0, "op_start", call_id=0, op="pay"),  # structural — not in the shape
                _ev(1, "operation", op="pay", outcome="ok"),
                _ev(2, "trace", trace_domain="document", surface="document_command",
                    route="orders", op="create", phase="command", outcome=None),
                _ev(3, "fault", fault="error", surface="document_command", op="update"),
            ),
        )

        fp = behavioral_fingerprint(history)

        # A stable 16-byte blake2b digest → 32 hex chars.
        assert isinstance(fp, str)
        assert len(fp) == 32
        # Deterministic: same input → same digest.
        assert fp == behavioral_fingerprint(history)

    def test_is_id_independent_like_coverage(self) -> None:
        # Different entity keys, same op/edge shape → identical fingerprint.
        def history(key: str) -> History:
            return History(
                seed=0,
                events=(
                    _ev(0, "operation", op="pay", outcome="ok"),
                    _ev(1, "trace", trace_domain="document", surface="document_command",
                        route="orders", op="update", phase="command", key=key, outcome=None),
                ),
            )

        assert behavioral_fingerprint(history("a")) == behavioral_fingerprint(history("b"))

    def test_order_sensitive_unlike_coverage(self) -> None:
        # The same two outcomes in a different order are the same *set* but a different *shape*.
        def history(*outcomes: str) -> History:
            return History(
                seed=0,
                events=tuple(
                    _ev(i, "operation", op="pay", outcome=o) for i, o in enumerate(outcomes)
                ),
            )

        forward = history("ok", "error")
        backward = history("error", "ok")

        # Coverage (a set) is order-insensitive…
        assert behavioral_coverage(forward) == behavioral_coverage(backward)
        # …the fingerprint (an ordered shape) is not.
        assert behavioral_fingerprint(forward) != behavioral_fingerprint(backward)

    def test_outcome_change_changes_fingerprint(self) -> None:
        ok = History(seed=0, events=(_ev(0, "operation", op="pay", outcome="ok"),))
        err = History(seed=0, events=(_ev(0, "operation", op="pay", outcome="error"),))

        assert behavioral_fingerprint(ok) != behavioral_fingerprint(err)

    def test_empty_history_has_a_stable_fingerprint(self) -> None:
        empty = behavioral_fingerprint(History(seed=0, events=()))

        assert len(empty) == 32
        assert empty == behavioral_fingerprint(History(seed=1, events=()))


# ....................... #


def _stats(**overrides: object) -> CoverageStats:
    base: dict[str, object] = {
        "behaviors": frozenset({("op", "make", "ok"), ("op", "make", "error")}),
        "seeds_run": 3,
        "new_by_seed": ((0, 2), (1, 0), (2, 1)),
        "plateaued": False,
    }
    base.update(overrides)
    return CoverageStats(**base)  # type: ignore[arg-type]


class TestCoverageStatsValueObject:
    """The frozen VO directly — size, productive_seeds, and the format() branches."""

    def test_size_counts_distinct_behaviors(self) -> None:
        assert _stats().size == 2
        assert _stats(behaviors=frozenset()).size == 0

    def test_productive_seeds_are_those_that_added_behavior(self) -> None:
        # seed 0 added 2, seed 1 added 0, seed 2 added 1 → only 0 and 2 are productive.
        assert _stats().productive_seeds == (0, 2)

    def test_no_productive_seeds_when_nothing_added(self) -> None:
        assert _stats(new_by_seed=((0, 0), (1, 0))).productive_seeds == ()

    def test_format_minimal_report(self) -> None:
        out = _stats().format()

        assert "DST coverage report" in out
        assert "behaviors covered: 2" in out
        assert "seeds run:         3" in out
        assert "productive seeds:  [0, 2]" in out
        # No reachability / no confidence gaps / no violation → none of those lines.
        assert "(saturated)" not in out
        assert "reachability:" not in out
        assert "confidence gaps" not in out
        assert "✗ violation" not in out

    def test_format_marks_saturation(self) -> None:
        assert "(saturated)" in _stats(plateaued=True).format()

    def test_format_reachability_all_reached(self) -> None:
        reach = ReachabilityReport(
            targets=frozenset({"breaker_open", "partition_isolated"}),
            hits=MappingProxyType({"breaker_open": 2, "partition_isolated": 1}),
            runs=3,
        )

        out = _stats(reachability=reach).format()

        assert "reachability:      2/2 targets" in out
        # Every target reached → no "never reached" suffix.
        assert "never reached" not in out

    def test_format_reachability_with_unreached(self) -> None:
        reach = ReachabilityReport(
            targets=frozenset({"breaker_open", "never_hit"}),
            hits=MappingProxyType({"breaker_open": 2}),
            runs=3,
        )

        out = _stats(reachability=reach).format()

        assert "reachability:      1/2 targets" in out
        assert "never reached: ['never_hit']" in out

    def test_format_renders_confidence_gaps(self) -> None:
        conf = ConfidenceReport(
            seeds_run=3, ran_ops=("make",), raced_ops=()
        )

        out = _stats(confidence=conf).format()

        assert "⚠ confidence gaps" in out
        assert "never raced" in out

    def test_format_omits_clean_confidence(self) -> None:
        conf = ConfidenceReport(seeds_run=3, ran_ops=("make",), raced_ops=("make",))

        out = _stats(confidence=conf).format()

        # A clean confidence report has no warnings → no gap section in the coverage report.
        assert "confidence gaps" not in out

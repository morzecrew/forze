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
from forze_dst import (
    ModelState,
    Rule,
    Scenario,
    Simulation,
    SimulationConfig,
    Strategy,
    behavioral_coverage,
    expect,
    operation_succeeds,
    record_event,
)
from forze_dst.recorder import Event, History
from forze_mock import MockDepsModule

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

"""Confidence — a clean sweep reports what it actually exercised (never-raced ops, unfired faults).

Driven through ``Simulation.audit`` so the real ``CausalGraph`` overlap analysis and injected-fault
timeline are exercised end to end, not a hand-built history.
"""

from __future__ import annotations

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, Rule, Scenario, Simulation, SimulationConfig
from forze_dst.faults import FaultPolicy, FaultRule
from forze_dst.invariants import operation_succeeds
from forze_mock import MockDepsModule

# ----------------------- #


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


def _sim() -> Simulation:
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


_SCENARIO = Scenario(state=ModelState, act=(Rule(op="make"),))


class DepositDTO(BaseModel):
    amount: int


# ....................... #


class TestRaced:
    def test_single_op_run_serially_never_races(self) -> None:
        stats = _sim().audit(
            SimulationConfig(seeds=range(3), act_count=3, concurrency=1),
            scenario=_SCENARIO,
        )
        conf = stats.confidence
        assert conf is not None
        assert "make" in conf.ran_ops
        # Concurrency 1 → operations never overlap → "make" is a confidence gap.
        assert "make" in conf.never_raced
        assert not conf.clean
        assert any("never raced" in w for w in conf.warnings)

    def test_concurrent_ops_are_marked_raced(self) -> None:
        stats = _sim().audit(
            SimulationConfig(seeds=range(3), act_count=4, concurrency=4),
            scenario=_SCENARIO,
        )
        conf = stats.confidence
        assert conf is not None
        assert "make" in conf.raced_ops
        assert conf.never_raced == ()


class TestFaultsFired:
    def test_declared_fault_on_unexercised_surface_never_fires(self) -> None:
        cfg = SimulationConfig(
            seeds=range(4),
            act_count=2,
            concurrency=2,
            faults=FaultPolicy(rules=(FaultRule(surface="does_not_exist", error=1.0),)),
        )
        conf = _sim().audit(cfg, scenario=_SCENARIO).confidence
        assert conf is not None
        assert len(conf.faults_declared) == 1
        # The app never calls that surface, so the rule can't fire.
        assert conf.faults_never_fired == conf.faults_declared
        assert any("never fired" in w for w in conf.warnings)

    def test_declared_fault_on_exercised_surface_fires(self) -> None:
        # The app calls document_command; a 100% error there fires on the first seed.
        cfg = SimulationConfig(
            seeds=range(4),
            act_count=2,
            concurrency=2,
            faults=FaultPolicy(rules=(FaultRule(surface="document_command", error=1.0),)),
        )
        conf = _sim().audit(cfg, scenario=_SCENARIO).confidence
        assert conf is not None
        assert conf.faults_fired  # it fired
        assert conf.faults_never_fired == ()


class TestAuditSemantics:
    def test_audit_runs_full_sweep_ignoring_plateau(self) -> None:
        # plateau in the passed config is overridden; every seed runs.
        stats = _sim().audit(
            SimulationConfig(seeds=range(7), act_count=2, concurrency=2, coverage_plateau=2),
            scenario=_SCENARIO,
        )
        assert stats.seeds_run == 7

    def test_coverage_also_populates_confidence(self) -> None:
        # Not just audit — a plain coverage sweep carries confidence too.
        stats = _sim().coverage(
            SimulationConfig(seeds=range(3), act_count=2, concurrency=1, coverage_plateau=0),
            scenario=_SCENARIO,
        )
        assert stats.confidence is not None
        assert "make" in stats.confidence.never_raced

    def test_confidence_warnings_render_in_coverage_report(self) -> None:
        stats = _sim().audit(
            SimulationConfig(seeds=range(2), act_count=2, concurrency=1),
            scenario=_SCENARIO,
        )
        out = stats.format()
        assert "confidence gaps" in out
        assert "make" in out

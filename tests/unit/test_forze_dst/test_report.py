"""Observed causal graph + counterexample report.

Reconstructs the causal structure (operation spans + the trace steps they caused + which
spans raced) from a recorded history, and renders a `ViolationReport` as a readable
counterexample. Exercised against the lost-deposit race — the report must surface the
concurrency that caused it and the violated invariant.
"""

from __future__ import annotations

import asyncio

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument

from forze_dst import OperationCase, Simulation, SimulationConfig, Strategy
from forze_dst.markers import record_event
from forze_dst.invariants import Violation, expect
from forze_dst.oracle import CausalGraph, format_report
from forze_dst.oracle import ViolationReport
from forze_dst.oracle.recorder import Event, History
from forze_mock import MockDepsModule

# ----------------------- #


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


def _lost_deposit_simulation() -> Simulation:
    ledger = {"balance": 0, "expected": 0}

    registry = OperationRegistry(
        handlers={"deposit": lambda _c: _Deposit(ledger=ledger)},
        descriptors={
            "deposit": OperationDescriptor(
                input_type=DepositDTO, output_type=None, description="Deposit."
            )
        },
    ).freeze()

    async def reset(_ctx: object) -> None:
        ledger["balance"] = ledger["expected"] = 0

    async def observe(_ctx: object) -> None:
        record_event("balance", final=ledger["balance"], expected=ledger["expected"])

    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(),
        setup=reset,
        observe=observe,
        invariants=[
            expect(
                "balance",
                lambda e: e.fields["final"] == e.fields["expected"],
                message="lost deposit: balance != expected",
            )
        ],
    )


# ....................... #


class TestCausalGraph:
    def test_reconstructs_spans_and_detects_the_race(self) -> None:
        report = _lost_deposit_simulation().run(
            SimulationConfig(
                strategy=Strategy.OP_CASE, count=6, concurrency=6, seeds=range(5)
            ),
            cases=[OperationCase(op="deposit")],
        )
        assert report is not None

        graph = CausalGraph.from_history(report.history)
        # One span per minimized call, all the same op.
        assert {span.op for span in graph.spans} == {"deposit"}
        assert len(graph.spans) == len(report.workload)
        # The concurrent deposits are detected as a race (sequence-interval overlap),
        # even though they share a virtual-time stamp.
        groups = graph.concurrent_groups()
        assert groups, "expected the racing deposits to be grouped"
        assert sum(len(g) for g in groups) == len(graph.spans)
        # The observe fact is preserved as a non-structural recorded fact.
        assert any(fact.kind == "balance" for fact in graph.facts)

    def test_pure_in_memory_op_has_no_side_effect_steps(self) -> None:
        # The ledger touches no ports, so spans carry no side-effect steps — the
        # operation boundaries are represented by the spans themselves, not re-listed.
        report = _lost_deposit_simulation().run(
            SimulationConfig(
                strategy=Strategy.OP_CASE, count=6, concurrency=6, seeds=range(5)
            ),
            cases=[OperationCase(op="deposit")],
        )
        assert report is not None
        graph = CausalGraph.from_history(report.history)
        assert all(not span.steps for span in graph.spans)


class _Thing(Document):
    label: str = "x"


class _ThingCreate(CreateDocumentCmd):
    label: str = "x"


class _ThingRead(ReadDocument):
    label: str


_THING_SPEC = DocumentSpec(
    name="things",
    read=_ThingRead,
    write=DocumentWriteTypes(domain=_Thing, create_cmd=_ThingCreate),
)


@attrs.define(slots=True, kw_only=True)
class _Store(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        await self.ctx.document.command(_THING_SPEC).create(_ThingCreate())


class TestSideEffectSteps:
    def test_port_calls_appear_as_causal_children(self) -> None:
        registry = OperationRegistry(
            handlers={"store": lambda ctx: _Store(ctx=ctx)}
        ).freeze()

        sim = Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            # Force a report so we can inspect the causal trace of a port-touching op.
            invariants=[expect("operation", lambda _e: False, message="forced")],
        )
        report = sim.run(
            SimulationConfig(
                strategy=Strategy.OP_CASE, count=3, concurrency=1, seeds=range(1)
            ),
            cases=[OperationCase(op="store")],
        )
        assert report is not None

        graph = CausalGraph.from_history(report.history)
        steps = [step for span in graph.spans for step in span.steps]
        # The document write is captured as a side effect (a non-operation trace step).
        assert any("document" in step.label for step in steps)
        assert all(step.domain != "operation" for step in steps)
        # Steps within a span stay ordered by the engine's own execution sequence.
        for span in graph.spans:
            seqs = [step.seq for step in span.steps]
            assert seqs == sorted(seqs)
        # And it renders under the span in the report.
        assert "document" in report.format()


class TestFormatReport:
    def test_render_is_readable_and_complete(self) -> None:
        report = _lost_deposit_simulation().run(
            SimulationConfig(
                strategy=Strategy.OP_CASE, count=6, concurrency=6, seeds=range(5)
            ),
            cases=[OperationCase(op="deposit")],
        )
        assert report is not None

        rendered = report.format()
        assert rendered == report.format()  # deterministic

        assert "DST counterexample" in rendered
        assert f"seed={report.seed}" in rendered
        assert "registry=" in rendered  # fingerprint stamped
        assert "workload" in rendered and "deposit" in rendered
        assert "concurrency" in rendered  # the race is surfaced
        assert "lost deposit: balance != expected" in rendered
        assert "balance" in rendered  # the observe fact

    def test_render_handles_errors_scalars_and_bare_header(self) -> None:
        # A hand-built history exercises the edge paths: an errored span, a scalar
        # (non-tuple) workload item, a long arg that truncates, no schedule_seed/no
        # fingerprint, and no concurrency.
        history = History(
            seed=7,
            events=(
                Event(
                    seq=0, kind="op_start", at=0.0, fields={"call_id": 0, "op": "boom"}
                ),
                Event(
                    seq=1,
                    kind="operation",
                    at=0.0,
                    fields={
                        "call_id": 0,
                        "op": "boom",
                        "outcome": "error",
                        "error": "RuntimeError",
                        "invoked_at": 0.0,
                        "returned_at": 0.0,
                    },
                ),
            ),
        )
        report = ViolationReport(
            seed=7,
            schedule_seed=None,
            violations=(Violation(invariant="expect", message="boom", events=()),),
            workload=(("boom", "x" * 200), "scalar"),
            history=history,
        )
        rendered = format_report(report)

        assert "✗ boom#0 → error [error=RuntimeError]" in rendered
        assert "[1] scalar()" in rendered  # scalar workload item rendered with no args
        assert "…" in rendered  # the long arg truncated
        assert "schedule_seed" not in rendered  # None → omitted
        assert "registry=" not in rendered  # no fingerprint → omitted
        assert "concurrency" not in rendered  # a single span: no race

    def test_render_with_no_violations_says_none(self) -> None:
        report = ViolationReport(
            seed=1,
            schedule_seed=1,
            violations=(),
            workload=(),
            history=History(seed=1, events=()),
        )
        assert "(none)" in format_report(report)

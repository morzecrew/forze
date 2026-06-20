"""Turnkey harness: feed an operation registry + deps, the harness finds the bug.

Demonstrates the app-author flow — declare operations (with input types), hand the harness
a deps factory (one MockDepsModule auto-mocks every port), and explore. The harness
generates inputs from each operation's ``input_type``, drives them through ``run_operation``
on the virtual-time loop, records each automatically, and on a violation returns a
reproducible, minimized, fingerprint-stamped counterexample. A correct implementation
yields nothing across seeds.
"""

from __future__ import annotations

import asyncio

import attrs
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry

from forze_dst import OperationCase, Simulation, SimulationConfig, Strategy
from forze_dst.markers import record_event
from forze_dst.invariants import Violation, expect
from forze_dst.oracle import History
from forze_mock import MockDepsModule

# ----------------------- #


class DepositDTO(BaseModel):
    amount: int


@attrs.define(slots=True, kw_only=True)
class _Deposit(Handler[DepositDTO, None]):
    ledger: dict[str, int]
    atomic: bool

    async def __call__(self, args: DepositDTO) -> None:
        self.ledger["expected"] += args.amount  # the true running total (atomic)
        if self.atomic:
            self.ledger["balance"] += args.amount
        else:
            current = self.ledger["balance"]
            await asyncio.sleep(0)  # yield: concurrent deposits race here
            self.ledger["balance"] = current + args.amount


def _make_simulation(*, atomic: bool) -> Simulation:
    ledger = {"balance": 0, "expected": 0}

    registry = OperationRegistry(
        handlers={"deposit": lambda _ctx: _Deposit(ledger=ledger, atomic=atomic)},
        descriptors={
            "deposit": OperationDescriptor(
                input_type=DepositDTO,
                output_type=None,
                description="Deposit into the ledger.",
            )
        },
    ).freeze()

    async def reset(_ctx: object) -> None:
        ledger["balance"] = ledger["expected"] = 0

    async def observe(_ctx: object) -> None:
        record_event("balance", final=ledger["balance"], expected=ledger["expected"])

    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(),  # one module auto-mocks every port
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


class TestTurnkeyHarness:
    def test_finds_lost_deposit_minimized_and_stamped(self) -> None:
        sim = _make_simulation(atomic=False)
        report = sim.run(
            SimulationConfig(
                strategy=Strategy.OP_CASE, count=6, concurrency=6, seeds=range(5)
            ),
            cases=[OperationCase(op="deposit")],
        )

        assert report is not None
        assert report.violations[0].invariant == "expect"
        assert report.registry_fingerprint  # tied to the operation contract
        # Inputs were auto-generated from DepositDTO, and the case minimized to two.
        ops, args = zip(*report.workload)
        assert set(ops) == {"deposit"}
        assert all(isinstance(a, DepositDTO) for a in args)
        assert 2 <= len(report.workload) < 6

    def test_atomic_implementation_has_no_violation(self) -> None:
        sim = _make_simulation(atomic=True)
        report = sim.run(
            SimulationConfig(
                strategy=Strategy.OP_CASE, count=6, concurrency=6, seeds=range(20)
            ),
            cases=[OperationCase(op="deposit")],
        )
        assert report is None

    def test_fingerprint_tracks_the_operation_contract(self) -> None:
        same = _make_simulation(atomic=True).fingerprint()
        assert _make_simulation(atomic=False).fingerprint() == same  # same contract

        class WideDepositDTO(BaseModel):
            amount: int
            note: str

        changed = OperationRegistry(
            handlers={
                "deposit": lambda _ctx: _Deposit(
                    ledger={"balance": 0, "expected": 0}, atomic=True
                )
            },
            descriptors={
                "deposit": OperationDescriptor(
                    input_type=WideDepositDTO, output_type=None, description="Deposit."
                )
            },
        ).freeze()
        other = Simulation(
            operations=changed, deps=lambda: MockDepsModule()
        ).fingerprint()
        assert other != same  # a changed input contract → a different fingerprint


class TestHarnessMechanics:
    def test_explicit_input_override_and_input_less_op(self) -> None:
        seen: list[int] = []

        @attrs.define(slots=True)
        class _Record(Handler[DepositDTO, None]):
            async def __call__(self, args: DepositDTO) -> None:
                seen.append(args.amount)

        @attrs.define(slots=True)
        class _Noop(Handler[None, None]):
            async def __call__(self, _args: None) -> None:
                return None

        registry = OperationRegistry(
            handlers={"rec": lambda _c: _Record(), "noop": lambda _c: _Noop()},
            descriptors={  # "noop" has no descriptor → its input is None
                "rec": OperationDescriptor(
                    input_type=DepositDTO, output_type=None, description="record"
                )
            },
        ).freeze()

        sim = Simulation(
            operations=registry, deps=lambda: MockDepsModule()
        )  # no invariants
        report = sim.run(
            SimulationConfig(
                strategy=Strategy.OP_CASE, count=8, concurrency=4, seeds=range(2)
            ),
            cases=[
                OperationCase(op="rec", inputs=lambda _rng: DepositDTO(amount=7)),
                OperationCase(op="noop"),
            ],
        )
        assert report is None  # no invariants declared → nothing to violate
        assert 7 in seen  # the explicit input factory was used (not auto-generated)

    def test_engine_runtime_trace_is_folded_into_history(self) -> None:
        # The harness enables runtime tracing and folds the engine's trace into the
        # recorded history, so invariants see port/op/dispatch events with their own
        # virtual-time stamps — not just what handler code records explicitly.
        captured: list[History] = []

        @attrs.define(slots=True)
        class _Noop(Handler[None, None]):
            async def __call__(self, _args: None) -> None:
                return None

        registry = OperationRegistry(handlers={"noop": lambda _c: _Noop()}).freeze()

        def capture(history: History) -> list[Violation]:
            captured.append(history)
            return []

        sim = Simulation(
            operations=registry, deps=lambda: MockDepsModule(), invariants=[capture]
        )
        report = sim.run(
            SimulationConfig(
                strategy=Strategy.OP_CASE, count=2, concurrency=2, seeds=range(1)
            ),
            cases=[OperationCase(op="noop")],
        )

        assert report is None
        history = captured[0]
        traces = history.of_kind("trace")
        # The op boundary (B3) is emitted as domain="operation" with invoke/complete phases.
        op_traces = [e for e in traces if e.fields.get("trace_domain") == "operation"]
        phases = {e.fields.get("phase") for e in op_traces}
        assert {"invoke", "complete"} <= phases
        # Folded events carry the engine's own stamp, monotonically non-decreasing.
        ats = [e.at for e in traces]
        assert ats == sorted(ats)

    def test_operation_error_is_recorded_and_assertable(self) -> None:
        @attrs.define(slots=True)
        class _Boom(Handler[None, None]):
            async def __call__(self, _args: None) -> None:
                raise RuntimeError("boom")

        registry = OperationRegistry(handlers={"boom": lambda _c: _Boom()}).freeze()
        no_errors = expect(
            "operation",
            lambda e: e.fields.get("outcome") != "error",
            message="an operation errored",
        )
        sim = Simulation(
            operations=registry, deps=lambda: MockDepsModule(), invariants=[no_errors]
        )
        report = sim.run(
            SimulationConfig(
                strategy=Strategy.OP_CASE, count=3, concurrency=1, seeds=range(1)
            ),
            cases=[OperationCase(op="boom")],
        )
        assert report is not None
        assert report.violations[0].message == "an operation errored"

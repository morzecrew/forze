"""Resource-leak invariants — an opened resource that never closes by end of run.

The detection logic is unit-tested on synthetic histories (precise), and a real transactional
sweep confirms a balanced run does not false-positive.
"""

from __future__ import annotations

import attrs

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, Rule, Scenario, Simulation, SimulationConfig
from forze_dst.invariants import check, no_resource_leak, no_unclosed_transaction
from forze_dst.oracle.recorder import Event, History
from forze_mock import MockDepsModule

# ----------------------- #


def _trace(seq: int, *, domain: str, op: str, **fields: object) -> Event:
    return Event(
        seq=seq, kind="trace", at=float(seq), fields={"trace_domain": domain, "op": op, **fields}
    )


class TestDetectionLogic:
    def test_balanced_transactions_hold(self) -> None:
        history = History(
            seed=0,
            events=(
                _trace(0, domain="tx", op="enter", route="mock"),
                _trace(1, domain="tx", op="exit", route="mock"),
            ),
        )
        assert check(history, [no_unclosed_transaction()]) == []

    def test_unclosed_transaction_is_flagged(self) -> None:
        history = History(
            seed=0,
            events=(
                _trace(0, domain="tx", op="enter", route="mock"),
                _trace(1, domain="tx", op="enter", route="mock"),
                _trace(2, domain="tx", op="exit", route="mock"),
            ),
        )
        violations = check(history, [no_unclosed_transaction()])
        assert [v.invariant for v in violations] == ["no_resource_leak"]
        assert "route='mock'" in violations[0].message

    def test_routes_are_paired_independently(self) -> None:
        history = History(
            seed=0,
            events=(
                _trace(0, domain="tx", op="enter", route="a"),
                _trace(1, domain="tx", op="exit", route="a"),
                _trace(2, domain="tx", op="enter", route="b"),  # b leaks
            ),
        )
        violations = check(history, [no_unclosed_transaction()])
        assert len(violations) == 1
        assert "route='b'" in violations[0].message

    def test_general_primitive_pairs_custom_open_close_by_key(self) -> None:
        # A custom resource port that traces open/close ops keyed by entity.
        leak = no_resource_leak(
            surface="lease_command", open_op="take", close_op="give_back", by="key"
        )
        history = History(
            seed=0,
            events=(
                _trace(0, domain="lease", op="take", surface="lease_command", key="L1"),
                _trace(1, domain="lease", op="take", surface="lease_command", key="L2"),
                _trace(2, domain="lease", op="give_back", surface="lease_command", key="L1"),
            ),
        )
        violations = check(history, [leak])
        assert len(violations) == 1
        assert "key='L2'" in violations[0].message

    def test_close_without_open_is_not_a_leak(self) -> None:
        # Only net-open counts; a stray close (already balanced or pre-existing) does not flag.
        leak = no_resource_leak(domain="tx", open_op="enter", close_op="exit", by="route")
        history = History(seed=0, events=(_trace(0, domain="tx", op="exit", route="mock"),))
        assert check(history, [leak]) == []


# ....................... #
# A real transactional sim — a balanced run must not false-positive.


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


_TX = OperationPlan().bind_tx().set_route("mock").finish(deep=False)


def _tx_sim() -> Simulation:
    registry = OperationRegistry(
        handlers={"make": lambda ctx: _Make(ctx=ctx)},
        plans={"make": _TX},
        descriptors={
            "make": OperationDescriptor(input_type=None, output_type=None, description="x")
        },
    ).freeze()
    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(),
        invariants=[no_unclosed_transaction()],
    )


_MAKE_SCENARIO = Scenario(state=ModelState, act=(Rule(op="make"),))


class TestRealRun:
    def test_balanced_transactional_sweep_has_no_leak(self) -> None:
        report = _tx_sim().run(
            SimulationConfig(seeds=range(16), act_count=4, concurrency=4),
            scenario=_MAKE_SCENARIO,
        )
        assert report is None  # every entered tx exited — no false positive

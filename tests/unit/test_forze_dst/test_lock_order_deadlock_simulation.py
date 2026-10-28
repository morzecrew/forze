"""A sweep finds a lock-order deadlock in a handler workload, and clears a correct lock order.

Two handlers each lock and bump accounts A and B in one transaction. Taking them in one order is
correct: under ``MockDepsModule(row_locks="block")`` the second waits for the first and both
succeed. Taking them in opposite orders deadlocks on a real database, and the sweep reports it as
a ``no_lock_order_deadlock`` violation naming both rows, with no invariant declared for it.

The claim-mode contrast is what makes the green run mean something: the same correct workload
there aborts under contention, so a sweep cannot tell it from the deadlock-prone one.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Final, Literal
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.deps import DepsModule
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst import OperationCase, Simulation, SimulationConfig, Strategy
from forze_dst import invariants as inv
from forze_mock import MockDepsModule

# ----------------------- #

A: Final = UUID(int=1)
B: Final = UUID(int=2)


class _Account(Document):
    value: int


class _AccountRead(ReadDocument):
    value: int


class _AccountCreate(CreateDocumentCmd):
    value: int


class _AccountUpdate(BaseDTO):
    value: int | None = None


SPEC = DocumentSpec[_AccountRead, _Account, _AccountCreate, _AccountUpdate](
    name="accounts",
    read=_AccountRead,
    write=DocumentWriteTypes(domain=_Account, create_cmd=_AccountCreate, update_cmd=_AccountUpdate),
)


class Bump(BaseModel):
    pass


@attrs.define(slots=True, kw_only=True)
class _BumpBoth(Handler[Bump, None]):
    ctx: ExecutionContext
    order: tuple[UUID, UUID]

    async def __call__(self, args: Bump) -> None:
        for pk in self.order:
            row = await self.ctx.doc.query(SPEC).get(pk, for_update=True)
            await self.ctx.doc.command(SPEC).update(
                row.id, row.rev, _AccountUpdate(value=row.value + 1)
            )


async def _setup(ctx: ExecutionContext) -> None:
    for pk in (A, B):
        await ctx.doc.command(SPEC).create(_AccountCreate(value=0), id=pk)


def _sweep(*, opposite: bool, row_locks: Literal["claim", "block"] = "block") -> Any:
    def deps() -> Sequence[DepsModule]:
        return [MockDepsModule(row_locks=row_locks)]

    plan = OperationPlan().bind_tx().set_route("mock").finish(deep=False)
    orders = {"ab": (A, B), "ba": (B, A) if opposite else (A, B)}
    registry = OperationRegistry(
        handlers={
            op: (lambda ctx, order=order: _BumpBoth(ctx=ctx, order=order))
            for op, order in orders.items()
        },
        plans=dict.fromkeys(orders, plan),
        descriptors={
            op: OperationDescriptor(input_type=Bump, output_type=None, description="bump A and B")
            for op in orders
        },
    ).freeze()

    simulation = Simulation(
        operations=registry,
        deps=deps,
        setup=_setup,
        invariants=[inv.operation_succeeds()],
    )

    return simulation.run(
        SimulationConfig(
            strategy=Strategy.OP_CASE,
            count=4,
            act_count=4,
            concurrency=4,
            seeds=range(4),
        ),
        cases=[OperationCase(op=op, inputs=lambda rng: Bump()) for op in orders],
    )


# ----------------------- #


class TestLockOrder:
    def test_one_order_waits_and_every_bump_succeeds(self) -> None:
        assert _sweep(opposite=False) is None

    def test_opposite_orders_are_reported_as_a_deadlock(self) -> None:
        report = _sweep(opposite=True)

        assert report is not None
        deadlocks = [v for v in report.violations if v.invariant == "no_lock_order_deadlock"]

        assert deadlocks, f"expected a lock-order deadlock, got {report.violations}"
        assert f"accounts/{A}" in deadlocks[0].message
        assert f"accounts/{B}" in deadlocks[0].message

    def test_the_report_reproduces_from_its_seed(self) -> None:
        first, second = _sweep(opposite=True), _sweep(opposite=True)

        assert first.seed == second.seed
        assert [v.message for v in first.violations] == [v.message for v in second.violations]

    def test_claim_mode_aborts_even_the_correct_order(self) -> None:
        # Why the option exists: without blocking, correct code under contention fails too.
        report = _sweep(opposite=False, row_locks="claim")

        assert report is not None
        assert {v.invariant for v in report.violations} == {"operation_succeeds"}

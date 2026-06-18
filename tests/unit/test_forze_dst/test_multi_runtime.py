"""Multi-instantiable runtime (WS6) — N real ExecutionRuntimes over one shared store.

WS4 showed a *restart* (a fresh context over a persisted `MockState`); this shows several
full `ExecutionRuntime`s (each with its own lifecycle scope) running **concurrently** over one
shared `MockState` under the simulation loop — the foundation for the multi-runtime
("distributed") DST the unified config will orchestrate in the next plan. The runtimes share
only the simulated substrate, so cross-runtime contention goes through the same transaction /
optimistic-concurrency semantics as in-process concurrency.
"""

from __future__ import annotations

import asyncio
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.execution.runtime import ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.domain.models import (
    BaseDTO,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)
from forze_dst import run_simulation
from forze_mock import MockDepsModule
from forze_mock.state import MockState

# ----------------------- #


class Slot(Document):
    owner: int | None = None


class SlotCreate(CreateDocumentCmd):
    owner: int | None = None


class SlotUpdate(BaseDTO):
    owner: int | None = None


class SlotRead(ReadDocument):
    owner: int | None


SLOT_SPEC = DocumentSpec(
    name="slots",
    read=SlotRead,
    write=DocumentWriteTypes(domain=Slot, create_cmd=SlotCreate, update_cmd=SlotUpdate),
)


class ClaimCmd(BaseModel):
    slot: UUID
    who: int


@attrs.define(slots=True, kw_only=True)
class _CreateSlot(Handler[None, UUID]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> UUID:
        slot = await self.ctx.document.command(SLOT_SPEC).create(SlotCreate())
        return slot.id


@attrs.define(slots=True, kw_only=True)
class _Claim(Handler[ClaimCmd, None]):
    ctx: ExecutionContext

    async def __call__(self, args: ClaimCmd) -> None:
        slot = await self.ctx.document.query(SLOT_SPEC).get(args.slot)
        if slot.owner is None:
            # The rev guard serializes the racing claimants: only one update wins, the
            # losers conflict and their transaction rolls back.
            await self.ctx.document.command(SLOT_SPEC).update(
                args.slot, slot.rev, SlotUpdate(owner=args.who)
            )


_TX_PLAN = OperationPlan().bind_tx().set_route("mock").finish(deep=False)


def _registry() -> OperationRegistry:
    handlers = {
        "create_slot": lambda ctx: _CreateSlot(ctx=ctx),
        "claim": lambda ctx: _Claim(ctx=ctx),
    }
    return OperationRegistry(
        handlers=handlers,
        plans={op: _TX_PLAN for op in handlers},
        descriptors={
            "create_slot": OperationDescriptor(
                input_type=None, output_type=None, description="x"
            ),
            "claim": OperationDescriptor(
                input_type=ClaimCmd, output_type=None, description="x"
            ),
        },
    ).freeze()


def _runtime(state: MockState) -> ExecutionRuntime:
    return ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule(state=state)).freeze()
    )


# ....................... #


def test_multiple_runtimes_share_one_store() -> None:
    registry = _registry()

    def run() -> int:
        state = MockState()

        async def replica(_i: int) -> None:
            runtime = _runtime(state)
            async with runtime.scope():
                await run_operation(
                    registry, "create_slot", None, runtime.get_context()
                )

        async def scenario() -> None:
            await asyncio.gather(*(replica(i) for i in range(5)))

        run_simulation(scenario, seed=0)
        return sum(len(store) for store in state.documents.values())

    # All five runtimes wrote into the one shared store, and the result is deterministic.
    assert run() == 5
    assert run() == 5


def test_cross_runtime_contention_goes_through_optimistic_concurrency() -> None:
    registry = _registry()

    def run() -> int | None:
        state = MockState()

        async def setup() -> UUID:
            runtime = _runtime(state)
            async with runtime.scope():
                return await run_operation(
                    registry, "create_slot", None, runtime.get_context()
                )

        async def claim(slot: UUID, who: int) -> None:
            runtime = _runtime(state)
            async with runtime.scope():
                try:
                    await run_operation(
                        registry,
                        "claim",
                        ClaimCmd(slot=slot, who=who),
                        runtime.get_context(),
                    )
                except CoreException:
                    pass  # the revision losers roll back

        async def scenario() -> None:
            slot = await setup()
            await asyncio.gather(*(claim(slot, who) for who in range(4)))

        run_simulation(scenario, seed=0, schedule_seed=0)

        store = next(iter(state.documents.values()))
        (row,) = store.values()
        return row["owner"]

    # Exactly one runtime claims the slot (mutual exclusion across runtimes), reproducibly.
    owner = run()
    assert owner is not None
    assert run() == owner

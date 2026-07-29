"""D family — distributed-primitive misuse over a lease-row lock protocol.

The lock is a lease row in a lock table (a real production pattern: DB-backed locks), acquired
by an atomic unique-id create committed in its own transaction; the critical section is a
read-modify-**blind**-write over a balance, so only the lock protects it. The correct twin holds
the lease for the run (no in-run release — a lease without expiry inside the horizon), so a
loser backs off and the balance always equals the transfer log. D1 skips the lock; D3 acquires
it check-then-set (count-then-create with a fresh id), so two acquirers can both "win".
"""

from __future__ import annotations

from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions.model import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, Rule, Scenario, Simulation
from forze_dst.invariants import expect
from forze_dst.markers import record_event
from forze_dst.misuse import MisuseCase
from forze_mock import MockDepsModule

# ----------------------- #


class Balance(Document):
    resource: int
    value: int


class BalanceCreate(CreateDocumentCmd):
    resource: int
    value: int


class BalanceUpdate(BaseDTO):
    value: int | None = None


class BalanceRead(ReadDocument):
    resource: int
    value: int


class LockRow(Document):
    resource: int


class LockRowCreate(CreateDocumentCmd):
    resource: int


class LockRowRead(ReadDocument):
    resource: int


class TransferRow(Document):
    resource: int


class TransferRowCreate(CreateDocumentCmd):
    resource: int


class TransferRowRead(ReadDocument):
    resource: int


BALANCE_SPEC = DocumentSpec(
    name="balances",
    read=BalanceRead,
    write=DocumentWriteTypes(domain=Balance, create_cmd=BalanceCreate, update_cmd=BalanceUpdate),
)
LOCK_SPEC = DocumentSpec(
    name="lock_rows",
    read=LockRowRead,
    write=DocumentWriteTypes(domain=LockRow, create_cmd=LockRowCreate),
)
TRANSFER_SPEC = DocumentSpec(
    name="transfer_log",
    read=TransferRowRead,
    write=DocumentWriteTypes(domain=TransferRow, create_cmd=TransferRowCreate),
)

RESOURCE = 1


def _balance_id(resource: int) -> UUID:
    return UUID(int=80000 + resource)


def _lock_id(resource: int) -> UUID:
    return UUID(int=90000 + resource)


class TransferCmd(BaseModel):
    resource: int


# ....................... #


@attrs.define(slots=True, kw_only=True)
class _Transfer(Handler[TransferCmd, None]):
    """``mode``: ``"locked"`` = correct; ``"skip"`` = MUTANT D1; ``"nonatomic"`` = MUTANT D3."""

    ctx: ExecutionContext
    mode: str

    async def __call__(self, args: TransferCmd) -> None:
        resource = args.resource

        if self.mode == "locked":
            try:
                async with self.ctx.tx_ctx.scope("mock"):
                    await self.ctx.document.command(LOCK_SPEC).create(
                        LockRowCreate(resource=resource), id=_lock_id(resource)
                    )
            except CoreException as error:
                if error.kind is ExceptionKind.CONFLICT:
                    return  # the lease is held — back off
                raise
        elif self.mode == "nonatomic":
            # MUTANT (D3 nonatomic_acquire): check-then-set — two acquirers both see "free"
            # and both create their own (fresh-id) lease row.
            async with self.ctx.tx_ctx.scope("mock"):
                held = await self.ctx.document.query(LOCK_SPEC).count(
                    {"$values": {"resource": resource}}
                )
            if held:
                return
            async with self.ctx.tx_ctx.scope("mock"):
                await self.ctx.document.command(LOCK_SPEC).create(LockRowCreate(resource=resource))
        # MUTANT (D1 skip_lock): mode == "skip" — straight into the critical section.

        async with self.ctx.tx_ctx.scope("mock"):
            balance = await self.ctx.document.query(BALANCE_SPEC).get(_balance_id(resource))
        async with self.ctx.tx_ctx.scope("mock"):
            await self.ctx.document.command(BALANCE_SPEC).update_matching(
                {"$values": {"id": {"$eq": _balance_id(resource)}}},
                BalanceUpdate(value=balance.value + 1),
                return_new=False,
            )
            await self.ctx.document.command(TRANSFER_SPEC).create(
                TransferRowCreate(resource=resource)
            )


# ....................... #

_SCENARIO = Scenario(
    state=ModelState,
    act=(Rule(op="transfer", arg=lambda _state, _rng: TransferCmd(resource=RESOURCE)),),
)


def _case(mode: str) -> MisuseCase:
    registry = OperationRegistry(
        handlers={"transfer": lambda ctx: _Transfer(ctx=ctx, mode=mode)},
        descriptors={
            "transfer": OperationDescriptor(
                input_type=TransferCmd, output_type=None, description="Transfer under the lease."
            )
        },
    ).freeze()

    async def setup(ctx: ExecutionContext) -> None:
        async with ctx.tx_ctx.scope("mock"):
            await ctx.document.command(BALANCE_SPEC).create(
                BalanceCreate(resource=RESOURCE, value=0), id=_balance_id(RESOURCE)
            )

    async def observe(ctx: ExecutionContext) -> None:
        async with ctx.tx_ctx.scope("mock"):
            balance = await ctx.document.query(BALANCE_SPEC).get(_balance_id(RESOURCE))
            transfers = await ctx.document.query(TRANSFER_SPEC).count(
                {"$values": {"resource": RESOURCE}}
            )
        record_event("ledger", value=balance.value, transfers=transfers)

    return MisuseCase(
        simulation=Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            setup=setup,
            observe=observe,
            invariants=[
                expect(
                    "ledger",
                    lambda e: e.fields["value"] == e.fields["transfers"],
                    message="a transfer was lost inside the unprotected critical section",
                )
            ],
        ),
        scenario=_SCENARIO,
    )


def d1_skip_lock() -> MisuseCase:
    return _case("skip")


def d3_nonatomic_acquire() -> MisuseCase:
    return _case("nonatomic")


def ctrl_lock_protocol() -> MisuseCase:
    return _case("locked")

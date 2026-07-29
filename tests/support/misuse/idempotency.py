"""I family — idempotency & retry misuse mutants: the same command delivered more than once.

The workload models at-least-once delivery by drawing command ids from a two-element pool, so
duplicates arrive naturally (sequentially or concurrently). The correct twin derives the charge
row's id from the command id — a redelivery conflicts and is swallowed as already-done; the
mutant appends a fresh row per delivery. The oracle reads port state (rows per command), never
markers — see the T-family module docstring for why.
"""

from __future__ import annotations

from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions.model import CoreException, ExceptionKind
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, Rule, Scenario, Simulation
from forze_dst.faults import CrashPolicy
from forze_dst.invariants import expect
from forze_dst.markers import record_event
from forze_dst.misuse import MisuseCase
from forze_mock import MockDepsModule

# ----------------------- #

_POOL = (0, 1)
"""The command-id pool; any act_count >= 3 delivers some command at least twice (the retry)."""


class ChargeRow(Document):
    command: int


class ChargeRowCreate(CreateDocumentCmd):
    command: int


class ChargeRowRead(ReadDocument):
    command: int


CHARGE_SPEC = DocumentSpec(
    name="charges",
    read=ChargeRowRead,
    write=DocumentWriteTypes(domain=ChargeRow, create_cmd=ChargeRowCreate),
)


class ChargeCmd(BaseModel):
    command: int


# ....................... #


@attrs.define(slots=True, kw_only=True)
class _ChargeWithoutKey(Handler[ChargeCmd, None]):
    ctx: ExecutionContext

    async def __call__(self, args: ChargeCmd) -> None:
        # MUTANT (I1 drop_idempotency_key): every delivery of the same command appends a fresh
        # charge row — the retried command charges twice.
        await self.ctx.document.command(CHARGE_SPEC).create(ChargeRowCreate(command=args.command))


@attrs.define(slots=True, kw_only=True)
class _ChargeWithKey(Handler[ChargeCmd, None]):
    """CORRECT (adversarial shape): the same retried workload, deduplicated by the command id."""

    ctx: ExecutionContext

    async def __call__(self, args: ChargeCmd) -> None:
        try:
            await self.ctx.document.command(CHARGE_SPEC).create(
                ChargeRowCreate(command=args.command), id=UUID(int=args.command + 1)
            )
        except CoreException as error:
            if error.kind is not ExceptionKind.CONFLICT:
                raise  # a conflict means already charged — the retry is a no-op


# ....................... #

_TX_PLAN = OperationPlan().bind_tx().set_route("mock").finish(deep=False)

_CAMPAIGN_POOL = tuple(range(100, 116))
"""The de-saturated campaign pool: two draws from 16 collide with probability ≈ 1/16."""

_RETRY_SCENARIO = Scenario(
    state=ModelState,
    act=(Rule(op="charge", arg=lambda _state, rng: ChargeCmd(command=rng.choice(_POOL))),),
)
_CAMPAIGN_SCENARIO = Scenario(
    state=ModelState,
    act=(Rule(op="charge", arg=lambda _state, rng: ChargeCmd(command=rng.choice(_CAMPAIGN_POOL))),),
)


def _case(handler_factory, *, pooled: bool = False) -> MisuseCase:  # type: ignore[no-untyped-def]
    registry = OperationRegistry(
        handlers={"charge": handler_factory},
        plans={"charge": _TX_PLAN},
        descriptors={
            "charge": OperationDescriptor(
                input_type=ChargeCmd, output_type=None, description="Charge a command."
            )
        },
    ).freeze()

    pool = _CAMPAIGN_POOL if pooled else _POOL

    async def observe(ctx: ExecutionContext) -> None:
        for command in pool:
            total = await ctx.document.query(CHARGE_SPEC).count({"$values": {"command": command}})
            record_event("charge_rows", command=command, total=total)

    return MisuseCase(
        simulation=Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            observe=observe,
            invariants=[
                expect(
                    "charge_rows",
                    lambda e: e.fields["total"] <= 1,
                    message="a command charged more than once",
                )
            ],
        ),
        scenario=_CAMPAIGN_SCENARIO if pooled else _RETRY_SCENARIO,
    )


# ....................... #
# I3 — ack before processing: the delivery is marked done in its own transaction before the
# effect is applied. A crash between the commits loses the effect FOREVER: the redelivery sees
# the ack and skips (at-most-once where at-least-once was required).


class AckRow(Document):
    message: int


class AckRowCreate(CreateDocumentCmd):
    message: int


class AckRowRead(ReadDocument):
    message: int


class EffectRow(Document):
    message: int


class EffectRowCreate(CreateDocumentCmd):
    message: int


class EffectRowRead(ReadDocument):
    message: int


ACK_SPEC = DocumentSpec(
    name="acks",
    read=AckRowRead,
    write=DocumentWriteTypes(domain=AckRow, create_cmd=AckRowCreate),
)
EFFECT_SPEC = DocumentSpec(
    name="effects",
    read=EffectRowRead,
    write=DocumentWriteTypes(domain=EffectRow, create_cmd=EffectRowCreate),
)


class ProcessCmd(BaseModel):
    message: int


@attrs.define(slots=True, kw_only=True)
class _Process(Handler[ProcessCmd, None]):
    """``ack_first=True`` is the MUTANT (I3 ack_before_processing): the ack commits alone before
    the effect; the correct twin applies effect and ack in one transaction."""

    ctx: ExecutionContext
    ack_first: bool

    async def __call__(self, args: ProcessCmd) -> None:
        if not self.ack_first:
            try:
                async with self.ctx.tx_ctx.scope("mock"):
                    await self.ctx.document.command(ACK_SPEC).create(
                        AckRowCreate(message=args.message), id=UUID(int=args.message + 1)
                    )
                    await self.ctx.document.command(EFFECT_SPEC).create(
                        EffectRowCreate(message=args.message)
                    )
            except CoreException as error:
                if error.kind is not ExceptionKind.CONFLICT:
                    raise  # already processed — the redelivery is a no-op
            return

        # MUTANT (I3 ack_before_processing): ack commits first, in its own transaction.
        try:
            async with self.ctx.tx_ctx.scope("mock"):
                await self.ctx.document.command(ACK_SPEC).create(
                    AckRowCreate(message=args.message), id=UUID(int=args.message + 1)
                )
        except CoreException as error:
            if error.kind is ExceptionKind.CONFLICT:
                return  # "already done" — after a crash in the window, the loss is permanent
            raise

        async with self.ctx.tx_ctx.scope("mock"):
            await self.ctx.document.command(EFFECT_SPEC).create(
                EffectRowCreate(message=args.message)
            )


_PROCESS_SCENARIO = Scenario(
    state=ModelState,
    act=(Rule(op="process", arg=lambda _state, rng: ProcessCmd(message=rng.choice(_POOL))),),
)

_CRASH = CrashPolicy(surface="document_command", probability=0.25)


def _process_case(*, ack_first: bool) -> MisuseCase:
    registry = OperationRegistry(
        handlers={"process": lambda ctx: _Process(ctx=ctx, ack_first=ack_first)},
        descriptors={
            "process": OperationDescriptor(
                input_type=ProcessCmd, output_type=None, description="Process a delivery."
            )
        },
    ).freeze()

    async def observe(ctx: ExecutionContext) -> None:
        async with ctx.tx_ctx.scope("mock"):
            for message in _POOL:
                acked = await ctx.document.query(ACK_SPEC).count(
                    {"$values": {"message": message}}
                )
                effects = await ctx.document.query(EFFECT_SPEC).count(
                    {"$values": {"message": message}}
                )
                record_event("ack_effect", message=message, acked=acked, effects=effects)

    return MisuseCase(
        simulation=Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            observe=observe,
            invariants=[
                expect(
                    "ack_effect",
                    lambda e: e.fields["acked"] == 0 or e.fields["effects"] >= 1,
                    message="a message was acked but its effect was never applied (lost)",
                )
            ],
        ),
        scenario=_PROCESS_SCENARIO,
        crash=_CRASH,
    )


def i3_ack_before_processing() -> MisuseCase:
    return _process_case(ack_first=True)


def ctrl_process_then_ack() -> MisuseCase:
    return _process_case(ack_first=False)


# ....................... #
# I2 — retry_without_idempotency: a naive in-handler retry loop around a NON-idempotent effect.
# The effect (a receipt row) commits in its own transaction — it is out the door — and the ack
# (a per-order submission marker) conflicts for the loser of a duplicate submission; the naive
# loop then re-runs the whole block, minting a second receipt for the same command. The correct
# twin runs the *same* retry loop with the receipt keyed by the command id, so a re-run re-creates
# the same row and the duplicate collapses into an already-done conflict.


class ReceiptRow(Document):
    command: int


class ReceiptRowCreate(CreateDocumentCmd):
    command: int


class ReceiptRowRead(ReadDocument):
    command: int


class SubmissionRow(Document):
    booking: int


class SubmissionRowCreate(CreateDocumentCmd):
    booking: int


class SubmissionRowRead(ReadDocument):
    booking: int


RECEIPT_SPEC = DocumentSpec(
    name="receipts",
    read=ReceiptRowRead,
    write=DocumentWriteTypes(domain=ReceiptRow, create_cmd=ReceiptRowCreate),
)
SUBMISSION_SPEC = DocumentSpec(
    name="submissions",
    read=SubmissionRowRead,
    write=DocumentWriteTypes(domain=SubmissionRow, create_cmd=SubmissionRowCreate),
)

_ORDER_POOL = (0, 1)
_ORDER_CAMPAIGN_POOL = tuple(range(300, 316))


class SubmitCmd(BaseModel):
    order: int
    command: int


@attrs.define(slots=True, kw_only=True)
class _Submit(Handler[SubmitCmd, None]):
    """``keyed=False`` is the MUTANT (I2 retry_without_idempotency); ``True`` the correct twin."""

    ctx: ExecutionContext
    keyed: bool

    async def __call__(self, args: SubmitCmd) -> None:
        for _attempt in range(2):
            if self.keyed:
                try:
                    async with self.ctx.tx_ctx.scope("mock"):
                        await self.ctx.document.command(RECEIPT_SPEC).create(
                            ReceiptRowCreate(command=args.command),
                            id=UUID(int=30000 + args.command),
                        )
                except CoreException as error:
                    if error.kind is not ExceptionKind.CONFLICT:
                        raise  # already receipted — the re-run collapses into a no-op
            else:
                # MUTANT (I2 retry_without_idempotency): a fresh receipt row per attempt —
                # committed before the ack, so the loser's re-run double-charges the command.
                async with self.ctx.tx_ctx.scope("mock"):
                    await self.ctx.document.command(RECEIPT_SPEC).create(
                        ReceiptRowCreate(command=args.command)
                    )

            try:
                async with self.ctx.tx_ctx.scope("mock"):
                    await self.ctx.document.command(SUBMISSION_SPEC).create(
                        SubmissionRowCreate(booking=args.order), id=UUID(int=40000 + args.order)
                    )
                return
            except CoreException as error:
                if error.kind is not ExceptionKind.CONFLICT:
                    raise
                continue  # naive retry: re-run the WHOLE effect block


def _submit_scenario(pool: tuple[int, ...]) -> Scenario:
    commands = iter(range(1_000_000))

    return Scenario(
        state=ModelState,
        act=(
            Rule(
                op="submit",
                arg=lambda _state, rng: SubmitCmd(
                    order=rng.choice(pool), command=next(commands)
                ),
            ),
        ),
    )


def _submit_case(*, keyed: bool, pooled: bool = False) -> MisuseCase:
    registry = OperationRegistry(
        handlers={"submit": lambda ctx: _Submit(ctx=ctx, keyed=keyed)},
        descriptors={
            "submit": OperationDescriptor(
                input_type=SubmitCmd, output_type=None, description="Submit an order."
            )
        },
    ).freeze()

    async def observe(ctx: ExecutionContext) -> None:
        async with ctx.tx_ctx.scope("mock"):
            rows = await ctx.document.query(RECEIPT_SPEC).find_many(
                {"$values": {"command": {"$gte": 0}}}
            )
        totals: dict[int, int] = {}
        for row in rows.hits:
            totals[row.command] = totals.get(row.command, 0) + 1
        for command, total in sorted(totals.items()):
            record_event("receipt_rows", command=command, total=total)

    return MisuseCase(
        simulation=Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            observe=observe,
            invariants=[
                expect(
                    "receipt_rows",
                    lambda e: e.fields["total"] <= 1,
                    message="a command receipted more than once through the naive retry loop",
                )
            ],
        ),
        scenario=_submit_scenario(_ORDER_CAMPAIGN_POOL if pooled else _ORDER_POOL),
    )


def i2_retry_without_idempotency() -> MisuseCase:
    return _submit_case(keyed=False)


def i2_retry_without_idempotency_campaign() -> MisuseCase:
    return _submit_case(keyed=False, pooled=True)


def ctrl_idempotent_retry() -> MisuseCase:
    return _submit_case(keyed=True)


# ....................... #


def i1_retry_without_key() -> MisuseCase:
    return _case(lambda ctx: _ChargeWithoutKey(ctx=ctx))


def i1_retry_without_key_campaign() -> MisuseCase:
    return _case(lambda ctx: _ChargeWithoutKey(ctx=ctx), pooled=True)


def ctrl_retry_with_key() -> MisuseCase:
    return _case(lambda ctx: _ChargeWithKey(ctx=ctx))

"""M family — messaging misuse mutants: a consumer without inbox deduplication.

The workload models broker redelivery by drawing message ids from a two-element pool. The
correct consumer runs the inbox pattern — insert the message id into the inbox table first (a
redelivery conflicts there) and apply the effect **in the same transaction**; the mutant applies
the effect unconditionally, so a redelivered message is processed twice. The oracle reads port
state (handled rows per message) — see the T-family module docstring for why markers are banned.
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
from forze_dst.invariants import expect
from forze_dst.markers import record_event
from forze_dst.misuse import MisuseCase
from forze_mock import MockDepsModule

# ----------------------- #

_POOL = (0, 1)
"""The message-id pool; any act_count >= 3 redelivers some message (at-least-once delivery)."""


class InboxRow(Document):
    message: int


class InboxRowCreate(CreateDocumentCmd):
    message: int


class InboxRowRead(ReadDocument):
    message: int


class HandledRow(Document):
    message: int


class HandledRowCreate(CreateDocumentCmd):
    message: int


class HandledRowRead(ReadDocument):
    message: int


INBOX_SPEC = DocumentSpec(
    name="consumer_inbox",
    read=InboxRowRead,
    write=DocumentWriteTypes(domain=InboxRow, create_cmd=InboxRowCreate),
)
HANDLED_SPEC = DocumentSpec(
    name="handled",
    read=HandledRowRead,
    write=DocumentWriteTypes(domain=HandledRow, create_cmd=HandledRowCreate),
)


class Delivery(BaseModel):
    message: int


# ....................... #


@attrs.define(slots=True, kw_only=True)
class _ConsumeWithoutInbox(Handler[Delivery, None]):
    ctx: ExecutionContext

    async def __call__(self, args: Delivery) -> None:
        # MUTANT (M2 drop_inbox_dedup): the effect is applied on every delivery — a redelivered
        # message is processed twice.
        await self.ctx.document.command(HANDLED_SPEC).create(HandledRowCreate(message=args.message))


@attrs.define(slots=True, kw_only=True)
class _ConsumeWithInbox(Handler[Delivery, None]):
    """CORRECT: inbox-first, effect in the same transaction — a redelivery conflicts and stops,
    and a concurrent duplicate loses the inbox row and its effect together at commit."""

    ctx: ExecutionContext

    async def __call__(self, args: Delivery) -> None:
        try:
            await self.ctx.document.command(INBOX_SPEC).create(
                InboxRowCreate(message=args.message), id=UUID(int=args.message + 1)
            )
        except CoreException as error:
            if error.kind is ExceptionKind.CONFLICT:
                return  # already processed — the redelivery is a no-op
            raise

        await self.ctx.document.command(HANDLED_SPEC).create(HandledRowCreate(message=args.message))


# ....................... #

_TX_PLAN = OperationPlan().bind_tx().set_route("mock").finish(deep=False)

_CAMPAIGN_POOL = tuple(range(100, 116))
"""The de-saturated campaign pool: two draws from 16 collide with probability ≈ 1/16."""

_REDELIVERY_SCENARIO = Scenario(
    state=ModelState,
    act=(Rule(op="consume", arg=lambda _state, rng: Delivery(message=rng.choice(_POOL))),),
)
_CAMPAIGN_SCENARIO = Scenario(
    state=ModelState,
    act=(Rule(op="consume", arg=lambda _state, rng: Delivery(message=rng.choice(_CAMPAIGN_POOL))),),
)


def _case(handler_factory, *, pooled: bool = False) -> MisuseCase:  # type: ignore[no-untyped-def]
    registry = OperationRegistry(
        handlers={"consume": handler_factory},
        plans={"consume": _TX_PLAN},
        descriptors={
            "consume": OperationDescriptor(
                input_type=Delivery, output_type=None, description="Consume a delivery."
            )
        },
    ).freeze()

    pool = _CAMPAIGN_POOL if pooled else _POOL

    async def observe(ctx: ExecutionContext) -> None:
        for message in pool:
            total = await ctx.document.query(HANDLED_SPEC).count({"$values": {"message": message}})
            record_event("handled_rows", message=message, total=total)

    return MisuseCase(
        simulation=Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            observe=observe,
            invariants=[
                expect(
                    "handled_rows",
                    lambda e: e.fields["total"] <= 1,
                    message="a message was processed more than once",
                )
            ],
        ),
        scenario=_CAMPAIGN_SCENARIO if pooled else _REDELIVERY_SCENARIO,
    )


def m2_consumer_without_inbox() -> MisuseCase:
    return _case(lambda ctx: _ConsumeWithoutInbox(ctx=ctx))


def m2_consumer_without_inbox_campaign() -> MisuseCase:
    return _case(lambda ctx: _ConsumeWithoutInbox(ctx=ctx), pooled=True)


def ctrl_inbox_consumer() -> MisuseCase:
    return _case(lambda ctx: _ConsumeWithInbox(ctx=ctx))

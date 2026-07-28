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


def i1_retry_without_key() -> MisuseCase:
    return _case(lambda ctx: _ChargeWithoutKey(ctx=ctx))


def i1_retry_without_key_campaign() -> MisuseCase:
    return _case(lambda ctx: _ChargeWithoutKey(ctx=ctx), pooled=True)


def ctrl_retry_with_key() -> MisuseCase:
    return _case(lambda ctx: _ChargeWithKey(ctx=ctx))

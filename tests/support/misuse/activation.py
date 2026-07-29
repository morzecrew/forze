"""T family, deep instance — torn activation: create and activate in separate transactions.

The corpus's first **genuinely depth-2** mutant. A provision that creates a profile in one
transaction and activates it in a second leaves a torn window (created-but-not-ready) between
the commits; a padded reader observes it only under a schedule that delays the activation past
the reader's profile-read. The padding is tuned so plain FIFO is clean in *both* spawn orders —
the 1-minimal reproducing schedule carries exactly one non-FIFO choice, mechanically verified
by :func:`forze_dst.depth.extract_depth` (``choices=(0, 0, 0, 1)``). The reader records what it
observed as a **row** (port state, transferable), never a marker.

The correct twin provisions atomically — create and activate in one transaction — and no
schedule at any depth can expose the window.
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


class Profile(Document):
    ready: bool = False


class ProfileCreate(CreateDocumentCmd):
    ready: bool = False


class ProfileUpdate(BaseDTO):
    ready: bool | None = None


class ProfileRead(ReadDocument):
    ready: bool


class ServeLog(Document):
    profile: UUID
    state: str


class ServeLogCreate(CreateDocumentCmd):
    profile: UUID
    state: str


class ServeLogRead(ReadDocument):
    profile: UUID
    state: str


PROFILE_SPEC = DocumentSpec(
    name="profiles",
    read=ProfileRead,
    write=DocumentWriteTypes(domain=Profile, create_cmd=ProfileCreate, update_cmd=ProfileUpdate),
)
SERVE_SPEC = DocumentSpec(
    name="serve_log",
    read=ServeLogRead,
    write=DocumentWriteTypes(domain=ServeLog, create_cmd=ServeLogCreate),
)

PROFILE_ID = UUID(int=1)

SERVE_PADDING = 2
"""The reader's phase padding — the value at which FIFO is clean in both spawn orders while one
non-FIFO choice still reaches the torn window (the d=2 alignment, mechanically verified; padding
0–1 degenerates to d=1, padding ≥3 makes the window unreachable within 40k systematic runs)."""


class Nothing(BaseModel):
    pass


# ....................... #


@attrs.define(slots=True, kw_only=True)
class _Provision(Handler[Nothing, None]):
    """``atomic=False`` is the MUTANT (T3 write_outside_tx, deep instance): the activation lives
    in its own transaction, so a torn created-but-not-ready state is externally visible."""

    ctx: ExecutionContext
    atomic: bool

    async def __call__(self, _args: Nothing) -> None:
        if self.atomic:
            async with self.ctx.tx_ctx.scope("mock"):
                await self.ctx.document.command(PROFILE_SPEC).create(
                    ProfileCreate(ready=False), id=PROFILE_ID
                )
                profile = await self.ctx.document.query(PROFILE_SPEC).get(PROFILE_ID)
                await self.ctx.document.command(PROFILE_SPEC).update(
                    PROFILE_ID, profile.rev, ProfileUpdate(ready=True)
                )
            return

        # MUTANT (T3 write_outside_tx): create commits alone; the activation is a second
        # transaction — the torn window is the gap between the two commits.
        async with self.ctx.tx_ctx.scope("mock"):
            await self.ctx.document.command(PROFILE_SPEC).create(
                ProfileCreate(ready=False), id=PROFILE_ID
            )
        async with self.ctx.tx_ctx.scope("mock"):
            profile = await self.ctx.document.query(PROFILE_SPEC).get(PROFILE_ID)
            await self.ctx.document.command(PROFILE_SPEC).update(
                PROFILE_ID, profile.rev, ProfileUpdate(ready=True)
            )


@attrs.define(slots=True, kw_only=True)
class _Serve(Handler[Nothing, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: Nothing) -> None:
        async with self.ctx.tx_ctx.scope("mock"):
            for _ in range(SERVE_PADDING):
                await self.ctx.document.query(SERVE_SPEC).count()  # benign phase padding

            try:
                profile = await self.ctx.document.query(PROFILE_SPEC).get(PROFILE_ID)
                state = "ready" if profile.ready else "torn"
            except CoreException as error:
                if error.kind is not ExceptionKind.NOT_FOUND:
                    raise
                state = "absent"

            await self.ctx.document.command(SERVE_SPEC).create(
                ServeLogCreate(profile=PROFILE_ID, state=state)
            )


# ....................... #

_SCENARIO = Scenario(
    state=ModelState,
    act=(
        Rule(op="provision", arg=lambda _state, _rng: Nothing()),
        Rule(op="serve", arg=lambda _state, _rng: Nothing()),
    ),
)


def _case(*, atomic: bool) -> MisuseCase:
    registry = OperationRegistry(
        handlers={
            "provision": lambda ctx: _Provision(ctx=ctx, atomic=atomic),
            "serve": lambda ctx: _Serve(ctx=ctx),
        },
        descriptors={
            "provision": OperationDescriptor(
                input_type=Nothing, output_type=None, description="Provision a profile."
            ),
            "serve": OperationDescriptor(
                input_type=Nothing, output_type=None, description="Serve from the profile."
            ),
        },
    ).freeze()

    async def observe(ctx: ExecutionContext) -> None:
        async with ctx.tx_ctx.scope("mock"):
            torn = await ctx.document.query(SERVE_SPEC).count({"$values": {"state": "torn"}})
        record_event("torn_serves", total=torn)

    return MisuseCase(
        simulation=Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            observe=observe,
            invariants=[
                expect(
                    "torn_serves",
                    lambda e: e.fields["total"] == 0,
                    message="a reader observed the torn created-but-not-ready state",
                )
            ],
        ),
        scenario=_SCENARIO,
    )


def t3_torn_activation() -> MisuseCase:
    return _case(atomic=False)


def ctrl_atomic_provision() -> MisuseCase:
    return _case(atomic=True)

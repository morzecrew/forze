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


async def provision_profile(
    ctx: ExecutionContext, pid: UUID, *, atomic: bool, scope: str = "mock"
) -> None:
    """Create + activate one profile: one transaction when *atomic*, two when not.

    The non-atomic split is the seeded misuse (T3 write_outside_tx): the create commits alone,
    so the torn created-but-not-ready window is the gap between the two commits.
    """

    if atomic:
        async with ctx.tx_ctx.scope(scope):
            await ctx.document.command(PROFILE_SPEC).create(ProfileCreate(ready=False), id=pid)
            profile = await ctx.document.query(PROFILE_SPEC).get(pid)
            await ctx.document.command(PROFILE_SPEC).update(
                pid, profile.rev, ProfileUpdate(ready=True)
            )
        return

    async with ctx.tx_ctx.scope(scope):
        await ctx.document.command(PROFILE_SPEC).create(ProfileCreate(ready=False), id=pid)
    async with ctx.tx_ctx.scope(scope):
        profile = await ctx.document.query(PROFILE_SPEC).get(pid)
        await ctx.document.command(PROFILE_SPEC).update(
            pid, profile.rev, ProfileUpdate(ready=True)
        )


async def profile_state(ctx: ExecutionContext, pid: UUID) -> str:
    """What a reader observes: ``ready`` / ``torn`` (created, not activated) / ``absent``.

    Queries only — runs inside whatever transaction scope the caller holds.
    """

    try:
        profile = await ctx.document.query(PROFILE_SPEC).get(pid)
    except CoreException as error:
        if error.kind is not ExceptionKind.NOT_FOUND:
            raise
        return "absent"

    return "ready" if profile.ready else "torn"


@attrs.define(slots=True, kw_only=True)
class _Provision(Handler[Nothing, None]):
    """``atomic=False`` is the MUTANT (T3 write_outside_tx, deep instance): the activation lives
    in its own transaction, so a torn created-but-not-ready state is externally visible."""

    ctx: ExecutionContext
    atomic: bool

    async def __call__(self, _args: Nothing) -> None:
        await provision_profile(self.ctx, PROFILE_ID, atomic=self.atomic)


@attrs.define(slots=True, kw_only=True)
class _Serve(Handler[Nothing, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: Nothing) -> None:
        async with self.ctx.tx_ctx.scope("mock"):
            for _ in range(SERVE_PADDING):
                await self.ctx.document.query(SERVE_SPEC).count()  # benign phase padding

            state = await profile_state(self.ctx, PROFILE_ID)
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


# ....................... #
# T3, third instance — the corpus's first genuinely depth-3 mutant. Provision creates and
# activates TWO profiles (a config half and a content half), each torn-style in separate
# transactions; the serve composes a response from both, degrading gracefully when one half is
# unavailable (state "partial") but serving a total "blackout" only when BOTH reads land in
# their torn windows. Reaching both windows needs the writer stalled twice at two separated
# points — mechanically two non-FIFO choices (d=3; every single-choice vector exhaustively
# refuted at the recorded pads), and *four* PCT priority segments, which is where the
# mechanical (tick-promotion) and PCT (priority-stall) depth models measurably diverge:
# random outruns every PCT variant on this bug, and pct-d4 outruns pct-d3.

PAIR_A_ID = UUID(int=11)
PAIR_B_ID = UUID(int=12)

PAIR_SERVE_PADDING = 2
"""The two-window phase alignment: pads before the first read (the second read follows
immediately) at which FIFO is clean and no single promotion reaches both windows."""


@attrs.define(slots=True, kw_only=True)
class _ProvisionPair(Handler[Nothing, None]):
    """``atomic=False`` is the MUTANT (T3 write_outside_tx, double-window instance): each half's
    activation commits separately, so two torn windows open in sequence."""

    ctx: ExecutionContext
    atomic: bool

    async def __call__(self, _args: Nothing) -> None:
        for pid in (PAIR_A_ID, PAIR_B_ID):
            await provision_profile(self.ctx, pid, atomic=self.atomic)


@attrs.define(slots=True, kw_only=True)
class _ServeBoth(Handler[Nothing, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: Nothing) -> None:
        async with self.ctx.tx_ctx.scope("mock"):
            for _ in range(PAIR_SERVE_PADDING):
                await self.ctx.document.query(SERVE_SPEC).count()  # benign phase padding

            state_a = await profile_state(self.ctx, PAIR_A_ID)
            state_b = await profile_state(self.ctx, PAIR_B_ID)

            # One torn half degrades gracefully; BOTH torn is the served blackout.
            state = "blackout" if state_a == "torn" and state_b == "torn" else "partial"
            await self.ctx.document.command(SERVE_SPEC).create(
                ServeLogCreate(profile=PAIR_A_ID, state=state)
            )


_PAIR_SCENARIO = Scenario(
    state=ModelState,
    act=(
        Rule(op="provision", arg=lambda _state, _rng: Nothing()),
        Rule(op="serve", arg=lambda _state, _rng: Nothing()),
    ),
)


def _pair_case(*, atomic: bool) -> MisuseCase:
    registry = OperationRegistry(
        handlers={
            "provision": lambda ctx: _ProvisionPair(ctx=ctx, atomic=atomic),
            "serve": lambda ctx: _ServeBoth(ctx=ctx),
        },
        descriptors={
            "provision": OperationDescriptor(
                input_type=Nothing, output_type=None, description="Provision both halves."
            ),
            "serve": OperationDescriptor(
                input_type=Nothing, output_type=None, description="Serve from both halves."
            ),
        },
    ).freeze()

    async def observe(ctx: ExecutionContext) -> None:
        async with ctx.tx_ctx.scope("mock"):
            blackouts = await ctx.document.query(SERVE_SPEC).count(
                {"$values": {"state": "blackout"}}
            )
        record_event("blackout_serves", total=blackouts)

    return MisuseCase(
        simulation=Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            observe=observe,
            invariants=[
                expect(
                    "blackout_serves",
                    lambda e: e.fields["total"] == 0,
                    message="a reader caught BOTH halves torn — the served blackout",
                )
            ],
        ),
        scenario=_PAIR_SCENARIO,
    )


def t3_double_torn() -> MisuseCase:
    return _pair_case(atomic=False)


def ctrl_atomic_pair() -> MisuseCase:
    return _pair_case(atomic=True)

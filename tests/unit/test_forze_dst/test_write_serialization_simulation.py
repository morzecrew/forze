"""Two writers for one owner, and their writes must not interleave.

This is the property the declaration delivers: **writes** for one owner are serialized. It is
not the same as "the application's own overlap check becomes correct" — the lock is taken before
the write, so two handlers can both read an empty relation, both decide, and land both rows
however well the inserts are ordered afterwards. Making a read-then-write atomic would need the
lock before the *read*, and reads are never serialized by this declaration.

So what is asserted here is the write spans. The contrast is the file's point: without the
declaration the two writes nest, which is what makes the governed run mean anything.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import date, timedelta
from typing import Any, Final

import attrs
from pydantic import BaseModel

from forze.application.contracts.deps import DepsModule
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.contracts.guarantees import SerializedBy
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst import OperationCase, Simulation, SimulationConfig, Strategy
from forze_dst import invariants as inv
from forze_dst.oracle.invariants import Violation, named
from forze_dst.markers import record_event
from forze_dst.oracle.recorder import History
from forze_mock import MockDepsModule, MockState

# ----------------------- #

_TX = "mock"
OWNER: Final = "the-one-owner"
EPOCH: Final = date(2026, 1, 1)

BY_OWNER: Final = SerializedBy(key=("owner",))

_EXPECTED_REFUSALS: Final = frozenset(
    {
        (ExceptionKind.CONFLICT, "core.conflict"),
        (ExceptionKind.PRECONDITION, "revision_mismatch"),
    }
)
"""The refusals the race produces; anything else is a defect wearing their clothes."""


class _Booking(Document):
    owner: str
    starts: date
    ends: date


class _BookingRead(ReadDocument):
    owner: str
    starts: date
    ends: date


class _BookingCreate(CreateDocumentCmd):
    owner: str
    starts: date
    ends: date


class _BookingUpdate(BaseDTO):
    ends: date | None = None


def _spec(*, governed: bool) -> DocumentSpec[Any, Any, Any, Any]:
    return DocumentSpec[_BookingRead, _Booking, _BookingCreate, _BookingUpdate](
        name="bookings",
        read=_BookingRead,
        write=DocumentWriteTypes(
            domain=_Booking, create_cmd=_BookingCreate, update_cmd=_BookingUpdate
        ),
        guarantees=(BY_OWNER,) if governed else (),
    )


class Book(BaseModel):
    offset: int
    length: int


# ....................... #


@attrs.define(slots=True, kw_only=True)
class _Book(Handler[Book, None]):
    """Write one interval for the owner, marking when the write starts and finishes.

    Markers rather than a clock: "did not interleave" means no other writer's span opened
    between these two, and a sequence answers that exactly where a virtual timestamp only
    answers it approximately.
    """

    ctx: ExecutionContext
    spec: DocumentSpec[Any, Any, Any, Any]
    unexpected: list[str]

    async def __call__(self, args: Book) -> None:
        starts = EPOCH + timedelta(days=args.offset)
        ends = starts + timedelta(days=args.length)

        try:
            await self.ctx.doc.command(self.spec).create(
                _BookingCreate(owner=OWNER, starts=starts, ends=ends)
            )

        except CoreException as caught:
            # A conflict is an ordinary outcome of the race: under a seeded generator two
            # concurrent creates can land on one id, which is a duplicate primary key rather
            # than anything about serialization. Anything else is a defect wearing its clothes.
            if (caught.kind, caught.code) not in _EXPECTED_REFUSALS:
                self.unexpected.append(f"{caught.kind}/{caught.code}")

            return

        # Marked *after* the write, not around it: a marker laid down before would time how
        # long this writer waited for the lock, and a second writer queueing behind it would
        # read as a second writer running inside it. What the declaration forbids is another
        # write landing before this transaction ends, which is the span below.
        record_event("write_span", owner=OWNER, edge="enter")
        await asyncio.sleep(0)
        record_event("write_span", owner=OWNER, edge="leave")


# ....................... #


def _writes_do_not_interleave():
    """No owner's write span opens while another span for that owner is still open.

    Read off the recorded history rather than a list the closure keeps: the sweep runs the
    workload several times — seeds, minimization candidates, the replay the report is built
    from — and an oracle reading state that outlives a run answers about the wrong one.
    """

    def check(history: History) -> list[Violation]:
        spans = [
            (str(event.fields["owner"]), str(event.fields["edge"]))
            for event in history.events
            if event.kind == "write_span"
        ]
        depth: dict[str, int] = {}
        found: list[Violation] = []

        for owner, edge in spans:
            held = depth.get(owner, 0)

            if edge == "enter":
                if held:
                    found.append(
                        Violation(
                            invariant="writes_do_not_interleave",
                            message=f"a second write for {owner!r} opened inside another",
                            events=(),
                        )
                    )

                depth[owner] = held + 1

            else:
                depth[owner] = max(held - 1, 0)

        return found

    return named("writes_do_not_interleave", check)


# ....................... #


def _run(*, governed: bool) -> tuple[Any, MockState, list[str]]:
    spec = _spec(governed=governed)
    state = MockState()
    unexpected: list[str] = []

    def deps() -> Sequence[DepsModule]:
        return [MockDepsModule(state=state)]

    plan = OperationPlan().bind_tx().set_route(_TX).finish(deep=False)
    registry = OperationRegistry(
        handlers={"book": lambda ctx: _Book(ctx=ctx, spec=spec, unexpected=unexpected)},
        plans={"book": plan},
        descriptors={
            "book": OperationDescriptor(
                input_type=Book, output_type=None, description="book an interval"
            ),
        },
    ).freeze()

    simulation = Simulation(
        operations=registry,
        deps=deps,
        invariants=[_writes_do_not_interleave(), inv.no_unexpected_error()],
    )

    report = simulation.run(
        SimulationConfig(
            strategy=Strategy.OP_CASE,
            count=4,
            act_count=6,
            concurrency=4,
            seeds=range(3),
        ),
        cases=[
            OperationCase(
                op="book",
                inputs=lambda rng: Book(offset=rng.randrange(6), length=rng.randrange(1, 5)),
            )
        ],
    )

    return report, state, unexpected


# ----------------------- #


class TestTwoWritersOneOwner:
    def test_the_declaration_keeps_writes_apart(self) -> None:
        report, state, unexpected = _run(governed=True)

        assert unexpected == [], f"refused for reasons the race cannot produce: {unexpected}"
        assert report is None, f"the declaration should have held, got {report}"

        # Not vacuous: a run where nothing landed would also have nothing interleaving.
        assert len(state.documents.get("bookings") or {}) > 1, "fewer than two bookings landed"

    def test_without_it_they_run_together(self) -> None:
        # The contrast. If this passed, the writers never raced and the run above attests
        # nothing — and it is the shape the origin application had in three writers of four.
        report, _, unexpected = _run(governed=False)

        assert unexpected == [], f"refused for reasons the race cannot produce: {unexpected}"
        assert report is not None, (
            "the ungoverned spec must let two writes for one owner overlap — if it does not, "
            "the workload is not concurrent enough for the governed run to attest anything"
        )
        assert any(v.invariant == "writes_do_not_interleave" for v in report.violations)

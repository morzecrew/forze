"""Two writers add contracts for one employee, and no two of them may overlap.

The kit batteries add periods one caller at a time, which proves the reads do what they say and
nothing about the case the declared guarantee exists for. Here concurrent writers add periods
for the same key under an interleaving scheduler, and the property is checked over the periods
that were actually stored.

The contrast is the point of the file. The same workload against a spec **without** the
guarantee must leave an overlapping pair; otherwise the governed run is green because the
writers never collided and the simulation attests nothing.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta
from typing import Any, Final

import attrs
from pydantic import BaseModel

from forze.application.contracts.deps import DepsModule
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.contracts.guarantees import NonOverlapping
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import Period
from forze.domain.models import BaseDTO, ReadDocument
from forze_dst import OperationCase, Simulation, SimulationConfig, Strategy
from forze_dst import invariants as inv
from forze_dst.oracle.invariants import Violation, named
from forze_dst.oracle.recorder import History
from forze_kits.domain.temporal import CreateCmdWithTemporalFields, DocWithTemporal
from forze_mock import MockDepsModule, MockState

# ----------------------- #

_TX = "mock"
EMPLOYEE: Final = "the-one-employee"
EPOCH: Final = date(2026, 1, 1)

_EXPECTED_REFUSALS: Final = frozenset(
    {
        (ExceptionKind.CONFLICT, "core.conflict"),
        (ExceptionKind.PRECONDITION, "revision_mismatch"),
    }
)
"""The refusals the race is meant to produce; anything else is a defect wearing their clothes."""


class Contract(DocWithTemporal):
    employee_id: str
    hours: int = 0


class ContractCreate(CreateCmdWithTemporalFields):
    employee_id: str
    hours: int = 0


class ContractUpdate(BaseDTO):
    hours: int | None = None


class ContractRead(ReadDocument):
    employee_id: str
    hours: int = 0
    valid_from: date
    valid_to: date | None = None


NO_OVERLAP: Final = NonOverlapping(
    key=("employee_id",), period=("valid_from", "valid_to"), bounds="[]"
)


def _spec(
    *, governed: bool
) -> DocumentSpec[ContractRead, Contract, ContractCreate, ContractUpdate]:
    return DocumentSpec[ContractRead, Contract, ContractCreate, ContractUpdate](
        name="contracts",
        read=ContractRead,
        write=DocumentWriteTypes(
            domain=Contract, create_cmd=ContractCreate, update_cmd=ContractUpdate
        ),
        guarantees=(NO_OVERLAP,) if governed else (),
    )


class AddContract(BaseModel):
    """One writer's attempt: a period starting on an offset it chose."""

    offset: int
    length: int


# ....................... #


@attrs.define(slots=True, kw_only=True)
class _Add(Handler[AddContract, None]):
    """Add a period for the one employee, recording what landed.

    Nothing is recorded: the property is read off the rows the workload left behind, which is
    what the guarantee is about. A history of attempted writes says nothing about which of them
    survived.
    """

    ctx: ExecutionContext
    spec: DocumentSpec[ContractRead, Contract, ContractCreate, ContractUpdate]
    unexpected: list[str]

    async def __call__(self, args: AddContract) -> None:
        start = EPOCH + timedelta(days=args.offset)
        end = start + timedelta(days=args.length)

        try:
            row = await self.ctx.doc.command(self.spec).create(
                ContractCreate(
                    employee_id=EMPLOYEE, hours=args.length, valid_from=start, valid_to=end
                )
            )

        except CoreException as caught:
            # Only the losing side of the race is expected here; anything else is a defect
            # that would otherwise read as the guarantee doing its job.
            if (caught.kind, caught.code) not in _EXPECTED_REFUSALS:
                self.unexpected.append(f"{caught.kind}/{caught.code}")

            return

        _ = row


# ....................... #


def _no_overlapping_rows(
    state: MockState,
    spec: DocumentSpec[ContractRead, Contract, ContractCreate, ContractUpdate],
):
    """No two stored periods for one employee overlap, read off the store.

    Over the rows rather than the recorded calls, as the sibling correction simulation is: a
    guarantee is about what is stored, and a history of refusals says nothing about how many
    rows survived. The comparison is :meth:`~forze.base.primitives.Period.overlaps`, the same
    predicate the store enforced and the reads use.
    """

    def check(history: History) -> list[Violation]:
        _ = history
        rows = state.documents.get(str(spec.name)) or {}
        periods: dict[Any, list[Period[date]]] = {}

        for row in rows.values():
            periods.setdefault(row.get("employee_id"), []).append(
                Period(row["valid_from"], row.get("valid_to"), "[]")
            )

        found: list[Violation] = []

        for owner, held in periods.items():
            for i, left in enumerate(held):
                for right in held[i + 1 :]:
                    if not left.overlaps(right):
                        continue

                    found.append(
                        Violation(
                            invariant="no_overlapping_rows",
                            message=f"{owner!r} holds {left} and {right}, which overlap",
                            events=(),
                        )
                    )

        return found

    return named("no_overlapping_rows", check)


# ....................... #


def _registry(
    spec: DocumentSpec[ContractRead, Contract, ContractCreate, ContractUpdate],
    *,
    unexpected: list[str],
) -> Any:
    plan = OperationPlan().bind_tx().set_route(_TX).finish(deep=False)

    return OperationRegistry(
        handlers={"add": lambda ctx: _Add(ctx=ctx, spec=spec, unexpected=unexpected)},
        plans={"add": plan},
        descriptors={
            "add": OperationDescriptor(
                input_type=AddContract,
                output_type=None,
                description="add a contract period",
            ),
        },
    ).freeze()


def _run(*, governed: bool) -> tuple[Any, MockState, list[str]]:
    spec = _spec(governed=governed)
    state = MockState()
    unexpected: list[str] = []

    def deps() -> Sequence[DepsModule]:
        return [MockDepsModule(state=state)]

    simulation = Simulation(
        operations=_registry(spec, unexpected=unexpected),
        deps=deps,
        invariants=[_no_overlapping_rows(state, spec), inv.no_unexpected_error()],
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
                op="add",
                # Deliberately narrow: overlapping windows are the point, so the offsets have
                # to collide often rather than spread out into a tidy timeline.
                inputs=lambda rng: AddContract(offset=rng.randrange(6), length=rng.randrange(1, 5)),
            )
        ],
    )

    return report, state, unexpected


# ----------------------- #


class TestTwoWritersOneEmployee:
    def test_no_two_stored_periods_overlap(self) -> None:
        report, state, unexpected = _run(governed=True)

        assert report is None, f"the guarantee should have held, got {report}"
        assert unexpected == [], (
            f"the workload was refused for reasons the race cannot produce: {unexpected}"
        )

        # Not vacuous: a run where every write refused would also leave nothing overlapping.
        rows = state.documents["contracts"]

        assert len(rows) > 1, "fewer than two periods landed — the green run proves nothing"

    def test_the_ungoverned_spec_leaves_an_overlap(self) -> None:
        # The contrast. If this passed, the writers never collided and the run above attests
        # nothing — and it is also the defect the declaration exists to prevent, reproduced.
        report, _, unexpected = _run(governed=False)

        assert unexpected == [], (
            f"the workload was refused for reasons the race cannot produce: {unexpected}"
        )
        assert report is not None, (
            "the ungoverned spec must leave two overlapping periods — if it does not, the "
            "workload is not concurrent enough for the governed run to attest anything"
        )
        assert any(v.invariant == "no_overlapping_rows" for v in report.violations)

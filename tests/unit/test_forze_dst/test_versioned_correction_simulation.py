"""Two corrections of one fact race, and only one of them may leave a current version.

The kit batteries drive `correct` one caller at a time, which proves the command does what it
says and nothing about the case the guarantee exists for. Here two writers correct the same fact
under an interleaving scheduler, and the property is read off the rows the workload left behind
rather than off the calls — a history of refusals says nothing about how many rows survived.

The contrast is the point of the file. The same workload against a spec **without** the
guarantees, correcting the way the pattern is hand-rolled, must leave two current rows; otherwise
the governed run is green because the writers never collided and the simulation attests nothing.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from typing import Any

import attrs
from pydantic import BaseModel

from forze.application.contracts.deps import DepsModule
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow, uuid7
from forze.domain.models import ReadDocument
from forze_dst import OperationCase, Simulation, SimulationConfig, Strategy
from forze_dst import invariants as inv
from forze_dst.oracle.invariants import Violation, named
from forze_dst.oracle.recorder import History
from forze_kits.aggregates.versioned import (
    ONE_CURRENT_VERSION,
    ONE_SUCCESSOR,
    CorrectDocument,
    CorrectDocumentDTO,
)
from forze_kits.domain.versioned import (
    CorrectionDoc,
    CreateCmdWithVersioningFields,
    CreateCorrectionCmd,
    DocWithVersioning,
    UpdateCmdWithVersioning,
)
from forze_mock import MockDepsModule, MockState

# ----------------------- #

_TX = "mock"
METER = "the-one-meter"


class Reading(DocWithVersioning):
    meter: str
    kwh: int = 0


class ReadingCreate(CreateCmdWithVersioningFields):
    meter: str
    kwh: int = 0


class ReadingUpdate(UpdateCmdWithVersioning):
    meter: str | None = None
    kwh: int | None = None


class ReadingRead(ReadDocument):
    meter: str
    kwh: int = 0
    root_id: Any = None
    version: int = 1
    supersedes_id: Any = None
    is_current: bool = True
    superseded_at: Any = None


class CorrectionRead(ReadDocument):
    root_id: Any = None
    from_id: Any = None
    to_id: Any = None
    actor_id: Any = None
    reason: str = ""


def _spec(*, governed: bool) -> DocumentSpec[ReadingRead, Reading, ReadingCreate, ReadingUpdate]:
    return DocumentSpec[ReadingRead, Reading, ReadingCreate, ReadingUpdate](
        name="readings",
        read=ReadingRead,
        write=DocumentWriteTypes(
            domain=Reading, create_cmd=ReadingCreate, update_cmd=ReadingUpdate
        ),
        guarantees=(ONE_CURRENT_VERSION, ONE_SUCCESSOR) if governed else (),
    )


CORRECTIONS = DocumentSpec(
    name="reading_corrections",
    read=CorrectionRead,
    write=DocumentWriteTypes(domain=CorrectionDoc, create_cmd=CreateCorrectionCmd),
)


class Correct(BaseModel):
    kwh: int


# ....................... #


def _seed(state: MockState, spec: DocumentSpec[Any, Any, Any, Any]) -> None:
    """Put version 1 of the fact in the store, so both arms start from the same place."""

    fact_id = uuid7()
    state.documents[str(spec.name)] = {
        fact_id: {
            "id": str(fact_id),
            "rev": 1,
            "created_at": utcnow(),
            "last_update_at": utcnow(),
            "meter": METER,
            "kwh": 0,
            "root_id": str(fact_id),
            "version": 1,
            "supersedes_id": None,
            "is_current": True,
            "superseded_at": None,
        }
    }


# ....................... #


@attrs.define(slots=True, kw_only=True)
class _Correct(Handler[Correct, None]):
    """Correct the fact's current version, either through the kit's command or by hand.

    Both arms read the current version themselves rather than being handed one, which is what
    makes them race: two writers can read the same current row before either has retired it.

    A refusal is a success here — the guarantee doing its job — so ``CoreException`` is swallowed
    and the invariant judges the rows, not the calls.
    """

    ctx: ExecutionContext
    spec: DocumentSpec[ReadingRead, Reading, ReadingCreate, ReadingUpdate]
    through_kit: bool

    async def __call__(self, args: Correct) -> None:
        query = self.ctx.doc.query(self.spec)
        cmd = self.ctx.doc.command(self.spec)

        try:
            page = await query.find_many(filters={"$values": {"is_current": True}})

            if not page.hits:
                return

            current = page.hits[0]

            if self.through_kit:
                await CorrectDocument(
                    doc=cmd,
                    query=query,
                    corrections=self.ctx.doc.command(CORRECTIONS),
                    create_cmd=ReadingCreate,
                    actor=self.ctx.inv_ctx.get_authn,
                )(
                    CorrectDocumentDTO(
                        id=current.id,
                        expected_version=current.version,
                        dto=ReadingUpdate(kwh=args.kwh),
                        reason="meter misread",
                    )
                )

                return

            # The hand-rolled shape the origin application wrote: insert the successor, then
            # retire the predecessor, with nothing making the pair atomic against a second
            # writer that read the same current row.
            await cmd.create(
                ReadingCreate(
                    meter=METER,
                    kwh=args.kwh,
                    root_id=current.root_id,
                    version=current.version + 1,
                    supersedes_id=current.id,
                ),
                id=uuid7(),
            )
            await cmd.update(
                pk=current.id,
                rev=current.rev,
                dto=ReadingUpdate(is_current=False, superseded_at=utcnow()),
            )

        except CoreException:
            return


# ....................... #


def _one_current_version(state: MockState, spec: DocumentSpec[Any, Any, Any, Any]):
    """At most one current row per fact, read off the store the workload left behind.

    Over the rows rather than the recorded calls: a guarantee is about what is stored, and a
    history of refusals says nothing about how many rows survived.
    """

    def check(history: History) -> list[Violation]:
        _ = history
        rows = state.documents.get(str(spec.name)) or {}
        counts = Counter(
            row.get("root_id") for row in rows.values() if row.get("is_current") is True
        )
        offenders = {root: n for root, n in counts.items() if n > 1}

        if not offenders:
            return []

        return [
            Violation(
                invariant="single_current_head",
                message=f"{offenders} — more than one current version of a fact",
                events=(),
            )
        ]

    return named("single_current_head", check)


# ....................... #


def _registry(
    spec: DocumentSpec[ReadingRead, Reading, ReadingCreate, ReadingUpdate],
    *,
    through_kit: bool,
) -> Any:
    plan = OperationPlan().bind_tx().set_route(_TX).finish(deep=False)

    return OperationRegistry(
        handlers={"correct": lambda ctx: _Correct(ctx=ctx, spec=spec, through_kit=through_kit)},
        plans={"correct": plan},
        descriptors={
            "correct": OperationDescriptor(
                input_type=Correct,
                output_type=None,
                description="correct the meter reading",
            ),
        },
    ).freeze()


def _run(*, governed: bool) -> tuple[Any, MockState]:
    spec = _spec(governed=governed)
    state = MockState()
    _seed(state, spec)

    def deps() -> Sequence[DepsModule]:
        return [MockDepsModule(state=state)]

    simulation = Simulation(
        operations=_registry(spec, through_kit=governed),
        deps=deps,
        invariants=[_one_current_version(state, spec), inv.no_unexpected_error()],
    )

    report = simulation.run(
        SimulationConfig(
            strategy=Strategy.OP_CASE,
            count=4,
            act_count=6,
            concurrency=4,
            seeds=range(3),
        ),
        cases=[OperationCase(op="correct", inputs=lambda rng: Correct(kwh=rng.randrange(1000)))],
    )

    return report, state


# ----------------------- #


class TestTwoCorrectionsOfOneFact:
    def test_only_one_leaves_a_current_version(self) -> None:
        report, state = _run(governed=True)

        assert report is None, f"the guarantee should have held, got {report}"

        # Not vacuous: a run where every correction refused would also leave one current row.
        # The chain has to have grown, or this leg says nothing about corrections at all.
        rows = state.documents["readings"]
        assert len(rows) > 1, "no correction landed — the green run proves nothing"
        assert max(int(row["version"]) for row in rows.values()) > 1

    def test_the_hand_rolled_shape_forks_the_chain(self) -> None:
        # The contrast. If this passed, the writers never raced and the run above proved nothing
        # — and it is also the defect the kit exists to replace, reproduced.
        report, _ = _run(governed=False)

        assert report is not None, (
            "the ungoverned spec must leave two current versions — if it does not, the workload "
            "is not concurrent enough for the governed run to attest anything"
        )
        assert any(v.invariant == "single_current_head" for v in report.violations)

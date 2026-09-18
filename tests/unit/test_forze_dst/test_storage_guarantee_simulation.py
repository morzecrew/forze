"""A declared storage guarantee holds under a concurrent workload — and is needed.

The unit batteries drive the guarantee straight: call the command twice, expect a refusal.
That proves the check fires; it does not prove the property *holds* when several tasks write
the same tuple at once, which is the case the guarantee exists for and the one a reader
doubts.

So the workload here races writers for one root, and the invariant reads the resulting store
rather than the calls: at most one current row per fact, whatever order the writers landed in.

The contrast is the point of the file. The same workload against the same spec **minus the
guarantee** must violate it — otherwise the green run above is green because the workload
never collided, and the whole simulation attests nothing. A test that only asserts the
governed case passes is a test that would keep passing with the enforcement deleted.
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
from forze.application.contracts.guarantees import UniqueTogether
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst import OperationCase, Simulation, SimulationConfig, Strategy
from forze_dst import invariants as inv
from forze_dst.oracle.invariants import Violation, named
from forze_dst.oracle.recorder import History
from forze_mock import MockDepsModule, MockState

# ----------------------- #

ONE_CURRENT = UniqueTogether(fields=("root_id",), where={"$values": {"is_current": True}})

ROOT = "the-one-fact"


class _Fact(Document):
    root_id: str
    is_current: bool = True


class _FactRead(ReadDocument):
    root_id: str
    is_current: bool = True


class _FactCreate(CreateDocumentCmd):
    root_id: str
    is_current: bool = True


class _FactUpdate(BaseDTO):
    is_current: bool | None = None


def _spec(*, governed: bool) -> DocumentSpec[_FactRead, _Fact, _FactCreate, _FactUpdate]:
    return DocumentSpec[_FactRead, _Fact, _FactCreate, _FactUpdate](
        name="fact",
        read=_FactRead,
        write=DocumentWriteTypes(domain=_Fact, create_cmd=_FactCreate, update_cmd=_FactUpdate),
        guarantees=(ONE_CURRENT,) if governed else (),
    )


class Supersede(BaseModel):
    pass


@attrs.define(slots=True, kw_only=True)
class _Supersede(Handler[Supersede, None]):
    """Write a new current row for one root, the way a correction would.

    Deliberately *not* read-modify-write under a lock: this is the naive implementation a kit
    ships before anyone thinks about concurrency, and the store is what has to catch it. A
    refusal is a success here — the guarantee doing its job — so `CoreException` is swallowed
    and the invariant judges the rows, not the calls.
    """

    ctx: ExecutionContext
    spec: DocumentSpec[_FactRead, _Fact, _FactCreate, _FactUpdate]

    async def __call__(self, args: Supersede) -> None:
        _ = args

        try:
            await self.ctx.doc.command(self.spec).create(_FactCreate(root_id=ROOT))

        except CoreException:
            return


def _registry(spec: DocumentSpec[Any, Any, Any, Any]) -> Any:
    return OperationRegistry(
        handlers={"supersede": lambda ctx: _Supersede(ctx=ctx, spec=spec)},
        descriptors={
            "supersede": OperationDescriptor(
                input_type=Supersede,
                output_type=None,
                description="publish a new current row for one fact",
            ),
        },
    ).freeze()


def _one_current_row(state: MockState, spec: DocumentSpec[Any, Any, Any, Any]):
    """The property, read off the store the workload left behind.

    Over the rows rather than over the recorded calls: a guarantee is about what is stored, and
    a history of refusals says nothing about how many rows survived.
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
                invariant="one_current_row",
                message=f"{offenders} — more than one current row for a fact",
                events=(),
            )
        ]

    return named("one_current_row", check)


def _run(*, governed: bool) -> tuple[Any, MockState]:
    spec = _spec(governed=governed)
    state = MockState()

    def deps() -> Sequence[DepsModule]:
        return [MockDepsModule(state=state)]

    simulation = Simulation(
        operations=_registry(spec),
        deps=deps,
        invariants=[_one_current_row(state, spec), inv.no_unexpected_error()],
    )

    report = simulation.run(
        SimulationConfig(
            strategy=Strategy.OP_CASE,
            count=4,
            act_count=6,
            concurrency=4,
            seeds=range(3),
        ),
        cases=[OperationCase(op="supersede", inputs=lambda _rng: Supersede())],
    )

    return report, state


# ----------------------- #


class TestTheGuaranteeHoldsUnderConcurrency:
    def test_racing_writers_leave_one_current_row(self) -> None:
        report, _state = _run(governed=True)

        assert report is None, f"the guarantee should have held, got {report}"

    def test_the_same_workload_breaks_it_without_the_guarantee(self) -> None:
        # The contrast that makes the leg above mean something. If this passed, the workload
        # never raced and the governed run proved nothing about the guarantee.
        report, _ = _run(governed=False)

        assert report is not None, (
            "the ungoverned spec must produce two current rows — if it does not, the workload "
            "is not concurrent enough for the governed run to attest anything"
        )
        assert any(v.invariant == "one_current_row" for v in report.violations)

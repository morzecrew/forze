"""One audit row per committed effect, however the attempts that produced it failed and retried.

Edits race over a few notes under an operation-level retry, with faults injected on the business
write and on the audit write. An attempt that fails after its row is written rolls back, taking
the row with it, and the retry writes the one that commits. The invariant reads the recorded
trace: every committed transaction that wrote a note carries exactly one audit row.

Two contrasts must break it, or the bound run attests nothing: an action bound twice records two
rows per effect, and an admitted row written after the transaction instead of inside it leaves
the effect's transaction without one.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Sequence
from typing import Any, Final
from uuid import UUID

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.audit import AuditSpec
from forze.application.contracts.deps import DepsModule
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.hooks.audit import Audited
from forze.application.hooks.resilience import ResilienceWrap
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze.testing import context_from_modules
from forze_dst import OperationCase, Simulation, SimulationConfig, Strategy
from forze_dst import invariants as inv
from forze_dst.faults import FaultPolicy, FaultRule
from forze_dst.oracle.invariants import Violation, named
from forze_dst.oracle.recorder import History
from forze_kits.integrations.audit import AuditDepsModule, audit_record_spec
from forze_mock import MockDepsModule, MockState

# ----------------------- #

NOTES: Final = tuple(UUID(f"00000000-0000-4000-8000-00000000000{n}") for n in range(1, 4))
TRAIL: Final = audit_record_spec()
EDIT: Final = AuditSpec(action="note.edit")


class _Note(Document):
    label: str


class _NoteCreate(BaseDTO):
    label: str


class _NoteUpdate(BaseDTO):
    label: str | None = None


class _NoteRead(ReadDocument):
    label: str


NOTE_SPEC = DocumentSpec(
    name="notes",
    read=_NoteRead,
    write=DocumentWriteTypes(domain=_Note, create_cmd=_NoteCreate, update_cmd=_NoteUpdate),
)


class Edit(BaseModel):
    note: int
    label: str


@attrs.define(slots=True, kw_only=True)
class _Edit:
    ctx: ExecutionContext

    async def __call__(self, args: Edit) -> None:
        note = NOTES[args.note]
        current = await self.ctx.document.query(NOTE_SPEC).get(note)
        await self.ctx.document.command(NOTE_SPEC).update(
            note, current.rev, _NoteUpdate(label=args.label)
        )


# ....................... #


def _observed(tally: Counter[str]) -> Any:
    """Counts what the sweep did, so a green run can be told from one where nothing happened.

    Not an invariant: it never reports. Summed across runs on purpose — the question is whether
    the sweep ever produced the shapes the invariant exists for, not which run did.
    """

    def check(history: History) -> list[Violation]:
        committed: set[Any] = set()
        rows: set[Any] = set()

        for event in history.of_kind("trace"):
            fields = event.fields
            tx_id = fields.get("tx_id")

            if fields.get("trace_domain") == "tx" and fields.get("op") == "exit":
                if fields.get("outcome") == "commit":
                    committed.add(tx_id)

            elif fields.get("trace_domain") == "resilience" and fields.get("op") == "retry_attempt":
                tally["retries"] += 1

            elif fields.get("route") == TRAIL.name and fields.get("phase") == "command":
                rows.add(tx_id)

        tally["rolled back with a row"] += len(rows - committed - {None})
        tally["committed with a row"] += len(rows & committed)
        tally["faults"] += len(history.of_kind("fault"))

        return []

    return named("observed", check)


def _run(*, bind: str) -> tuple[Any, Counter[str]]:
    state = MockState()
    tally: Counter[str] = Counter()

    seed = context_from_modules(MockDepsModule(state=state))

    for note in NOTES:
        asyncio.run(seed.document.command(NOTE_SPEC).create(_NoteCreate(label="L0"), id=note))

    def deps() -> Sequence[DepsModule]:
        # The mock's resilience executor passes through by default; the retry leg needs the one
        # that retries.
        return [MockDepsModule(state=state, resilience="real"), AuditDepsModule(tx_route="mock")]

    binder = (
        OperationRegistry(
            handlers={"edit": lambda ctx: _Edit(ctx=ctx)},
            descriptors={"edit": OperationDescriptor(input_type=Edit, output_type=None)},
        )
        .bind("edit")
        .bind_tx()
        .set_route("mock")
        .finish()
        .bind_outer()
        .wrap(ResilienceWrap(policy="transient").to_step())
        .finish()
    )
    audited = Audited(spec=EDIT)

    if bind == "once":
        binder = audited.bind(binder)
    elif bind == "twice":
        binder = audited.bind(audited.bind(binder), step_id="audit.again")
    else:
        # The admitted row written by the outer hook, after the transaction has closed.
        binder = audited.bind(binder, transactional=False)

    simulation = Simulation(
        operations=binder.finish().freeze(),
        deps=deps,
        invariants=[
            inv.audit_row_per_effect(audit_route=TRAIL.name, effect_routes=[NOTE_SPEC.name]),
            inv.no_unexpected_error(),
            _observed(tally),
        ],
    )
    report = simulation.run(
        SimulationConfig(
            strategy=Strategy.OP_CASE,
            count=12,
            act_count=8,
            concurrency=3,
            seeds=range(4),
            faults=FaultPolicy(
                rules=(
                    FaultRule(surface="document_command", route=TRAIL.name, error=0.3),
                    FaultRule(surface="document_command", route=NOTE_SPEC.name, error=0.2),
                )
            ),
        ),
        cases=[
            OperationCase(
                op="edit",
                inputs=lambda rng: Edit(
                    note=rng.randrange(len(NOTES)), label=f"L{rng.randrange(9)}"
                ),
            ),
        ],
    )

    return report, tally


# ----------------------- #


class TestOneRowPerEffect:
    def test_retried_attempts_leave_one_row_per_committed_effect(self) -> None:
        report, tally = _run(bind="once")

        assert report is None, f"every committed effect should carry one row, got {report}"
        # Not vacuous: faults fired and were retried, attempts that had written their row rolled
        # back, and effects committed with their rows. An invariant over a run with none of these
        # attests nothing.
        assert tally["faults"] > 0 and tally["retries"] > 0, tally
        assert tally["rolled back with a row"] > 0, tally
        assert tally["committed with a row"] > 0, tally

    @pytest.mark.parametrize(
        ("bind", "why"),
        [("twice", "committed 2 audit rows"), ("after", "with no audit row")],
    )
    def test_a_row_outside_the_effects_transaction_breaks_it(self, bind: str, why: str) -> None:
        report, _ = _run(bind=bind)

        assert report is not None, f"binding {bind!r} should break the invariant"
        assert any(
            v.invariant == "audit_row_per_effect" and why in v.message for v in report.violations
        ), [v.message for v in report.violations]


class TestTheDeclaration:
    def test_it_needs_an_effect_route_other_than_the_trail(self) -> None:
        with pytest.raises(ValueError):
            inv.audit_row_per_effect(audit_route=TRAIL.name, effect_routes=[])

        with pytest.raises(ValueError):
            inv.audit_row_per_effect(audit_route=TRAIL.name, effect_routes=[TRAIL.name])

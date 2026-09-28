"""A confirmation lands only on the state its preview showed, however edits interleave.

Each flow previews a quote, yields — so concurrent edits can land between preview and
confirmation, the gap the binding exists for — and confirms. The invariant reads the recorded
history: every committed confirmation carried the same label its own preview showed. The run
without the binding is the contrast: it must violate the invariant, or the workload never
raced and the bound run attests nothing.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any, Final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.deps import DepsModule
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import ExecutionContext
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze.testing import context_from_modules
from forze_dst import OperationCase, Simulation, SimulationConfig, Strategy
from forze_dst import invariants as inv
from forze_dst.markers import record_event
from forze_dst.oracle.invariants import Violation, named
from forze_dst.oracle.recorder import History
from forze_kits.domain.preview import PREVIEW_CHANGED, PreviewBinding, ReviewedCommand
from forze_mock import MockDepsModule, MockState

# ----------------------- #

QUOTE_ID: Final = UUID("00000000-0000-4000-8000-00000000000a")
_EXPECTED: Final = frozenset({PREVIEW_CHANGED, "revision_mismatch"})
"""The refusals the race produces; anything else is a defect wearing their clothes."""


class _Quote(Document):
    label: str


class _QuoteCreate(BaseDTO):
    label: str


class _QuoteUpdate(BaseDTO):
    label: str | None = None


class _QuoteRead(ReadDocument):
    label: str


QUOTES = DocumentSpec(
    name="quotes",
    read=_QuoteRead,
    write=DocumentWriteTypes(domain=_Quote, create_cmd=_QuoteCreate, update_cmd=_QuoteUpdate),
)


class _Shown(BaseModel):
    label: str


class Flow(BaseModel):
    flow: int


class Edit(BaseModel):
    label: str


class _Confirm(ReviewedCommand):
    flow: int


async def _project(ctx: ExecutionContext, args: Any) -> _Shown:
    return _Shown(label=(await ctx.document.query(QUOTES).get(QUOTE_ID)).label)


BINDING: Final = PreviewBinding(name="quote", projector=_project)


# ....................... #


@attrs.define(slots=True, kw_only=True)
class _Confirmed:
    """The confirmation: records the label it committed against, inside its transaction."""

    ctx: ExecutionContext

    async def __call__(self, args: _Confirm) -> None:
        shown = await _project(self.ctx, args)
        record_event("confirmed", flow=args.flow, label=shown.label)


@attrs.define(slots=True, kw_only=True)
class _Flow:
    ctx: ExecutionContext
    registry: dict[str, Any]
    unexpected: list[str]
    outcomes: list[str]

    async def __call__(self, args: Flow) -> None:
        preview = await BINDING.reviewed(self.ctx, args)
        record_event("previewed", flow=args.flow, label=preview.data.label)

        # The gap between preview and confirmation, where another writer's edit lands.
        await asyncio.sleep(0)

        try:
            await run_operation(
                self.registry["frozen"],
                "confirm",
                _Confirm(flow=args.flow, fingerprint=preview.fingerprint),
                self.ctx,
            )
            self.outcomes.append("confirmed")

        except CoreException as caught:
            if caught.code not in _EXPECTED:
                self.unexpected.append(f"{caught.kind}/{caught.code}")

            self.outcomes.append(str(caught.code))


@attrs.define(slots=True, kw_only=True)
class _Edit:
    ctx: ExecutionContext
    unexpected: list[str]

    async def __call__(self, args: Edit) -> None:
        current = await self.ctx.document.query(QUOTES).get(QUOTE_ID)

        try:
            await self.ctx.document.command(QUOTES).update(
                QUOTE_ID, current.rev, _QuoteUpdate(label=args.label)
            )

        except CoreException as caught:
            if caught.code not in _EXPECTED:
                self.unexpected.append(f"{caught.kind}/{caught.code}")


# ....................... #


def _confirmed_what_was_shown():
    """Every committed confirmation carries the label its own preview showed.

    Read off the recorded history: the sweep runs the workload many times, and state kept
    outside the run answers about the wrong one.
    """

    def check(history: History) -> list[Violation]:
        shown: dict[int, str] = {}
        found: list[Violation] = []

        for event in history.events:
            if event.kind == "previewed":
                shown[int(event.fields["flow"])] = str(event.fields["label"])

            elif event.kind == "confirmed":
                flow, label = int(event.fields["flow"]), str(event.fields["label"])

                if shown.get(flow) != label:
                    found.append(
                        Violation(
                            invariant="confirmed_what_was_shown",
                            message=f"flow {flow} confirmed a state it was never shown",
                            events=(),
                        )
                    )

        return found

    return named("confirmed_what_was_shown", check)


def _run(*, bound: bool, count: int = 16, seeds: int = 4) -> tuple[Any, list[str], list[str]]:
    state = MockState()
    unexpected: list[str] = []
    outcomes: list[str] = []
    holder: dict[str, Any] = {}

    # The quote exists before the workload starts; the flows and edits race over it.
    seed_ctx = context_from_modules(MockDepsModule(state=state))
    asyncio.run(seed_ctx.document.command(QUOTES).create(_QuoteCreate(label="L0"), id=QUOTE_ID))

    def deps() -> Sequence[DepsModule]:
        return [MockDepsModule(state=state)]

    confirm = (
        OperationRegistry(
            handlers={
                "flow": lambda ctx: _Flow(
                    ctx=ctx, registry=holder, unexpected=unexpected, outcomes=outcomes
                ),
                "edit": lambda ctx: _Edit(ctx=ctx, unexpected=unexpected),
                "confirm": lambda ctx: _Confirmed(ctx=ctx),
            },
            descriptors={
                "flow": OperationDescriptor(input_type=Flow, output_type=None),
                "edit": OperationDescriptor(input_type=Edit, output_type=None),
                "confirm": OperationDescriptor(input_type=_Confirm, output_type=None),
            },
        )
        .bind("confirm")
        .bind_tx()
        .set_route("mock")
        .finish()
    )
    registry = (BINDING.bind(confirm) if bound else confirm).finish().freeze()
    holder["frozen"] = registry

    simulation = Simulation(
        operations=registry,
        deps=deps,
        invariants=[_confirmed_what_was_shown(), inv.no_unexpected_error()],
    )
    report = simulation.run(
        SimulationConfig(
            strategy=Strategy.OP_CASE,
            count=count,
            act_count=8,
            concurrency=4,
            seeds=range(seeds),
        ),
        cases=[
            OperationCase(op="flow", inputs=lambda rng: Flow(flow=rng.randrange(10**9))),
            OperationCase(op="edit", inputs=lambda rng: Edit(label=f"L{rng.randrange(1, 6)}")),
        ],
    )

    return report, unexpected, outcomes


# ----------------------- #


class TestConfirmingWhatWasShown:
    def test_the_binding_keeps_every_confirmation_on_its_preview(self) -> None:
        report, unexpected, outcomes = _run(bound=True)

        assert unexpected == [], f"refused for reasons the race cannot produce: {unexpected}"
        assert report is None, f"the binding should have held, got {report}"
        # This sweep broke the invariant while the binding left isolation at read committed: a
        # write landing between the check and the handler's read reached the handler.
        # Not vacuous: confirmations landed, and the check refused some — an invariant over
        # nothing, or over a run the check never had to act in, attests nothing.
        assert "confirmed" in outcomes and PREVIEW_CHANGED in outcomes, sorted(set(outcomes))

    def test_without_it_a_confirmation_lands_on_a_changed_state(self) -> None:
        # The contrast. If this passed, no edit ever landed between a preview and its
        # confirmation, and the bound run above attests nothing.
        report, unexpected, _ = _run(bound=False)

        assert unexpected == [], f"refused for reasons the race cannot produce: {unexpected}"
        assert report is not None, (
            "an unbound confirmation must be able to land after an edit — if it cannot, the "
            "workload is not concurrent enough for the bound run to attest anything"
        )
        assert any(v.invariant == "confirmed_what_was_shown" for v in report.violations)

"""``denial_bodies_identical`` — every refusal of an operation renders one response.

The invariant reads the envelope digest the operation's error terminal records, so the
end-to-end legs below run the same seeded workload three ways: without a posture (the foreign
and missing reads name their own ids — the invariant must fire, which also proves the workload
produced refusals at all), with the posture (it must hold), and with the posture plus a handler
that raises its own untagged not-found (it must fire again).
"""

from __future__ import annotations

import random
from typing import Any
from uuid import UUID

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.document import OwnedBy
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.base.exceptions import (
    DenialPosture,
    configure_denial_posture,
    current_denial_posture,
    exc,
)
from forze_dst import ModelState, Rule, Scenario, Simulation, SimulationConfig, Strategy
from forze_dst.invariants import check, denial_bodies_identical
from forze_dst.oracle.recorder import Event, History
from forze_mock import MockDepsModule
from tests.support.owned_reads_conformance import OwnedCreate, owned_spec

# ----------------------- #

NOTES = owned_spec("notes")
OWNER = UUID(int=1)
STRANGER = UUID(int=2)


def _op(seq: int, **fields: object) -> Event:
    return Event(seq=seq, kind="operation", at=0.0, fields=fields)


def _history(*events: Event) -> History:
    return History(seed=0, events=events)


class TestOverAHistory:
    def test_two_renderings_of_one_operation_are_flagged(self) -> None:
        history = _history(
            _op(0, op="read", outcome="failed", status=404, rendered="a"),
            _op(1, op="read", outcome="failed", status=404, rendered="b"),
        )

        (violation,) = check(history, [denial_bodies_identical("read")])

        assert violation.invariant == "denial_bodies_identical"
        assert len(violation.events) == 2

    def test_a_403_beside_a_404_is_two_renderings(self) -> None:
        history = _history(
            _op(0, op="read", outcome="failed", status=403, rendered="a"),
            _op(1, op="read", outcome="failed", status=404, rendered="a"),
        )

        (violation,) = check(history, [denial_bodies_identical()])

        assert "2 different responses" in violation.message

    def test_one_rendering_holds(self) -> None:
        history = _history(
            _op(0, op="read", outcome="failed", status=404, rendered="a"),
            _op(1, op="read", outcome="failed", status=404, rendered="a"),
            _op(2, op="read", outcome="ok"),
        )

        assert check(history, [denial_bodies_identical("read")]) == []

    def test_other_statuses_and_other_operations_are_not_refusals_of_this_one(self) -> None:
        history = _history(
            _op(0, op="read", outcome="failed", status=404, rendered="a"),
            _op(1, op="read", outcome="failed", status=409, rendered="b"),
            _op(2, op="write", outcome="failed", status=404, rendered="c"),
        )

        assert check(history, [denial_bodies_identical("read")]) == []


# ....................... #


class ReadCmd(BaseModel):
    pk: UUID
    reader: UUID


@attrs.define(slots=True, kw_only=True)
class _Open(Handler[None, UUID]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> UUID:
        return (await self.ctx.doc.command(NOTES).create(OwnedCreate(owner_id=OWNER))).id


@attrs.define(slots=True, kw_only=True)
class _Read(Handler[ReadCmd, Any]):
    ctx: ExecutionContext
    leaks: bool = False

    async def __call__(self, args: ReadCmd) -> Any:
        if self.leaks and args.reader == STRANGER:
            # A handler's own not-found, naming no resource type: the posture cannot see it.
            raise exc.not_found(f"Note {args.pk} is not yours")

        return await self.ctx.doc.query(NOTES).get(
            args.pk,
            owned_by=OwnedBy(field="owner_id", value=args.reader),
        )


def _registry(*, leaks: bool) -> FrozenOperationRegistry:
    return OperationRegistry(
        handlers={
            "open": lambda ctx: _Open(ctx=ctx),
            "read": lambda ctx: _Read(ctx=ctx, leaks=leaks),
        },
        descriptors={
            "open": OperationDescriptor(input_type=None, output_type=None, description="x"),
            "read": OperationDescriptor(input_type=ReadCmd, output_type=None, description="x"),
        },
    ).freeze()


def _read(state: ModelState, rng: random.Random) -> ReadCmd:
    """The owner's row, read by its owner or a stranger — or an id that was never created."""

    pk = state.pick("note", rng) if rng.random() < 0.7 else UUID(int=rng.getrandbits(128))
    return ReadCmd(pk=pk, reader=rng.choice([OWNER, STRANGER]))


_SCENARIO = Scenario(
    state=ModelState,
    arrange=(Rule(op="open", produces="note"),),
    act=(Rule(op="read", requires=("note",), arg=_read),),
)


def _run(*, leaks: bool = False) -> Any:
    simulation = Simulation(
        operations=_registry(leaks=leaks),
        deps=lambda: MockDepsModule(),
        invariants=[denial_bodies_identical("read")],
    )

    return simulation.run(
        SimulationConfig(strategy=Strategy.SCENARIO, act_count=12, concurrency=1, seeds=range(3)),
        scenario=_SCENARIO,
    )


@pytest.fixture
def posture() -> Any:
    previous = current_denial_posture()

    yield configure_denial_posture

    configure_denial_posture(previous)


class TestEndToEnd:
    def test_without_the_posture_each_refusal_names_its_own_row(self, posture: Any) -> None:
        posture(DenialPosture())

        report = _run()

        assert report is not None
        assert report.violations[0].invariant == "denial_bodies_identical"

    def test_with_the_posture_every_refusal_is_one_response(self, posture: Any) -> None:
        posture(DenialPosture(mode="non_disclosing", resource_types=frozenset({"notes"})))

        assert _run() is None

    def test_a_handlers_own_not_found_is_a_second_response(self, posture: Any) -> None:
        posture(DenialPosture(mode="non_disclosing", resource_types=frozenset({"notes"})))

        report = _run(leaks=True)

        assert report is not None
        assert report.violations[0].invariant == "denial_bodies_identical"

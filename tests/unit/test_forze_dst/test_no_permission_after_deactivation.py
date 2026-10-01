"""``no_permission_after_deactivation`` — a derived permission closes when its state does.

The end-to-end legs run one seeded workload twice: a member writes to a ledger their membership
permits, and a deactivation lands somewhere in the run. With a provider that reads the membership
per decision the invariant holds; with one that remembers its first answer — the stale grant a
binding sync or a cross-request cache produces — it must fire, which also proves the workload
reaches writes after the deactivation at all.
"""

from __future__ import annotations

import random
from typing import Any
from uuid import UUID

import attrs
import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import AuthzSpec, DerivedPermissions
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.application.hooks.authz import authorize_action
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, Rule, Scenario, Simulation, SimulationConfig, Strategy
from forze_dst.invariants import check, no_permission_after_deactivation
from forze_dst.oracle.recorder import Event, History
from forze_mock import MockDepsModule
from forze_mock.adapters.identity import MockAuthzDecisionPort

pytestmark = pytest.mark.unit

# ----------------------- #


def _op(op: str, outcome: str, start: int, end: int) -> Event:
    return Event(
        seq=start,
        kind="operation",
        at=0.0,
        fields={"op": op, "outcome": outcome, "start_seq": start, "end_seq": end},
    )


def _history(*events: Event) -> History:
    return History(seed=0, events=events)


INVARIANT = no_permission_after_deactivation("deactivate", ["write"])


class TestOverAHistory:
    def test_a_success_started_after_the_deactivation_returned_is_flagged(self) -> None:
        history = _history(_op("deactivate", "ok", 1, 2), _op("write", "ok", 3, 4))

        (violation,) = check(history, [INVARIANT])

        assert violation.invariant == "no_permission_after_deactivation"
        assert [event.fields["op"] for event in violation.events] == ["deactivate", "write"]

    def test_an_operation_overlapping_the_deactivation_is_not_judged(self) -> None:
        history = _history(_op("deactivate", "ok", 1, 4), _op("write", "ok", 2, 5))

        assert check(history, [INVARIANT]) == []

    def test_a_refusal_after_it_holds(self) -> None:
        history = _history(_op("deactivate", "ok", 1, 2), _op("write", "failed", 3, 4))

        assert check(history, [INVARIANT]) == []

    def test_a_deactivation_that_failed_closes_nothing(self) -> None:
        history = _history(_op("deactivate", "failed", 1, 2), _op("write", "ok", 3, 4))

        assert check(history, [INVARIANT]) == []

    def test_the_first_deactivation_is_the_one_that_closes(self) -> None:
        history = _history(
            _op("deactivate", "ok", 1, 2),
            _op("write", "ok", 3, 4),
            _op("deactivate", "ok", 5, 6),
        )

        assert len(check(history, [INVARIANT])) == 1

    def test_other_operations_are_not_guarded(self) -> None:
        history = _history(_op("deactivate", "ok", 1, 2), _op("read", "ok", 3, 4))

        assert check(history, [INVARIANT]) == []

    @pytest.mark.parametrize(
        ("deactivate", "guarded"),
        [pytest.param("deactivate", [], id="none-guarded"), pytest.param("x", ["x"], id="both")],
    )
    def test_a_statement_that_judges_nothing_is_refused(
        self, deactivate: str, guarded: list[str]
    ) -> None:
        with pytest.raises(ValueError):
            no_permission_after_deactivation(deactivate, guarded)


# ....................... #

MEMBER = UUID(int=1)
WRITE = "ledger.write"
AUTHZ = AuthzSpec(name="main")


class Membership(Document):
    principal_id: UUID
    active: bool = True


class MembershipCreate(CreateDocumentCmd):
    principal_id: UUID


class MembershipUpdate(BaseDTO):
    active: bool | None = None


class Deactivate(BaseDTO):
    pk: UUID


class MembershipRead(ReadDocument):
    principal_id: UUID
    active: bool


MEMBERS = DocumentSpec(
    name="members",
    read=MembershipRead,
    write=DocumentWriteTypes(
        domain=Membership, create_cmd=MembershipCreate, update_cmd=MembershipUpdate
    ),
)


@attrs.define(slots=True, kw_only=True)
class _Membership:
    """Derives the ledger permission from the member's row — or, when *stale*, from the row as
    it first read it."""

    name: str = "members"
    keys: frozenset[str] = frozenset({WRITE})
    stale: bool = False
    remembered: dict[UUID, bool] = attrs.field(factory=dict)

    async def derive(self, principal_id: UUID, ctx: Any) -> DerivedPermissions:
        if not (self.stale and principal_id in self.remembered):
            page = await ctx.doc.query(MEMBERS).find_many(
                filters={"$values": {"principal_id": principal_id}}
            )
            self.remembered[principal_id] = any(row.active for row in page.hits)

        if self.remembered[principal_id]:
            return DerivedPermissions(granted=self.keys)

        return DerivedPermissions(denied=self.keys)


@attrs.define(slots=True, kw_only=True)
class _Join(Handler[None, UUID]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> UUID:
        return (await self.ctx.doc.command(MEMBERS).create(MembershipCreate(principal_id=MEMBER))).id


@attrs.define(slots=True, kw_only=True)
class _Deactivate(Handler[Deactivate, None]):
    ctx: ExecutionContext

    async def __call__(self, args: Deactivate) -> None:
        row = await self.ctx.doc.query(MEMBERS).get(args.pk)
        await self.ctx.doc.command(MEMBERS).update(
            pk=args.pk, rev=row.rev, dto=MembershipUpdate(active=False)
        )


@attrs.define(slots=True, kw_only=True)
class _Write(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        with self.ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=MEMBER)):
            await authorize_action(
                self.ctx, self.ctx.authz.decision(AUTHZ), WRITE, delegation_port=None
            )


def _registry() -> FrozenOperationRegistry:
    def described(input_type: Any) -> OperationDescriptor:
        return OperationDescriptor(input_type=input_type, output_type=None, description="x")

    return OperationRegistry(
        handlers={
            "join": lambda ctx: _Join(ctx=ctx),
            "deactivate": lambda ctx: _Deactivate(ctx=ctx),
            "write": lambda ctx: _Write(ctx=ctx),
        },
        descriptors={
            "join": described(None),
            "deactivate": described(Deactivate),
            "write": described(None),
        },
    ).freeze()


def _member(state: ModelState, rng: random.Random) -> Deactivate:
    return Deactivate(pk=state.pick("member", rng))


_SCENARIO = Scenario(
    state=ModelState,
    arrange=(Rule(op="join", produces="member"),),
    act=(
        Rule(op="write", weight=4.0),
        Rule(op="deactivate", requires=("member",), arg=_member),
    ),
)


def _run(
    *, stale: bool = False, provider: bool = True, bound: bool = False, concurrency: int = 1
) -> Any:
    def deps() -> MockDepsModule:
        module = MockDepsModule(
            permission_providers=(_Membership(stale=stale),) if provider else ()
        )

        if bound:
            # A catalog binding of the key, as a sync from the membership table leaves it.
            MockAuthzDecisionPort(state=module.state).seed_grant(MEMBER, WRITE)

        return module

    simulation = Simulation(
        operations=_registry(),
        deps=deps,
        invariants=[no_permission_after_deactivation("deactivate", ["write"])],
    )

    return simulation.run(
        SimulationConfig(
            strategy=Strategy.SCENARIO, act_count=12, concurrency=concurrency, seeds=range(5)
        ),
        scenario=_SCENARIO,
    )


class TestEndToEnd:
    def test_a_provider_reading_the_row_per_decision_closes_with_it(self) -> None:
        assert _run() is None

    def test_its_denial_closes_a_binding_the_row_left_behind(self) -> None:
        assert _run(bound=True) is None

    def test_it_holds_when_writes_overlap_the_deactivation(self) -> None:
        assert _run(concurrency=3) is None

    def test_a_binding_nobody_revoked_is_caught(self) -> None:
        report = _run(provider=False, bound=True)

        assert report is not None
        assert report.violations[0].invariant == "no_permission_after_deactivation"

    def test_a_provider_remembering_its_first_answer_is_caught(self) -> None:
        report = _run(stale=True)

        assert report is not None
        assert report.violations[0].invariant == "no_permission_after_deactivation"

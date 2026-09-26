"""Two principals, one idempotency key, submitted concurrently.

A client generates its idempotency key, so two principals picking the same one is ordinary. What
the scope promises is that they never meet: each executes its own operation once and replays its
own record, and nobody is ever handed another principal's stored result.

The contrast is the file's point: the same workload against a store with no principal scope
serves one principal's record to the other, which is what makes the scoped run mean anything.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Sequence
from typing import Any, Final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.deps import DepsModule
from forze.application.contracts.execution import Handler
from forze.application.contracts.idempotency import IdempotencyRecord, IdempotencySpec
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException, ExceptionKind
from forze_dst import OperationCase, Simulation, SimulationConfig, Strategy
from forze_dst import invariants as inv
from forze_dst.markers import record_event
from forze_dst.oracle.invariants import Violation, named
from forze_dst.oracle.recorder import History
from forze_mock import MockDepsModule, MockState

# ----------------------- #

SPEC: Final = IdempotencySpec(name="submissions")
OP: Final = "submit"
KEY: Final = "the-same-key-everyone-picked"
HASH: Final = "identical-arguments"


class Submit(BaseModel):
    principal: int


# ....................... #


@attrs.define(slots=True, kw_only=True)
class _Submit(Handler[Submit, None]):
    """Claim the shared key as one principal: execute once, or replay what is stored."""

    ctx: ExecutionContext
    scoped: bool
    unexpected: list[str]

    async def __call__(self, args: Submit) -> None:
        principal = UUID(int=args.principal + 1)

        # The contrast binds no identity: every caller lands in one space, which is what a
        # store with no principal scope gave everyone.
        identity = AuthnIdentity(principal_id=principal) if self.scoped else None

        with self.ctx.inv_ctx.bind_identity(authn=identity):
            port = self.ctx.idempotency(SPEC)

            try:
                existing = await port.begin(OP, KEY, HASH)

            except CoreException as caught:
                # The same principal's claim still in flight: an ordinary outcome of the race.
                if caught.kind is not ExceptionKind.CONFLICT:
                    self.unexpected.append(f"{caught.kind}/{caught.code}")

                return

            if existing is not None:
                record_event("replay", principal=str(principal), holder=existing.result.decode())
                return

            record_event("effect", principal=str(principal))
            await asyncio.sleep(0)
            await port.commit(OP, KEY, HASH, IdempotencyRecord(result=str(principal).encode()))


# ....................... #


def _no_foreign_replay():
    """Nobody is served a record another principal stored — read off the run's own history."""

    def check(history: History) -> list[Violation]:
        return [
            Violation(
                invariant="no_foreign_replay",
                message=f"{event.fields['principal']} was served {event.fields['holder']}'s record",
                events=(),
            )
            for event in history.events
            if event.kind == "replay" and event.fields["principal"] != event.fields["holder"]
        ]

    return named("no_foreign_replay", check)


def _each_principal_executes_once():
    """The dedup still holds per principal: one key, one execution each."""

    def check(history: History) -> list[Violation]:
        effects = Counter(
            str(event.fields["principal"]) for event in history.events if event.kind == "effect"
        )

        return [
            Violation(
                invariant="each_principal_executes_once",
                message=f"{principal} executed {count} times under one key",
                events=(),
            )
            for principal, count in effects.items()
            if count > 1
        ]

    return named("each_principal_executes_once", check)


# ....................... #


def _run(*, scoped: bool) -> tuple[Any, MockState, list[str]]:
    state = MockState()
    unexpected: list[str] = []

    def deps() -> Sequence[DepsModule]:
        return [MockDepsModule(state=state)]

    registry = OperationRegistry(
        handlers={"submit": lambda ctx: _Submit(ctx=ctx, scoped=scoped, unexpected=unexpected)},
        plans={},
        descriptors={
            "submit": OperationDescriptor(
                input_type=Submit, output_type=None, description="submit under a shared key"
            ),
        },
    ).freeze()

    simulation = Simulation(
        operations=registry,
        deps=deps,
        invariants=[
            _no_foreign_replay(),
            _each_principal_executes_once(),
            inv.no_unexpected_error(),
        ],
    )

    report = simulation.run(
        SimulationConfig(
            strategy=Strategy.OP_CASE,
            count=4,
            act_count=6,
            concurrency=4,
            seeds=range(3),
        ),
        cases=[OperationCase(op="submit", inputs=lambda rng: Submit(principal=rng.randrange(2)))],
    )

    return report, state, unexpected


# ----------------------- #


class TestTwoPrincipalsOneKey:
    def test_the_scope_keeps_them_apart(self) -> None:
        report, state, unexpected = _run(scoped=True)

        assert unexpected == [], f"refused for reasons the race cannot produce: {unexpected}"
        assert report is None, f"the scope should have held, got {report}"

        # Not vacuous: two principals each executed and stored their own record.
        assert len(state.idempotency) == 2, state.idempotency.keys()

    def test_without_it_one_is_served_the_others_record(self) -> None:
        # The contrast. If this passed, the two principals never shared the key and the run
        # above attests nothing.
        report, _, unexpected = _run(scoped=False)

        assert unexpected == [], f"refused for reasons the race cannot produce: {unexpected}"
        assert report is not None, "an unscoped store must serve one principal the other's record"
        assert any(v.invariant == "no_foreign_replay" for v in report.violations)

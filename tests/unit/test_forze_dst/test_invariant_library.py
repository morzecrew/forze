"""Trace-driven invariant library (S4) — assertions sourced from the converged engine trace.

After convergence the engine trace is the single source of truth for operation outcome (``ok``
/ ``failed`` / ``error``), virtual-time interval, and the entity **key** each port call targets.
These invariants read that — no handler instrumentation:

* :func:`operation_succeeds` — named operations must reach ``ok`` (domain failures flagged too);
* :func:`completes_within` — an operation's virtual-time duration must stay within a budget;
* :func:`single_key_per_operation` — an operation must touch one entity key on a surface (the
  *wrong-entity* guard — fires from the trace key alone).
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
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst import (
    ModelState,
    Rule,
    Scenario,
    Simulation,
    SimulationConfig,
    Strategy,
    check,
    completes_within,
    operation_succeeds,
    single_key_per_operation,
)
from forze_dst.recorder import Event, History
from forze_mock import MockDepsModule

# ----------------------- #


def _op(seq: int, **fields: object) -> Event:
    return Event(seq=seq, kind="operation", at=0.0, fields=fields)


def _trace(seq: int, **fields: object) -> Event:
    return Event(seq=seq, kind="trace", at=0.0, fields={"trace_seq": seq, **fields})


def _history(*events: Event) -> History:
    return History(seed=0, events=events)


# ....................... #


class TestOperationSucceeds:
    def test_flags_domain_failure_and_bug(self) -> None:
        history = _history(
            _op(0, op="pay", outcome="ok"),
            _op(1, op="pay", outcome="failed"),
            _op(2, op="ship", outcome="error"),
        )
        # Named: only "pay" — the failed one is flagged, the bug on "ship" is not in scope.
        violations = check(history, [operation_succeeds("pay")])
        assert len(violations) == 1
        assert violations[0].events[0].fields["outcome"] == "failed"

    def test_all_operations_when_unscoped(self) -> None:
        history = _history(
            _op(0, op="pay", outcome="ok"),
            _op(1, op="ship", outcome="error"),
        )
        assert len(check(history, [operation_succeeds()])) == 1

    def test_holds_when_all_ok(self) -> None:
        history = _history(_op(0, op="pay", outcome="ok"))
        assert check(history, [operation_succeeds("pay")]) == []


class TestCompletesWithin:
    def test_flags_an_operation_over_budget(self) -> None:
        history = _history(_op(0, op="charge", invoked_at=0.0, returned_at=1.0))
        assert len(check(history, [completes_within("charge", 0.5)])) == 1
        assert check(history, [completes_within("charge", 2.0)]) == []

    def test_only_the_named_op(self) -> None:
        history = _history(_op(0, op="other", invoked_at=0.0, returned_at=10.0))
        assert check(history, [completes_within("charge", 0.5)]) == []


class TestSingleKeyPerOperation:
    def test_flags_an_operation_touching_two_keys(self) -> None:
        # One "pay" span (trace seq 0..5) whose document_command calls hit two distinct keys.
        history = _history(
            _op(9, op="pay", start_seq=0, end_seq=5),
            _trace(1, surface="document_command", key="A"),
            _trace(2, surface="document_command", key="B"),
        )
        violations = check(history, [single_key_per_operation("pay")])
        assert len(violations) == 1
        assert "A" in violations[0].message and "B" in violations[0].message

    def test_holds_for_one_key(self) -> None:
        history = _history(
            _op(9, op="pay", start_seq=0, end_seq=5),
            _trace(1, surface="document_command", key="A"),
            _trace(2, surface="document_command", key="A"),
        )
        assert check(history, [single_key_per_operation("pay")]) == []

    def test_ignores_calls_on_other_surfaces(self) -> None:
        history = _history(
            _op(9, op="pay", start_seq=0, end_seq=5),
            _trace(1, surface="document_command", key="A"),
            _trace(2, surface="document_query", key="B"),  # a read elsewhere — ignored
        )
        assert check(history, [single_key_per_operation("pay")]) == []


# ....................... #
# End-to-end: the wrong-entity bug fires from a real converged trace.


class Account(Document):
    balance: int = 0


class AccountCreate(CreateDocumentCmd):
    balance: int = 0


class AccountUpdate(BaseDTO):
    balance: int | None = None


class AccountRead(ReadDocument):
    balance: int


ACCOUNT_SPEC = DocumentSpec(
    name="accounts",
    read=AccountRead,
    write=DocumentWriteTypes(
        domain=Account, create_cmd=AccountCreate, update_cmd=AccountUpdate
    ),
)


class TransferCmd(BaseModel):
    account_id: UUID


@attrs.define(slots=True, kw_only=True)
class _OpenAccount(Handler[None, UUID]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> UUID:
        account = await self.ctx.document.command(ACCOUNT_SPEC).create(
            AccountCreate(balance=100)
        )
        return account.id


@attrs.define(slots=True, kw_only=True)
class _BuggyDebit(Handler[TransferCmd, None]):
    """Debits the intended account, then erroneously also writes a *second*, unrelated account
    — the wrong-entity bug. Both writes show in the trace under one operation span."""

    ctx: ExecutionContext

    async def __call__(self, args: TransferCmd) -> None:
        account = await self.ctx.document.query(ACCOUNT_SPEC).get(args.account_id)
        await self.ctx.document.command(ACCOUNT_SPEC).update(
            args.account_id, account.rev, AccountUpdate(balance=account.balance - 10)
        )
        # BUG: touch a different entity than the one the operation is about.
        stray = await self.ctx.document.command(ACCOUNT_SPEC).create(
            AccountCreate(balance=0)
        )
        await self.ctx.document.command(ACCOUNT_SPEC).update(
            stray.id, stray.rev, AccountUpdate(balance=-10)
        )


def _registry() -> OperationRegistry:
    return OperationRegistry(
        handlers={
            "open": lambda ctx: _OpenAccount(ctx=ctx),
            "debit": lambda ctx: _BuggyDebit(ctx=ctx),
        },
        descriptors={
            "open": OperationDescriptor(
                input_type=None, output_type=None, description="x"
            ),
            "debit": OperationDescriptor(
                input_type=TransferCmd, output_type=None, description="x"
            ),
        },
    ).freeze()


_SCENARIO = Scenario(
    state=ModelState,
    arrange=(Rule(op="open", produces="account"),),
    act=(
        Rule(
            op="debit",
            requires=("account",),
            arg=lambda state, rng: TransferCmd(account_id=state.pick("account", rng)),
        ),
    ),
)


class TestWrongEntityEndToEnd:
    def test_wrong_entity_write_is_caught_from_the_trace(self) -> None:
        sim = Simulation(
            operations=_registry(),
            deps=lambda: MockDepsModule(),
            invariants=[single_key_per_operation("debit")],
        )
        report = sim.run(
            SimulationConfig(
                strategy=Strategy.SCENARIO,
                act_count=1,
                concurrency=1,
                seeds=range(2),
            ),
            scenario=_SCENARIO,
        )
        assert report is not None
        assert report.violations[0].invariant == "single_key_per_operation"

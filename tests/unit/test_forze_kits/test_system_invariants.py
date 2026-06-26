"""SystemInvariant (RFC 0012 P1) — declare a cross-aggregate law, evaluate + detectively enforce it.

A ledger conservation law (the balances in a ledger sum to zero) and a payment cardinality law (at
most one captured payment per order) are the two canonical cross-record invariants the entity-level
``@invariant`` cannot express. These tests pin the kernel: ``evaluate`` reads the read-set aggregate
and reports whether the predicate held; ``enforce`` runs that check (immediately outside a
transaction, deferred to post-commit inside one) and raises on a violation — detective, not
preventive.
"""

from __future__ import annotations

import attrs
import pytest

from forze.application.contracts.invariants import (
    Count,
    ReadSet,
    Sum,
    SystemInvariant,
)
from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze.application.contracts.document import DocumentSpec
from forze_kits.invariants import enforce, enforce_preventive, evaluate
from forze_mock import MockDepsModule

from tests.support.execution_context import context_from_deps

# ----------------------- #


class Account(Document):
    ledger_id: str
    balance: int = 0


class AccountCreate(CreateDocumentCmd):
    ledger_id: str
    balance: int = 0


class AccountRead(ReadDocument):
    ledger_id: str
    balance: int = 0


ACCOUNTS = DocumentSpec(
    name="ledger_accounts",
    read=AccountRead,
    write={"domain": Account, "create_cmd": AccountCreate, "update_cmd": AccountCreate},
)


class Payment(Document):
    order_id: str
    status: str = "captured"


class PaymentCreate(CreateDocumentCmd):
    order_id: str
    status: str = "captured"


class PaymentRead(ReadDocument):
    order_id: str
    status: str = "captured"


PAYMENTS = DocumentSpec(
    name="ledger_payments",
    read=PaymentRead,
    write={"domain": Payment, "create_cmd": PaymentCreate, "update_cmd": PaymentCreate},
)


LEDGER_BALANCED = SystemInvariant(
    name="ledger_balanced",
    read_set=ReadSet(spec=ACCOUNTS, scope_keys=("ledger_id",)),
    aggregate=Sum("balance"),
    holds=lambda total: total == 0,
)

SINGLE_CAPTURED_PAYMENT = SystemInvariant(
    name="single_captured_payment",
    read_set=ReadSet(
        spec=PAYMENTS,
        scope_keys=("order_id",),
        where={"$values": {"status": "captured"}},
    ),
    aggregate=Count(),
    holds=lambda n: n <= 1,
)


# ....................... #


def _ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule()())


async def _open(ctx: ExecutionContext, ledger: str, balance: int) -> None:
    await ctx.document.command(ACCOUNTS).create(
        AccountCreate(ledger_id=ledger, balance=balance)
    )


# ----------------------- #


class TestEvaluateConservation:
    async def test_balanced_ledger_holds(self) -> None:
        ctx = _ctx()
        await _open(ctx, "L1", 100)
        await _open(ctx, "L1", -100)

        result = await evaluate(LEDGER_BALANCED, ctx, {"ledger_id": "L1"})

        assert result.held
        assert result.observed == 0.0
        assert result.name == "ledger_balanced"

    async def test_unbalanced_ledger_is_caught(self) -> None:
        ctx = _ctx()
        await _open(ctx, "L1", 100)
        await _open(ctx, "L1", -70)  # leaves +30 — the law is broken

        result = await evaluate(LEDGER_BALANCED, ctx, {"ledger_id": "L1"})

        assert not result.held
        assert result.observed == 30.0

    async def test_scope_isolates_the_read_set_per_ledger(self) -> None:
        ctx = _ctx()
        await _open(ctx, "L1", 100)
        await _open(ctx, "L1", -100)  # L1 balanced
        await _open(ctx, "L2", 50)  # L2 alone is unbalanced

        assert (await evaluate(LEDGER_BALANCED, ctx, {"ledger_id": "L1"})).held
        assert not (await evaluate(LEDGER_BALANCED, ctx, {"ledger_id": "L2"})).held


class TestEvaluateCardinality:
    async def test_at_most_one_payment_holds(self) -> None:
        ctx = _ctx()
        await ctx.document.command(PAYMENTS).create(PaymentCreate(order_id="O1"))

        result = await evaluate(SINGLE_CAPTURED_PAYMENT, ctx, {"order_id": "O1"})

        assert result.held
        assert result.observed == 1.0

    async def test_two_captured_payments_is_caught(self) -> None:
        ctx = _ctx()
        await ctx.document.command(PAYMENTS).create(PaymentCreate(order_id="O1"))
        await ctx.document.command(PAYMENTS).create(PaymentCreate(order_id="O1"))

        result = await evaluate(SINGLE_CAPTURED_PAYMENT, ctx, {"order_id": "O1"})

        assert not result.held
        assert result.observed == 2.0


# ....................... #


class TestEnforce:
    async def test_enforce_outside_a_transaction_raises_on_violation(self) -> None:
        # Outside a tx run_or_defer runs the check immediately, so the violation surfaces directly.
        ctx = _ctx()
        await _open(ctx, "L1", 30)  # unbalanced

        with pytest.raises(exc, match="ledger_balanced"):
            await enforce(LEDGER_BALANCED, ctx, {"ledger_id": "L1"})

    async def test_enforce_is_a_noop_when_the_law_holds(self) -> None:
        ctx = _ctx()
        await _open(ctx, "L1", 10)
        await _open(ctx, "L1", -10)

        await enforce(LEDGER_BALANCED, ctx, {"ledger_id": "L1"})  # must not raise

    async def test_enforce_inside_a_transaction_defers_to_post_commit(self) -> None:
        # The realistic detective flow: the check is deferred and surfaces at commit (scope exit).
        ctx = _ctx()

        with pytest.raises(exc):
            async with ctx.tx_ctx.scope("mock"):
                await ctx.document.command(ACCOUNTS).create(
                    AccountCreate(ledger_id="L1", balance=30)
                )
                await enforce(LEDGER_BALANCED, ctx, {"ledger_id": "L1"})
            # violation raises here, at post-commit, not before


# ....................... #
# P2 — preventive enforcement: evaluate inside the writing tx, raise before commit (rolls back),
# fail closed below the law's isolation floor.

_SER = IsolationLevel.SERIALIZABLE
_SNAPSHOT_LAW = attrs.evolve(
    SINGLE_CAPTURED_PAYMENT, name="payment_snapshot", required_isolation=IsolationLevel.SNAPSHOT
)


class TestEnforcePreventive:
    async def test_held_commits_the_write(self) -> None:
        ctx = _ctx()
        async with ctx.tx_ctx.scope("mock", isolation=_SER):
            await ctx.document.command(PAYMENTS).create(PaymentCreate(order_id="O1"))
            await enforce_preventive(SINGLE_CAPTURED_PAYMENT, ctx, {"order_id": "O1"})

        assert (await evaluate(SINGLE_CAPTURED_PAYMENT, ctx, {"order_id": "O1"})).observed == 1.0

    async def test_violation_rolls_the_write_back(self) -> None:
        # The point of preventive mode: the violating write is UNDONE (unlike detective enforcement,
        # where it stays durable). Two captured payments would break the cardinality law, so the
        # whole transaction rolls back and neither payment persists.
        ctx = _ctx()

        with pytest.raises(exc, match="would be violated"):
            async with ctx.tx_ctx.scope("mock", isolation=_SER):
                await ctx.document.command(PAYMENTS).create(PaymentCreate(order_id="O1"))
                await ctx.document.command(PAYMENTS).create(PaymentCreate(order_id="O1"))
                await enforce_preventive(SINGLE_CAPTURED_PAYMENT, ctx, {"order_id": "O1"})

        assert (await evaluate(SINGLE_CAPTURED_PAYMENT, ctx, {"order_id": "O1"})).observed == 0.0

    async def test_too_weak_isolation_fails_closed(self) -> None:
        # The law needs SERIALIZABLE; running the check in a SNAPSHOT tx is rejected (a write-skew
        # interleaving would defeat it), so it can't silently give weak "prevention".
        ctx = _ctx()

        with pytest.raises(exc, match="isolation"):
            async with ctx.tx_ctx.scope("mock", isolation=IsolationLevel.SNAPSHOT):
                await ctx.document.command(PAYMENTS).create(PaymentCreate(order_id="O1"))
                await enforce_preventive(SINGLE_CAPTURED_PAYMENT, ctx, {"order_id": "O1"})

    async def test_no_transaction_fails_closed(self) -> None:
        ctx = _ctx()

        with pytest.raises(exc, match="isolation"):
            await enforce_preventive(SINGLE_CAPTURED_PAYMENT, ctx, {"order_id": "O1"})

    async def test_a_stronger_transaction_satisfies_a_lower_floor(self) -> None:
        # `>=`, not `==`: a SNAPSHOT-floor law is fine inside a SERIALIZABLE transaction.
        ctx = _ctx()
        async with ctx.tx_ctx.scope("mock", isolation=_SER):
            await ctx.document.command(PAYMENTS).create(PaymentCreate(order_id="O1"))
            await enforce_preventive(_SNAPSHOT_LAW, ctx, {"order_id": "O1"})  # must not raise

        assert (await evaluate(_SNAPSHOT_LAW, ctx, {"order_id": "O1"})).observed == 1.0

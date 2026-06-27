"""SystemInvariant — declare a cross-aggregate law, evaluate + detectively enforce it.

A ledger conservation law (the balances in a ledger sum to zero) and a payment cardinality law (at
most one captured payment per order) are the two canonical cross-record invariants the entity-level
``@invariant`` cannot express. These tests pin the kernel: ``evaluate`` reads the read-set aggregate
and reports whether the predicate held; ``enforce`` runs that check (immediately outside a
transaction, deferred to post-commit inside one) and raises on a violation — detective, not
preventive.
"""

from __future__ import annotations

from uuid import uuid4

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
from forze_kits.invariants import enforce, enforce_preventive, evaluate, propose
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

    async def test_a_missing_scope_key_fails_closed(self) -> None:
        # An unbound scope key is a clear configuration error, not a cryptic KeyError.
        ctx = _ctx()

        with pytest.raises(exc, match="missing scope key"):
            await evaluate(SINGLE_CAPTURED_PAYMENT, ctx, {})  # order_id not bound


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
# Preventive enforcement: evaluate inside the writing tx, raise before commit (rolls back),
# fail closed below the law's isolation floor.

_SER = IsolationLevel.SERIALIZABLE
_SNAPSHOT_LAW = attrs.evolve(
    SINGLE_CAPTURED_PAYMENT,
    name="payment_snapshot",
    required_isolation=IsolationLevel.SNAPSHOT,
)


class TestEnforcePreventive:
    async def test_held_commits_the_write(self) -> None:
        ctx = _ctx()
        async with ctx.tx_ctx.scope("mock", isolation=_SER):
            await ctx.document.command(PAYMENTS).create(PaymentCreate(order_id="O1"))
            await enforce_preventive(SINGLE_CAPTURED_PAYMENT, ctx, {"order_id": "O1"})

        assert (
            await evaluate(SINGLE_CAPTURED_PAYMENT, ctx, {"order_id": "O1"})
        ).observed == 1.0

    async def test_violation_rolls_the_write_back(self) -> None:
        # The point of preventive mode: the violating write is UNDONE (unlike detective enforcement,
        # where it stays durable). Two captured payments would break the cardinality law, so the
        # whole transaction rolls back and neither payment persists.
        ctx = _ctx()

        with pytest.raises(exc, match="would be violated"):
            async with ctx.tx_ctx.scope("mock", isolation=_SER):
                await ctx.document.command(PAYMENTS).create(
                    PaymentCreate(order_id="O1")
                )
                await ctx.document.command(PAYMENTS).create(
                    PaymentCreate(order_id="O1")
                )
                await enforce_preventive(
                    SINGLE_CAPTURED_PAYMENT, ctx, {"order_id": "O1"}
                )

        assert (
            await evaluate(SINGLE_CAPTURED_PAYMENT, ctx, {"order_id": "O1"})
        ).observed == 0.0

    async def test_too_weak_isolation_fails_closed(self) -> None:
        # The law needs SERIALIZABLE; running the check in a SNAPSHOT tx is rejected (a write-skew
        # interleaving would defeat it), so it can't silently give weak "prevention".
        ctx = _ctx()

        with pytest.raises(exc, match="isolation"):
            async with ctx.tx_ctx.scope("mock", isolation=IsolationLevel.SNAPSHOT):
                await ctx.document.command(PAYMENTS).create(
                    PaymentCreate(order_id="O1")
                )
                await enforce_preventive(
                    SINGLE_CAPTURED_PAYMENT, ctx, {"order_id": "O1"}
                )

    async def test_no_transaction_fails_closed(self) -> None:
        ctx = _ctx()

        with pytest.raises(exc, match="isolation"):
            await enforce_preventive(SINGLE_CAPTURED_PAYMENT, ctx, {"order_id": "O1"})

    async def test_a_stronger_transaction_satisfies_a_lower_floor(self) -> None:
        # `>=`, not `==`: a SNAPSHOT-floor law is fine inside a SERIALIZABLE transaction.
        ctx = _ctx()
        async with ctx.tx_ctx.scope("mock", isolation=_SER):
            await ctx.document.command(PAYMENTS).create(PaymentCreate(order_id="O1"))
            await enforce_preventive(
                _SNAPSHOT_LAW, ctx, {"order_id": "O1"}
            )  # must not raise

        assert (await evaluate(_SNAPSHOT_LAW, ctx, {"order_id": "O1"})).observed == 1.0


# ....................... #
# propose — a rollback-only dry-run: "would this write be accepted?" A FILTER, not a proof.


class TestPropose:
    async def test_holds_when_the_proposed_write_keeps_the_law(self) -> None:
        # L1 holds +100 (unbalanced on its own); proposing the matching -100 would balance it.
        ctx = _ctx()
        await _open(ctx, "L1", 100)

        async def _apply(c: ExecutionContext) -> None:
            await c.document.command(ACCOUNTS).create(
                AccountCreate(ledger_id="L1", balance=-100)
            )

        verdict = await propose(
            ctx,
            _apply,
            [(LEDGER_BALANCED, {"ledger_id": "L1"})],
            route="mock",
            isolation=_SER,
        )

        assert verdict.holds
        assert verdict.failed == ()
        assert verdict.error is None
        assert [r.observed for r in verdict.results] == [0.0]

    async def test_fails_when_the_proposed_write_breaks_the_law(self) -> None:
        ctx = _ctx()
        await _open(ctx, "L1", 100)

        async def _apply(c: ExecutionContext) -> None:
            await c.document.command(ACCOUNTS).create(
                AccountCreate(ledger_id="L1", balance=-70)
            )

        verdict = await propose(
            ctx,
            _apply,
            [(LEDGER_BALANCED, {"ledger_id": "L1"})],
            route="mock",
            isolation=_SER,
        )

        assert not verdict.holds
        assert verdict.failed == ("ledger_balanced",)
        assert verdict.results[0].observed == 30.0

    async def test_nothing_persists_after_the_dry_run(self) -> None:
        # The defining property: hold or not, the dry-run leaves NO durable trace. The proposal would
        # balance L1, yet afterwards L1 is still the original +100 — the dry-run's write was rolled back.
        ctx = _ctx()
        await _open(ctx, "L1", 100)

        async def _apply(c: ExecutionContext) -> None:
            await c.document.command(ACCOUNTS).create(
                AccountCreate(ledger_id="L1", balance=-100)
            )

        verdict = await propose(
            ctx,
            _apply,
            [(LEDGER_BALANCED, {"ledger_id": "L1"})],
            route="mock",
            isolation=_SER,
        )

        assert verdict.holds
        assert (
            await evaluate(LEDGER_BALANCED, ctx, {"ledger_id": "L1"})
        ).observed == 100.0

    async def test_a_rejected_write_is_captured_as_an_error(self) -> None:
        # When apply itself raises (here: update a non-existent account), the write is rejected — it is
        # captured as `error`, holds is False, and the laws are not evaluated (results stays empty).
        ctx = _ctx()

        async def _apply(c: ExecutionContext) -> None:
            await c.document.command(ACCOUNTS).update(
                uuid4(), 1, AccountCreate(ledger_id="L1", balance=5)
            )

        verdict = await propose(
            ctx,
            _apply,
            [(LEDGER_BALANCED, {"ledger_id": "L1"})],
            route="mock",
            isolation=_SER,
        )

        assert not verdict.holds
        assert verdict.error is not None
        assert verdict.results == ()

    async def test_empty_checks_holds_when_the_write_applies(self) -> None:
        # No laws to check: a clean apply is acceptable by default — and still persists nothing (the
        # +5 it wrote is gone, so the scope reads as the empty 0.0, not 5.0).
        ctx = _ctx()

        async def _apply(c: ExecutionContext) -> None:
            await c.document.command(ACCOUNTS).create(
                AccountCreate(ledger_id="L1", balance=5)
            )

        verdict = await propose(ctx, _apply, [], route="mock", isolation=_SER)

        assert verdict.holds
        assert verdict.results == ()
        assert (
            await evaluate(LEDGER_BALANCED, ctx, {"ledger_id": "L1"})
        ).observed == 0.0

    async def test_a_check_misconfiguration_propagates(self) -> None:
        # A setup bug in a check (an unbound scope key) is NOT a verdict — it propagates, so a broken
        # check cannot masquerade as "the write was rejected".
        ctx = _ctx()

        async def _apply(c: ExecutionContext) -> None:
            await c.document.command(ACCOUNTS).create(
                AccountCreate(ledger_id="L1", balance=0)
            )

        with pytest.raises(exc, match="missing scope key"):
            await propose(
                ctx, _apply, [(LEDGER_BALANCED, {})], route="mock", isolation=_SER
            )

    async def test_nothing_persists_when_a_law_fails(self) -> None:
        # The rollback is unconditional, so a FAILING dry-run must also leave no durable trace — L1 is
        # still the original +100 afterwards, not the +30 the rejected proposal would have produced.
        ctx = _ctx()
        await _open(ctx, "L1", 100)

        async def _apply(c: ExecutionContext) -> None:
            await c.document.command(ACCOUNTS).create(
                AccountCreate(ledger_id="L1", balance=-70)
            )

        verdict = await propose(
            ctx,
            _apply,
            [(LEDGER_BALANCED, {"ledger_id": "L1"})],
            route="mock",
            isolation=_SER,
        )

        assert not verdict.holds
        assert (
            await evaluate(LEDGER_BALANCED, ctx, {"ledger_id": "L1"})
        ).observed == 100.0

    async def test_a_partially_applied_rejection_rolls_back_the_successful_write(
        self,
    ) -> None:
        # apply does a successful create, THEN a rejected update — the create must NOT survive. This
        # pins that the rollback fires even when apply is captured as an error mid-way.
        ctx = _ctx()

        async def _apply(c: ExecutionContext) -> None:
            await c.document.command(ACCOUNTS).create(
                AccountCreate(ledger_id="L1", balance=42)
            )
            await c.document.command(ACCOUNTS).update(
                uuid4(), 1, AccountCreate(ledger_id="L1", balance=5)
            )  # not found — rejected

        verdict = await propose(
            ctx,
            _apply,
            [(LEDGER_BALANCED, {"ledger_id": "L1"})],
            route="mock",
            isolation=_SER,
        )

        assert not verdict.holds
        assert verdict.error is not None
        assert verdict.results == ()
        assert (
            await evaluate(LEDGER_BALANCED, ctx, {"ledger_id": "L1"})
        ).observed == 0.0  # the +42 create was rolled back

    async def test_inside_a_transaction_fails_closed(self) -> None:
        # propose must open its OWN root transaction: called inside an active scope it fails closed
        # rather than running as a savepoint whose rollback the backend may not honor (which would
        # silently leak the dry-run's writes into the caller's transaction).
        ctx = _ctx()

        async def _apply(c: ExecutionContext) -> None:
            await c.document.command(ACCOUNTS).create(
                AccountCreate(ledger_id="L1", balance=0)
            )

        with pytest.raises(exc, match="own root transaction"):
            async with ctx.tx_ctx.scope("mock", isolation=_SER):
                await propose(ctx, _apply, [], route="mock", isolation=_SER)

    async def test_isolation_is_threaded_into_the_scope_and_capability_gated(
        self,
    ) -> None:
        # The no-op ("none") manager supports only READ_COMMITTED; requesting SERIALIZABLE must fail
        # closed — which proves the isolation kwarg is actually forwarded into scope().
        ctx = context_from_deps(MockDepsModule(transactions="none")())

        async def _apply(c: ExecutionContext) -> None:
            await c.document.command(ACCOUNTS).create(
                AccountCreate(ledger_id="L1", balance=0)
            )

        with pytest.raises(exc, match="isolation"):
            await propose(ctx, _apply, [], route="mock", isolation=_SER)

    async def test_isolation_none_leaves_the_manager_default(self) -> None:
        # isolation=None requests nothing, so even the READ_COMMITTED-only manager accepts it (no gate).
        ctx = context_from_deps(MockDepsModule(transactions="none")())

        async def _apply(c: ExecutionContext) -> None:
            await c.document.command(ACCOUNTS).create(
                AccountCreate(ledger_id="L1", balance=0)
            )

        verdict = await propose(ctx, _apply, [], route="mock", isolation=None)

        assert verdict.holds

    async def test_multi_check_failed_lists_exactly_the_violated_laws_in_order(
        self,
    ) -> None:
        # With >1 law, `failed` must list exactly the violated ones in `checks` order, and `holds` is
        # False iff any fails. Here the 1st and 3rd break, the 2nd holds.
        law_a = LEDGER_BALANCED  # name "ledger_balanced"
        law_b = SINGLE_CAPTURED_PAYMENT  # name "single_captured_payment"
        law_c = attrs.evolve(LEDGER_BALANCED, name="ledger_balanced_b")
        ctx = _ctx()

        async def _apply(c: ExecutionContext) -> None:
            await c.document.command(ACCOUNTS).create(
                AccountCreate(ledger_id="L1", balance=30)
            )  # unbalanced -> a fails
            await c.document.command(PAYMENTS).create(
                PaymentCreate(order_id="O1")
            )  # -> b holds
            await c.document.command(ACCOUNTS).create(
                AccountCreate(ledger_id="L2", balance=50)
            )  # unbalanced -> c fails

        verdict = await propose(
            ctx,
            _apply,
            [
                (law_a, {"ledger_id": "L1"}),
                (law_b, {"order_id": "O1"}),
                (law_c, {"ledger_id": "L2"}),
            ],
            route="mock",
            isolation=_SER,
        )

        assert not verdict.holds
        assert len(verdict.results) == 3
        assert verdict.failed == ("ledger_balanced", "ledger_balanced_b")
        assert "single_captured_payment" not in verdict.failed

    async def test_multi_check_all_hold(self) -> None:
        ctx = _ctx()

        async def _apply(c: ExecutionContext) -> None:
            await c.document.command(ACCOUNTS).create(
                AccountCreate(ledger_id="L1", balance=100)
            )
            await c.document.command(ACCOUNTS).create(
                AccountCreate(ledger_id="L1", balance=-100)
            )  # balanced
            await c.document.command(PAYMENTS).create(
                PaymentCreate(order_id="O1")
            )  # one payment

        verdict = await propose(
            ctx,
            _apply,
            [
                (LEDGER_BALANCED, {"ledger_id": "L1"}),
                (SINGLE_CAPTURED_PAYMENT, {"order_id": "O1"}),
            ],
            route="mock",
            isolation=_SER,
        )

        assert verdict.holds
        assert verdict.failed == ()

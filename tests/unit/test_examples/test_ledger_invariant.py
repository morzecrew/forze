"""Runs the ledger-invariant example end to end — a cross-aggregate conservation law in a real flow.

A double-entry ledger declares the law "the balances in a ledger sum to zero" as a `SystemInvariant`
and enforces it detectively after each balance-changing operation. A correct `transfer` preserves the
sum (the law passes silently); a single-sided `mint` breaks it (the post-commit check raises) — but
*detectively*: the bad write is already durable when the breach is reported, which the last assertion
pins so the example can't quietly oversell post-commit enforcement as prevention.
"""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException

from examples.recipes.ledger_invariant.app import (
    build_context,
    ledger_balance,
    mint,
    open_account,
    transfer,
)

# ----------------------- #


class TestLedgerInvariantExample:
    async def test_transfer_preserves_the_conservation_law(self) -> None:
        ctx = build_context()
        asset = await open_account(ctx, "L1", 100)
        liability = await open_account(ctx, "L1", -100)

        assert (await ledger_balance(ctx, "L1")).held  # balanced opening

        await transfer(ctx, "L1", asset, liability, 30)  # must not raise

        result = await ledger_balance(ctx, "L1")
        assert result.held
        assert result.observed == 0.0

    async def test_single_sided_mint_is_caught_but_not_prevented(self) -> None:
        ctx = build_context()
        asset = await open_account(ctx, "L1", 100)
        await open_account(ctx, "L1", -100)

        # The deferred conservation check raises post-commit when the single-sided write lands.
        with pytest.raises(CoreException):
            await mint(ctx, "L1", asset, 50)

        # Detective, not preventive: the credit committed, so the ledger now reports the breach.
        broken = await ledger_balance(ctx, "L1")
        assert not broken.held
        assert broken.observed == 50.0

    async def test_the_law_is_scoped_per_ledger(self) -> None:
        ctx = build_context()
        await open_account(ctx, "L1", 100)
        await open_account(ctx, "L1", -100)  # L1 balanced
        await open_account(ctx, "L2", 25)  # L2 alone is unbalanced

        assert (await ledger_balance(ctx, "L1")).held
        assert not (await ledger_balance(ctx, "L2")).held

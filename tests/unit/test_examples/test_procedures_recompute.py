"""Batch-recompute recipe — ingest a batch, run one governed set-based recompute, read totals."""

from __future__ import annotations

import pytest

from examples.recipes.procedures_recompute.app import (
    RECOMPUTE_SPEC,
    Sale,
    build_context,
    ingest_sales,
    recompute,
    region_totals,
)
from forze.base.exceptions import CoreException


async def test_batch_recompute_flow() -> None:
    ctx = build_context()

    await ingest_sales(
        ctx,
        [
            Sale(region="us", amount=30),
            Sale(region="us", amount=20),
            Sale(region="eu", amount=15),
            Sale(region="apac", amount=8),
        ],
    )

    # One procedure call recomputes every region over the whole batch.
    written = await recompute(ctx)
    assert written == 3  # us, eu, apac

    totals = await region_totals(ctx)
    assert [(t.region, t.total) for t in totals] == [
        ("apac", 8),
        ("eu", 15),
        ("us", 50),
    ]


async def test_recompute_over_empty_batch() -> None:
    ctx = build_context()
    assert await recompute(ctx) == 0
    assert await region_totals(ctx) == []


def test_spec_is_command_only_side_effect() -> None:
    # No result model → side-effect procedure (returns an affected count).
    assert RECOMPUTE_SPEC.result is None
    assert not RECOMPUTE_SPEC.returns_row
    assert not RECOMPUTE_SPEC.returns_scalar


async def test_command_refused_in_read_only_operation() -> None:
    # The procedures port is command-only: acquiring it in a read-only (QUERY) op fails closed.
    ctx = build_context()
    ctx.inv_ctx.set_read_only()
    with pytest.raises(CoreException, match="read-only"):
        ctx.procedure.command(RECOMPUTE_SPEC)

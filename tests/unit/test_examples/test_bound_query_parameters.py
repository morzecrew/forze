"""Bound-query-parameters recipe — bind an "as of" date the view ranks over internally."""

from __future__ import annotations

from datetime import date

import pytest

from examples.recipes.bound_query_parameters.app import (
    STANDINGS_SPEC,
    AsOf,
    build_context,
    regional_top,
)
from forze.base.exceptions import CoreException


async def test_ranking_is_bounded_by_the_as_of_parameter() -> None:
    ctx = build_context()

    # As of March the late April entry (cy) is excluded, so eu ranks bo then ana.
    march = await regional_top(ctx, "eu", date(2026, 3, 1))
    assert [(s.player, s.rank) for s in march] == [("bo", 1), ("ana", 2)]


async def test_a_later_as_of_reshuffles_the_ranks() -> None:
    ctx = build_context()

    # As of May cy (score 40) is in and lands second — the rank is recomputed inside the source.
    may = await regional_top(ctx, "eu", date(2026, 5, 1))
    assert [(s.player, s.rank) for s in may] == [("bo", 1), ("cy", 2), ("ana", 3)]


async def test_dsl_filter_composes_over_the_parametrized_source() -> None:
    ctx = build_context()

    # The region filter is the ordinary document DSL applied on top of the bound read.
    us = await regional_top(ctx, "us", date(2026, 5, 1))
    assert [s.player for s in us] == ["dot", "el"]
    assert all(s.region == "us" for s in us)


async def test_reading_without_binding_fails_closed() -> None:
    # A query_params spec read without with_parameters fails closed — the view depends on the
    # setting, so an unbound read is a bug, not an empty result.
    ctx = build_context()
    with pytest.raises(CoreException, match="query_parameters_unbound"):
        await ctx.document.query(STANDINGS_SPEC).find_many()


async def test_count_composes_over_the_bound_source() -> None:
    ctx = build_context()
    total = await (
        ctx.document.query(STANDINGS_SPEC).with_parameters(AsOf(as_of=date(2026, 5, 1))).count()
    )
    assert total == 5  # 3 eu + 2 us, as of May

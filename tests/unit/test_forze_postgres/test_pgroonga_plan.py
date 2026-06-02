"""Unit tests for PGroonga plan selection and candidate caps."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from forze_postgres.adapters.search._pgroonga_plan import (
    effective_candidate_limit,
    effective_pgroonga_plan_option,
    is_coalesced_read_heap,
    is_trivial_filter,
    resolve_pgroonga_plan,
)
from forze_postgres.kernel.gateways import PostgresQualifiedName

# ----------------------- #


def test_is_coalesced_read_heap_default_id_join() -> None:
    read = ("public", "docs")
    assert is_coalesced_read_heap(read, read, None) is True
    assert is_coalesced_read_heap(read, read, (("id", "id"),)) is True


def test_is_coalesced_read_heap_different_tables() -> None:
    read = ("public", "docs")
    heap = ("public", "docs_heap")
    assert is_coalesced_read_heap(read, heap, None) is False


def test_effective_candidate_limit_page_and_snapshot() -> None:
    cap = effective_candidate_limit(
        config_limit=100,
        options={"candidate_limit": 200},
        pagination={"limit": 50, "offset": 100},
        snapshot=None,
        result_snapshot=None,
        rs_spec=None,
    )
    assert cap == 200

    cap2 = effective_candidate_limit(
        config_limit=100,
        options=None,
        pagination={"limit": 20, "offset": 0},
        snapshot=None,
        result_snapshot=None,
        rs_spec=None,
    )
    assert cap2 >= 70


@pytest.mark.asyncio
async def test_resolve_pgroonga_plan_auto_with_filters_uses_filter_first() -> None:
    intro = AsyncMock()
    plan = await resolve_pgroonga_plan(
        configured="auto",
        options=None,
        parsed_filters=object(),  # type: ignore[arg-type]
        read_qname=PostgresQualifiedName("public", "docs"),
        introspector=intro,
        auto_index_first_min_rows=10,
        auto_use_exact_count=False,
        count_filtered_rows=None,
    )
    assert plan == "filter_first"
    intro.estimate_relation_rows.assert_not_called()


@pytest.mark.asyncio
async def test_resolve_pgroonga_plan_auto_large_table_index_first() -> None:
    intro = AsyncMock()
    intro.estimate_relation_rows = AsyncMock(return_value=500_000)

    plan = await resolve_pgroonga_plan(
        configured="auto",
        options=None,
        parsed_filters=None,
        read_qname=PostgresQualifiedName("public", "docs"),
        introspector=intro,
        auto_index_first_min_rows=100_000,
        auto_use_exact_count=False,
        count_filtered_rows=None,
    )
    assert plan == "index_first"


@pytest.mark.asyncio
async def test_resolve_pgroonga_plan_option_override() -> None:
    intro = AsyncMock()
    plan = await resolve_pgroonga_plan(
        configured="filter_first",
        options={"pgroonga_plan": "index_first"},
        parsed_filters=None,
        read_qname=PostgresQualifiedName("public", "docs"),
        introspector=intro,
        auto_index_first_min_rows=1,
        auto_use_exact_count=False,
        count_filtered_rows=None,
    )
    assert plan == "index_first"


def test_is_trivial_filter_and_plan_option() -> None:
    assert is_trivial_filter(None) is True
    assert effective_pgroonga_plan_option({"pgroonga_plan": "auto"}) == "auto"
    assert effective_pgroonga_plan_option({}) is None

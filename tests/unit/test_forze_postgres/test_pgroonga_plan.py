"""Unit tests for PGroonga plan selection and candidate caps."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from forze.application.contracts.querying.internal import QueryField
from forze_postgres.adapters.search._pgroonga_plan import (
    effective_candidate_limit,
    effective_combo_limit,
    effective_pgroonga_plan_option,
    effective_ranked_candidate_limit,
    ensure_pgroonga_plan_with_candidate_cap,
    index_first_heap_limit,
    is_coalesced_read_heap,
    is_index_first_eligible_filter,
    is_trivial_filter,
    resolve_pgroonga_plan,
)
from forze_postgres.adapters.search.hub.merge import merge_hub_leg_rows
from forze_postgres.kernel.catalog.introspect.introspector import PostgresIntrospector
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


def test_index_first_eligible_filter() -> None:
    assert is_index_first_eligible_filter(None) is True
    assert is_index_first_eligible_filter(
        QueryField(name="tenant_id", op="$eq", value="t1"),
    )
    assert not is_index_first_eligible_filter(
        QueryField(name="title", op="$like", value="%x%"),
    )


def test_index_first_heap_limit_margin() -> None:
    assert index_first_heap_limit(100, has_projection_filters=False, filter_margin=3.0) == 100
    assert index_first_heap_limit(100, has_projection_filters=True, filter_margin=3.0) == 300


def test_plan_rows_from_explain_payload() -> None:
    payload = [{"Plan": {"Plan Rows": 42}}]
    assert PostgresIntrospector._plan_rows_from_explain_payload(payload) == 42


def test_merge_hub_leg_rows_max_or() -> None:
    leg_a = [{"id": 1, "_hub_rank": 2.0}]
    leg_b = [{"id": 1, "_hub_rank": 5.0}, {"id": 2, "_hub_rank": 1.0}]
    merged = merge_hub_leg_rows(
        leg_rows=[leg_a, leg_b],
        weights=[1.0, 1.0],
        score_merge="max",
        combine="or",
        read_fields=frozenset({"id"}),
    )
    assert merged[0]["id"] == 1
    assert merged[0]["_hub_rank"] == 5.0


@pytest.mark.asyncio
async def test_resolve_pgroonga_plan_auto_ineligible_filter_uses_filter_first() -> None:
    intro = AsyncMock()
    plan = await resolve_pgroonga_plan(
        configured="auto",
        options=None,
        parsed_filters=QueryField(name="title", op="$like", value="%a%"),
        read_qname=PostgresQualifiedName("public", "docs"),
        introspector=intro,
        auto_index_first_min_rows=10,
        auto_filter_first_max_rows=50_000,
        auto_with_filters=True,
        auto_use_exact_count=False,
        count_filtered_rows=None,
        estimate_filtered_rows=None,
    )
    assert plan == "filter_first"
    intro.estimate_relation_rows.assert_not_called()


@pytest.mark.asyncio
async def test_resolve_pgroonga_plan_auto_selective_filter_index_first() -> None:
    intro = AsyncMock()
    intro.estimate_filtered_rows = AsyncMock(return_value=200_000)

    plan = await resolve_pgroonga_plan(
        configured="auto",
        options=None,
        parsed_filters=QueryField(name="tenant_id", op="$eq", value="t1"),
        read_qname=PostgresQualifiedName("public", "docs"),
        introspector=intro,
        auto_index_first_min_rows=100_000,
        auto_filter_first_max_rows=50_000,
        auto_with_filters=True,
        auto_use_exact_count=False,
        count_filtered_rows=None,
        estimate_filtered_rows=intro.estimate_filtered_rows,
    )
    assert plan == "index_first"


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
        auto_filter_first_max_rows=50_000,
        auto_with_filters=True,
        auto_use_exact_count=False,
        count_filtered_rows=None,
        estimate_filtered_rows=None,
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
        auto_filter_first_max_rows=50_000,
        auto_with_filters=True,
        auto_use_exact_count=False,
        count_filtered_rows=None,
        estimate_filtered_rows=None,
    )
    assert plan == "index_first"


def test_is_trivial_filter_and_plan_option() -> None:
    assert is_trivial_filter(None) is True
    assert effective_pgroonga_plan_option({"pgroonga_plan": "auto"}) == "auto"
    assert effective_pgroonga_plan_option({}) is None


def test_ensure_pgroonga_plan_with_candidate_cap() -> None:
    assert (
        ensure_pgroonga_plan_with_candidate_cap("index_first", None)
        == "filter_first"
    )
    assert ensure_pgroonga_plan_with_candidate_cap("index_first", 100) == "index_first"
    assert ensure_pgroonga_plan_with_candidate_cap("filter_first", None) == "filter_first"


def test_effective_combo_limit_derives_from_per_leg() -> None:
    cap = effective_combo_limit(
        config_limit=None,
        per_leg_limit=5000,
        options=None,
        pagination={"limit": 10, "offset": 0},
        snapshot=None,
        result_snapshot=None,
        rs_spec=None,
    )
    assert cap is not None
    assert cap >= 5000

"""Unit tests for PGroonga plan selection and candidate caps."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from forze.application.contracts.querying.internal import QueryAnd, QueryField
from forze_postgres.adapters.search._pgroonga_plan import (
    effective_candidate_limit,
    effective_combo_limit,
    effective_pgroonga_plan_option,
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


@pytest.mark.asyncio
async def test_resolve_pgroonga_plan_tenant_aware_forces_filter_first() -> None:
    """Tenant-aware search never uses ``index_first``, even when otherwise selected.

    ``index_first`` ranks a heap top-K across all tenants and applies the tenant
    predicate only as an outer post-filter, which scans cross-tenant rows and
    truncates a tenant's results. Tenant-awareness overrides every other signal —
    an explicit ``index_first`` option, a large-table ``auto`` estimate, and the
    filterless-browse path — without issuing the (misleading) estimation queries.
    """

    intro = AsyncMock()
    intro.estimate_relation_rows = AsyncMock(return_value=500_000)
    intro.estimate_filtered_rows = AsyncMock(return_value=500_000)

    for configured, options, parsed in (
        ("index_first", None, None),
        ("auto", {"pgroonga_plan": "index_first"}, None),
        ("auto", None, None),
        ("auto", None, QueryField(name="tenant_id", op="$eq", value="t1")),
    ):
        plan = await resolve_pgroonga_plan(
            configured=configured,
            options=options,
            parsed_filters=parsed,
            read_qname=PostgresQualifiedName("public", "docs"),
            introspector=intro,
            auto_index_first_min_rows=1,
            auto_filter_first_max_rows=50_000,
            auto_with_filters=True,
            auto_use_exact_count=False,
            count_filtered_rows=None,
            estimate_filtered_rows=intro.estimate_filtered_rows,
            tenant_aware=True,
        )
        assert plan == "filter_first"

    # No estimation queries are issued on the tenant-aware short circuit.
    intro.estimate_relation_rows.assert_not_awaited()
    intro.estimate_filtered_rows.assert_not_awaited()


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


# ----------------------- #


class _StubResultSnapshot:
    """Minimal stand-in for ``SearchResultSnapshot`` snapshot-cap hooks."""

    def __init__(self, *, should_write: bool, max_ids: int) -> None:
        self._should_write = should_write
        self._max_ids = max_ids

    def should_write_result_snapshot(self, snapshot: object, rs_spec: object) -> bool:
        return self._should_write

    def effective_snapshot_max_ids(self, snapshot: object, rs_spec: object) -> int:
        return self._max_ids


# ....................... #


def test_is_index_first_eligible_filter_query_and_recursion() -> None:
    eligible = QueryAnd(
        items=(
            QueryField(name="tenant_id", op="$eq", value="t1"),
            QueryField(name="kind", op="$in", value=("a", "b")),
        ),
    )
    assert is_index_first_eligible_filter(eligible) is True

    mixed = QueryAnd(
        items=(
            QueryField(name="tenant_id", op="$eq", value="t1"),
            QueryField(name="title", op="$like", value="%x%"),
        ),
    )
    assert is_index_first_eligible_filter(mixed) is False


def test_is_index_first_eligible_filter_unknown_node_false() -> None:
    # Neither QueryAnd nor QueryField -> conservative False (line 51).
    assert is_index_first_eligible_filter(object()) is False  # type: ignore[arg-type]


def test_is_coalesced_read_heap_non_static_heap_false() -> None:
    read = ("public", "docs")
    # Non-tuple heap is not a static relation -> False (line 62).
    assert is_coalesced_read_heap(read, ["public", "docs"], None) is False  # type: ignore[arg-type]


def test_is_coalesced_read_heap_non_static_read_false() -> None:
    # Non-tuple read is not a static relation -> False (line 65).
    assert is_coalesced_read_heap(["public", "docs"], ("public", "docs"), None) is False  # type: ignore[arg-type]


def test_effective_candidate_limit_returns_none_without_caps() -> None:
    cap = effective_candidate_limit(
        config_limit=None,
        options=None,
        pagination=None,
        snapshot=None,
        result_snapshot=None,
        rs_spec=None,
    )
    assert cap is None


def test_effective_candidate_limit_snapshot_raises_floor() -> None:
    snap = _StubResultSnapshot(should_write=True, max_ids=9_000)
    cap = effective_candidate_limit(
        config_limit=100,
        options=None,
        pagination=None,
        snapshot=None,
        result_snapshot=snap,
        rs_spec=object(),
    )
    assert cap == 9_000


def test_effective_combo_limit_option_override() -> None:
    cap = effective_combo_limit(
        config_limit=None,
        per_leg_limit=10,
        options={"combo_limit": 777},
        pagination=None,
        snapshot=None,
        result_snapshot=None,
        rs_spec=None,
    )
    assert cap == 777


def test_effective_combo_limit_config_limit() -> None:
    cap = effective_combo_limit(
        config_limit=321,
        per_leg_limit=10,
        options=None,
        pagination=None,
        snapshot=None,
        result_snapshot=None,
        rs_spec=None,
    )
    assert cap == 321


def test_effective_combo_limit_option_override_snapshot_floor() -> None:
    snap = _StubResultSnapshot(should_write=True, max_ids=5_000)
    cap = effective_combo_limit(
        config_limit=None,
        per_leg_limit=10,
        options={"combo_limit": 100},
        pagination={"limit": 10, "offset": 0},
        snapshot=None,
        result_snapshot=snap,
        rs_spec=object(),
    )
    assert cap == 5_000


def test_effective_combo_limit_derived_snapshot_floor() -> None:
    snap = _StubResultSnapshot(should_write=True, max_ids=8_000)
    cap = effective_combo_limit(
        config_limit=None,
        per_leg_limit=100,
        options=None,
        pagination={"limit": 10, "offset": 0},
        snapshot=None,
        result_snapshot=snap,
        rs_spec=object(),
    )
    assert cap == 8_000


@pytest.mark.asyncio
async def test_resolve_pgroonga_plan_configured_filter_first_short_circuits() -> None:
    intro = AsyncMock()
    plan = await resolve_pgroonga_plan(
        configured="filter_first",
        options=None,
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
    assert plan == "filter_first"
    intro.estimate_relation_rows.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("filtered_count", "expected"),
    [(500_000, "index_first"), (10, "filter_first")],
)
async def test_resolve_pgroonga_plan_trivial_exact_count(
    filtered_count: int,
    expected: str,
) -> None:
    intro = AsyncMock()
    count = AsyncMock(return_value=filtered_count)
    plan = await resolve_pgroonga_plan(
        configured="auto",
        options=None,
        parsed_filters=None,
        read_qname=PostgresQualifiedName("public", "docs"),
        introspector=intro,
        auto_index_first_min_rows=100_000,
        auto_filter_first_max_rows=50_000,
        auto_with_filters=True,
        auto_use_exact_count=True,
        count_filtered_rows=count,
        estimate_filtered_rows=None,
    )
    assert plan == expected
    count.assert_awaited_once()
    intro.estimate_relation_rows.assert_not_called()


@pytest.mark.asyncio
async def test_resolve_pgroonga_plan_trivial_estimate_filter_first() -> None:
    intro = AsyncMock()
    intro.estimate_relation_rows = AsyncMock(return_value=10)
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
    assert plan == "filter_first"
    intro.estimate_relation_rows.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("filtered_count", "expected"),
    [
        (10, "filter_first"),  # <= max -> filter_first (line 293)
        (500_000, "index_first"),  # >= min -> index_first (line 296)
        (75_000, "filter_first"),  # between caps -> fall-through (line 298)
    ],
)
async def test_resolve_pgroonga_plan_filtered_exact_count_thresholds(
    filtered_count: int,
    expected: str,
) -> None:
    intro = AsyncMock()
    count = AsyncMock(return_value=filtered_count)
    plan = await resolve_pgroonga_plan(
        configured="auto",
        options=None,
        parsed_filters=QueryField(name="tenant_id", op="$eq", value="t1"),
        read_qname=PostgresQualifiedName("public", "docs"),
        introspector=intro,
        auto_index_first_min_rows=100_000,
        auto_filter_first_max_rows=50_000,
        auto_with_filters=True,
        auto_use_exact_count=True,
        count_filtered_rows=count,
        estimate_filtered_rows=None,
    )
    assert plan == expected
    count.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("filtered_estimate", "expected"),
    [
        (10, "filter_first"),  # <= max (line 303)
        (500_000, "index_first"),  # >= min (line 306)
        (75_000, "filter_first"),  # between caps -> final fall-through (line 309)
    ],
)
async def test_resolve_pgroonga_plan_filtered_estimate_thresholds(
    filtered_estimate: int,
    expected: str,
) -> None:
    intro = AsyncMock()
    estimate = AsyncMock(return_value=filtered_estimate)
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
        estimate_filtered_rows=estimate,
    )
    assert plan == expected
    estimate.assert_awaited_once()


@pytest.mark.asyncio
async def test_resolve_pgroonga_plan_filtered_no_estimate_falls_through() -> None:
    # auto_use_exact_count off and no estimate callable: first-block filtered_estimate
    # stays None (branch 288->291) and the function falls through to filter_first.
    intro = AsyncMock()
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
        estimate_filtered_rows=None,
    )
    assert plan == "filter_first"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("filtered_estimate", "expected"),
    [
        (10, "filter_first"),  # <= max (line 303->304)
        (500_000, "index_first"),  # >= min (line 306->307)
        (75_000, "filter_first"),  # between caps -> final fall-through (line 309)
    ],
)
async def test_resolve_pgroonga_plan_count_returns_none_uses_estimate_block(
    filtered_estimate: int,
    expected: str,
) -> None:
    # auto_use_exact_count on but count callable yields None -> first filtered_estimate
    # is None, exercising the second estimate_filtered_rows block (lines 300-309).
    intro = AsyncMock()
    count = AsyncMock(return_value=None)
    estimate = AsyncMock(return_value=filtered_estimate)
    plan = await resolve_pgroonga_plan(
        configured="auto",
        options=None,
        parsed_filters=QueryField(name="tenant_id", op="$eq", value="t1"),
        read_qname=PostgresQualifiedName("public", "docs"),
        introspector=intro,
        auto_index_first_min_rows=100_000,
        auto_filter_first_max_rows=50_000,
        auto_with_filters=True,
        auto_use_exact_count=True,
        count_filtered_rows=count,
        estimate_filtered_rows=estimate,
    )
    assert plan == expected
    count.assert_awaited_once()
    estimate.assert_awaited_once()


def test_effective_combo_limit_derived_snapshot_floor_no_page() -> None:
    # Derived path with no pagination need (branch 177->180) but snapshot raises floor.
    snap = _StubResultSnapshot(should_write=True, max_ids=8_000)
    cap = effective_combo_limit(
        config_limit=None,
        per_leg_limit=100,
        options=None,
        pagination=None,
        snapshot=None,
        result_snapshot=snap,
        rs_spec=object(),
    )
    assert cap == 8_000

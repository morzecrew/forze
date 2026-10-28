"""The analytics differential's own controls — it must agree when the rows agree, and fail loudly.

Run mock-against-mock here: the "real" leg is a mock whose resolver is the reference, so the
helper's comparison is what is under test, not an engine. The Postgres leg lives in the
integration suite and runs the same plan and query against real SQL.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel

from forze.application.execution import ExecutionContext
from forze_dst.conformance import RowDifference, compare_analytics_query
from forze_mock import (
    MockAnalyticsRegistry,
    MockAnalyticsResolver,
    MockAnalyticsView,
    MockDepsModule,
)
from forze_mock.seeding import apply_seed
from tests.support.analytics_resolver_conformance import (
    PARAMS,
    PLAN,
    TOTALS,
    drifting_resolver,
    matching_resolver,
)
from tests.support.execution_context import context_from_modules

# ----------------------- #


def _ctx(resolver: MockAnalyticsResolver) -> ExecutionContext:
    return context_from_modules(
        MockDepsModule(analytics=MockAnalyticsRegistry().on(TOTALS, "by_customer", resolver))
    )


async def _seed(ctx: ExecutionContext) -> None:
    await apply_seed(ctx, PLAN)


def _fixed(*rows: dict[str, Any]) -> MockAnalyticsResolver:
    def resolver(view: MockAnalyticsView, params: BaseModel) -> Iterable[dict[str, Any]]:
        return rows

    return resolver


# ....................... #


async def test_a_resolver_that_matches_its_reference_agrees() -> None:
    comparison = await compare_analytics_query(
        TOTALS,
        "by_customer",
        PARAMS,
        mock=_ctx(matching_resolver),
        real=_ctx(matching_resolver),
        seed=_seed,
    )

    assert comparison.agrees
    assert sorted(row["customer"] for row in comparison.real_rows) == ["ada", "cy"]
    assert comparison.render().endswith("mock and real agree on 2 row(s)")


async def test_a_drifting_resolver_is_reported_row_by_row() -> None:
    comparison = await compare_analytics_query(
        TOTALS,
        "by_customer",
        PARAMS,
        mock=_ctx(drifting_resolver),
        real=_ctx(matching_resolver),
        seed=_seed,
    )

    assert not comparison.agrees
    assert comparison.differences == (
        RowDifference(position=None, mock={"customer": "bob", "total": 7}, real=None),
    )
    assert comparison.render().splitlines() == [
        (
            "analytics 'order_totals' query 'by_customer' (unordered): 1 differing row(s); "
            "mock returned 3, real returned 2"
        ),
        '  only on mock: {"customer": "bob", "total": 7}',
    ]


async def test_order_counts_only_when_the_query_declares_one() -> None:
    ada, bob = {"customer": "ada", "total": 1}, {"customer": "bob", "total": 2}
    mock, real = _ctx(_fixed(bob, ada)), _ctx(_fixed(ada, bob))

    unordered = await compare_analytics_query(TOTALS, "by_customer", PARAMS, mock=mock, real=real)
    ordered = await compare_analytics_query(
        TOTALS, "by_customer", PARAMS, mock=mock, real=real, ordered=True
    )

    assert unordered.agrees
    assert [difference.render() for difference in ordered.differences] == [
        'row 0: mock {"customer": "bob", "total": 2} != real {"customer": "ada", "total": 1}',
        'row 1: mock {"customer": "ada", "total": 1} != real {"customer": "bob", "total": 2}',
    ]


async def test_rows_are_compared_as_a_multiset() -> None:
    ada = {"customer": "ada", "total": 1}

    comparison = await compare_analytics_query(
        TOTALS, "by_customer", PARAMS, mock=_ctx(_fixed(ada)), real=_ctx(_fixed(ada, ada))
    )

    assert comparison.differences == (RowDifference(position=None, mock=None, real=ada),)
    assert comparison.render().splitlines()[1] == '  only on real: {"customer": "ada", "total": 1}'


async def test_an_ordered_comparison_reports_a_missing_tail() -> None:
    ada = {"customer": "ada", "total": 1}

    comparison = await compare_analytics_query(
        TOTALS, "by_customer", PARAMS, mock=_ctx(_fixed()), real=_ctx(_fixed(ada)), ordered=True
    )

    assert [difference.render() for difference in comparison.differences] == [
        'row 0: only on real: {"customer": "ada", "total": 1}',
    ]


async def test_the_unordered_report_does_not_follow_the_engine_s_row_order() -> None:
    ada, bob = {"customer": "ada", "total": 1}, {"customer": "bob", "total": 2}

    comparisons = [
        await compare_analytics_query(
            TOTALS, "by_customer", PARAMS, mock=_ctx(_fixed()), real=_ctx(_fixed(*rows))
        )
        for rows in ((ada, bob), (bob, ada))
    ]

    assert comparisons[0].render() == comparisons[1].render()
    assert [difference.real for difference in comparisons[1].differences] == [ada, bob]


async def test_two_empty_answers_do_not_agree_by_default() -> None:
    comparison = await compare_analytics_query(
        TOTALS, "by_customer", PARAMS, mock=_ctx(_fixed()), real=_ctx(_fixed())
    )

    assert comparison.differences == ()
    assert not comparison.agrees
    assert "both engines returned no rows" in comparison.render()
    assert "allow_empty=True" in comparison.render()


async def test_two_empty_answers_agree_when_empty_is_the_right_answer() -> None:
    comparison = await compare_analytics_query(
        TOTALS,
        "by_customer",
        PARAMS,
        mock=_ctx(_fixed()),
        real=_ctx(_fixed()),
        allow_empty=True,
    )

    assert comparison.agrees
    assert comparison.render().endswith("mock and real agree on 0 row(s)")

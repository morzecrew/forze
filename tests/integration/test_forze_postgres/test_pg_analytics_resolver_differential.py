"""The analytics resolver differential, run against real Postgres.

One seed plan is loaded through the document write path into the mock and into a Postgres
table; the same analytics query then runs on both — answered by a mock resolver on one side and
by SQL on the other — and the rows are diffed. A resolver that agrees with its SQL passes; one
that has drifted from it (a forgotten ``HAVING``) fails with the row that gave it away.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.execution import ExecutionContext
from forze_dst.conformance import RowDifference, compare_analytics_query
from forze_mock import MockAnalyticsRegistry, MockAnalyticsResolver, MockDepsModule
from forze_mock.seeding import apply_seed
from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.execution.deps.configs import (
    PostgresAnalyticsConfig,
    PostgresDocumentConfig,
    PostgresQueryConfig,
)
from forze_postgres.kernel.client import PostgresClient
from tests.support.analytics_resolver_conformance import (
    BY_CUSTOMER_SQL,
    ORDERS,
    PARAMS,
    PLAN,
    TOTALS,
    drifting_resolver,
    matching_resolver,
)
from tests.support.execution_context import context_from_deps, context_from_modules

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #


async def _postgres(pg_client: PostgresClient) -> ExecutionContext:
    table = f"orders_{uuid4().hex[:10]}"

    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            customer text NOT NULL,
            amount integer NOT NULL
        );
        """
    )

    return context_from_deps(
        PostgresDepsModule(
            client=pg_client,
            rw_documents={
                str(ORDERS.name): PostgresDocumentConfig(
                    read=("public", table),
                    write=("public", table),
                    bookkeeping_strategy="application",
                ),
            },
            analytics={
                str(TOTALS.name): PostgresAnalyticsConfig(
                    queries={
                        "by_customer": PostgresQueryConfig(
                            sql=BY_CUSTOMER_SQL.format(table=f"public.{table}"),
                        ),
                    },
                ),
            },
        )()
    )


def _mock(resolver: MockAnalyticsResolver) -> ExecutionContext:
    return context_from_modules(
        MockDepsModule(analytics=MockAnalyticsRegistry().on(TOTALS, "by_customer", resolver))
    )


async def _seed(ctx: ExecutionContext) -> None:
    await apply_seed(ctx, PLAN)


# ....................... #


async def test_a_resolver_that_matches_its_sql_agrees(pg_client: PostgresClient) -> None:
    comparison = await compare_analytics_query(
        TOTALS,
        "by_customer",
        PARAMS,
        mock=_mock(matching_resolver),
        real=await _postgres(pg_client),
        seed=_seed,
    )

    assert comparison.agrees, comparison.render()
    assert sorted(row["customer"] for row in comparison.real_rows) == ["ada", "cy"]


async def test_a_resolver_that_drifted_from_its_sql_fails(pg_client: PostgresClient) -> None:
    comparison = await compare_analytics_query(
        TOTALS,
        "by_customer",
        PARAMS,
        mock=_mock(drifting_resolver),
        real=await _postgres(pg_client),
        seed=_seed,
    )

    assert comparison.differences == (
        RowDifference(position=None, mock={"customer": "bob", "total": 7}, real=None),
    )

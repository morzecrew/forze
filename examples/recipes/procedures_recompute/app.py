"""Recipe: governed batch recompute with the procedures port.

Ingest a large batch, then run **one** set-based statement over it — instead of per-row triggers
that overload the database. The handler-facing surface is a :class:`ProcedureSpec`: typed params
in, a typed :class:`ExecResult` out, command-only (refused in a read-only operation). It says
nothing about SQL or the backend.

In production the spec maps to one Postgres set-based statement (see ``PROCEDURE_CONFIG`` below);
here it runs in-process on the mock, which models the recompute over ``MockState`` so the recipe is
test-backed without Docker. The same handler code runs unchanged against either.

Run it:  uv run python -m examples.recipes.procedures_recompute.app
Exercised by tests/unit/test_examples/test_procedures_recompute.py.
"""

from __future__ import annotations

import asyncio

from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.application.contracts.procedure import ExecResult, ProcedureSpec
from forze.application.execution import DepsRegistry, ExecutionContext
from forze_mock import MockDepsModule, MockProcedureRegistry, MockState


# --8<-- [start:spec]
class Sale(BaseModel):
    region: str
    amount: int


class RegionTotal(BaseModel):
    region: str
    total: int


class RegionQuery(BaseModel):
    pass


class RecomputeWindow(BaseModel):
    since: str = "2026-01-01"


# The batch lands via analytics append; the read side serves recomputed per-region totals.
SALES_SPEC = AnalyticsSpec[RegionTotal, Sale](
    name="sales",
    read=RegionTotal,
    queries={"by_region": AnalyticsQueryDefinition(params=RegionQuery)},
    ingest=Sale,
)

# The procedure is the whole handler-facing surface: typed params, no result model means
# side-effect only (returns an affected-row count). Command-only — it mutates/computes.
RECOMPUTE_SPEC = ProcedureSpec[RecomputeWindow, None](
    name="recompute_region_totals",
    params=RecomputeWindow,
)
# --8<-- [end:spec]


# In production the spec maps to ONE set-based Postgres statement that recomputes every region in a
# single pass over the freshly-ingested batch — e.g. a function or `INSERT ... SELECT ... GROUP BY`:
#
#   from forze_postgres import PostgresProcedureConfig, PostgresDepsModule
#
#   PROCEDURE_CONFIG = PostgresProcedureConfig(sql="SELECT recompute_region_totals(%(since)s)")
#   module = PostgresDepsModule(
#       client=client,
#       procedures={"recompute_region_totals": PROCEDURE_CONFIG},
#   )
#
# The handlers below call `ctx.procedure.command(RECOMPUTE_SPEC).run(...)` unchanged.


# --8<-- [start:recompute-model]
def _recompute_region_totals(
    params: RecomputeWindow,
    state: MockState,
) -> ExecResult[None]:
    """Model the set-based recompute the production SQL performs: read the ingested batch,
    aggregate per region in one pass, materialize the read model. Returns the rows written.
    """

    sales = state.analytics_ingest_log.get("sales", [])

    totals: dict[str, int] = {}
    for row in sales:
        totals[row["region"]] = totals.get(row["region"], 0) + int(row["amount"])

    state.analytics_query_hits["sales"] = {
        "by_region": [{"region": r, "total": t} for r, t in sorted(totals.items())]
    }

    return ExecResult(affected_count=len(totals))


# --8<-- [end:recompute-model]


def build_context() -> ExecutionContext:
    # The mock answers the procedure with the handler above; in production this is wired to the
    # PostgresProcedureConfig shown earlier and the handler code does not change.
    registry = MockProcedureRegistry().on(
        "recompute_region_totals",
        _recompute_region_totals,  # pyright: ignore[reportArgumentType]
    )
    module = MockDepsModule(procedures=registry)
    return ExecutionContext(deps=DepsRegistry.from_modules(module).freeze().resolve())


# --8<-- [start:flow]
async def ingest_sales(ctx: ExecutionContext, sales: list[Sale]) -> None:
    # Append-only ingest of the batch (stands in for a large bulk load).
    await ctx.analytics.ingest(SALES_SPEC).append(sales)


async def recompute(ctx: ExecutionContext, since: str = "2026-01-01") -> int:
    # ONE governed set-based statement over the whole batch — no per-row triggers.
    result = await ctx.procedure.command(RECOMPUTE_SPEC).run(
        RecomputeWindow(since=since)
    )
    return result.affected_count or 0


async def region_totals(ctx: ExecutionContext) -> list[RegionTotal]:
    page = await ctx.analytics.query(SALES_SPEC).run("by_region", RegionQuery())
    return list(page.hits)


# --8<-- [end:flow]


async def main() -> None:
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

    written = await recompute(ctx)
    print(f"recomputed {written} regions in one statement")

    for row in await region_totals(ctx):
        print(f"{row.region}: {row.total}")


if __name__ == "__main__":
    asyncio.run(main())

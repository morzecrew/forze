"""Recipe: bound query parameters — feed a value to logic *deep inside* a read source.

A leaderboard ranks players by score, but only over results recorded on or before an "as of"
date. The rank is a window function computed **inside** the view, over the as-of-filtered set —
so an outer ``WHERE recorded_on <= as_of`` can't reproduce it (it would filter *after* ranking and
hand back wrong ranks). The date has to drive logic the result filter can't reach.

That's the gap :meth:`with_parameters` fills. The read resource declares a typed query-parameter
contract; the handler binds a value through ``ctx.document.query(spec).with_parameters(AsOf(...))``;
the backend applies it as a query-scoped session setting the relation reads internally. On Postgres
that's a GUC: ``SET LOCAL forze.as_of = '…'`` inside the read transaction, read by
``current_setting('forze.as_of')::date`` deep in the view (see ``POSTGRES_VIEW`` below). The view
stays a normal relation, so the full document DSL — filter by region, sort, paginate, count —
composes on top, unchanged.

Distinct from analytics (binds ``%(param)s`` but loses the document DSL) and from an outer filter
(can't reach inside the relation). Here it runs in-process on the mock, which models the as-of
ranking over a registered source so the recipe is test-backed without Docker; the same handler code
runs unchanged against Postgres.

Run it:  uv run python -m examples.recipes.bound_query_parameters.app
Exercised by tests/unit/test_examples/test_bound_query_parameters.py.
"""

from __future__ import annotations

import asyncio
from datetime import date

from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.execution import DepsRegistry, ExecutionContext
from forze_mock import MockDepsModule, MockQueryParamsRegistry, MockState


# --8<-- [start:spec]
class Standing(BaseModel):
    region: str
    player: str
    score: int
    rank: int  # computed by a window function *inside* the view, over the as-of set


class AsOf(BaseModel):
    as_of: date  # the parameter the view reads internally to bound its ranking


# The read resource declares its query-parameter contract; the relation name and the GUC stay in
# the wiring. The full read DSL still applies on top of the parametrized source.
STANDINGS_SPEC = DocumentSpec(
    name="standings",
    read=Standing,
    write=None,
    query_params=AsOf,
)
# --8<-- [end:spec]


# In production STANDINGS_SPEC maps to a plain Postgres view that reads the parameter deep inside,
# where an outer WHERE can't reach — the rank must be computed over the as-of-filtered set:
#
#   POSTGRES_VIEW = '''
#   CREATE VIEW standings AS
#   SELECT region, player, score,
#          rank() OVER (PARTITION BY region ORDER BY score DESC) AS rank
#   FROM results
#   WHERE recorded_on <= current_setting('forze.as_of')::date;   -- the bound parameter
#   '''
#
# The read adapter, when params are bound, opens or JOINS a transaction and emits
# `SET LOCAL forze.as_of = '2026-03-01'` (value sql.Literal-escaped) before the governed SELECT.
# The handler below does not change between the mock and Postgres.


# --8<-- [start:source-model]
# The raw results a parametrized view would rank over (region, player, score, recorded_on).
_RESULTS: list[tuple[str, str, int, date]] = [
    ("eu", "ana", 30, date(2026, 1, 10)),
    ("eu", "bo", 50, date(2026, 2, 20)),
    ("eu", "cy", 40, date(2026, 4, 5)),  # after a March as-of cutoff
    ("us", "dot", 70, date(2026, 1, 15)),
    ("us", "el", 60, date(2026, 3, 1)),
]


def _standings_as_of(params: BaseModel, state: MockState) -> list[Standing]:
    """Model what the parametrized view yields: filter results to the as-of date, then rank per
    region by score — the ranking lives *inside* the source, so it sees only the as-of set.
    """

    as_of = params.as_of  # type: ignore[attr-defined]  # bound AsOf, validated against the spec

    eligible = [r for r in _RESULTS if r[3] <= as_of]
    rows: list[Standing] = []
    for region in {r[0] for r in eligible}:
        ranked = sorted(
            (r for r in eligible if r[0] == region), key=lambda r: r[2], reverse=True
        )
        for position, (reg, player, score, _) in enumerate(ranked, start=1):
            rows.append(Standing(region=reg, player=player, score=score, rank=position))
    return rows


# --8<-- [end:source-model]


def build_context() -> ExecutionContext:
    # The mock answers the parametrized read with the source above; in production this is the
    # Postgres view shown earlier and the handler code does not change.
    sources = MockQueryParamsRegistry().on("standings", _standings_as_of)
    module = MockDepsModule(query_param_sources=sources)
    return ExecutionContext(deps=DepsRegistry.from_modules(module).freeze().resolve())


# --8<-- [start:flow]
async def regional_top(
    ctx: ExecutionContext, region: str, as_of: date
) -> list[Standing]:
    # Bind the parameter once, then read with the full document DSL on top: the view ranks over the
    # as-of set internally; here we just filter to one region and order by the rank it computed.
    page = await (
        ctx.document.query(STANDINGS_SPEC)
        .with_parameters(AsOf(as_of=as_of))
        .find_many(filters={"$values": {"region": region}}, sorts={"rank": "asc"})
    )
    return list(page.hits)


# --8<-- [end:flow]


async def main() -> None:
    ctx = build_context()

    # As of March, eu's late entry (cy, recorded in April) isn't ranked yet.
    march = await regional_top(ctx, "eu", date(2026, 3, 1))
    print("eu as of 2026-03-01:")
    for s in march:
        print(f"  #{s.rank} {s.player} ({s.score})")

    # As of May, cy is in and reshuffles the ranks — recomputed inside the source, not by us.
    may = await regional_top(ctx, "eu", date(2026, 5, 1))
    print("eu as of 2026-05-01:")
    for s in may:
        print(f"  #{s.rank} {s.player} ({s.score})")


if __name__ == "__main__":
    asyncio.run(main())

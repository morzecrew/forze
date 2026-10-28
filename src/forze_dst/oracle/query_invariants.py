"""An invariant stated as an analytics query — one law for the DST oracle and the production check.

Apps keep integrity checks as one query over the real store ("orders whose captured amount
exceeds their total"), run on a schedule. Under simulation the same law had to be restated by hand
in Python, with nothing keeping the two in agreement. :func:`query_invariant` runs the query
itself: on the mock it is answered by the query's resolver (``MockAnalyticsRegistry``), in
production by its SQL, and the predicate over the rows is the one function both share. The
analytics differential (``forze_dst.conformance.compare_analytics_query``) is what keeps the
resolver honest to that SQL.

Final-state only, like :func:`~forze_dst.oracle.system_invariants.compile_oracle` without
``per_commit``: the observe hook runs the query once over a clean context after all work, so a
violation a later transaction healed is not seen.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from pydantic import BaseModel, ValidationError

from forze.application.contracts.analytics import AnalyticsSpec
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc

from .invariants import Violation, named
from .recorder import History, record_event
from .system_invariants import CompiledOracle

# ----------------------- #

QUERY_INVARIANT_KIND = "query_invariant"
"""The recorder event kind the observe hook stamps with each query's outcome."""

_SHOWN_ROWS = 3
"""How many returned rows a violation message quotes."""


# ....................... #


def query_invariant(
    spec: AnalyticsSpec[Any, Any],
    query_key: str,
    *,
    holds: Callable[[Sequence[Any]], bool],
    params: BaseModel | None = None,
    name: str | None = None,
) -> CompiledOracle:
    """Check that *holds* accepts the rows *query_key* returns in final state.

    Wire it like any compiled oracle: ``Simulation(..., observe=oracle.observe,
    invariants=[*oracle.invariants])``.

    :param spec: The analytics spec that declares the query.
    :param query_key: The query whose rows the law is stated over.
    :param holds: The law: receives the rows (read-model instances) and returns whether it
        holds. A query that lists violations takes ``lambda rows: not rows``.
    :param params: Params for the run; defaults to the query's params model built with no
        arguments.
    :param name: The invariant's name; defaults to ``"<spec>.<query_key>"``. Give each
        oracle a distinct one: its invariant reads every recorded outcome under its name, so
        two oracles sharing a name each report the other's failures too. Names meet only where
        you combine the invariants into a simulation, which rejects duplicates when invariant
        accounting is on and cannot otherwise tell them apart.
    :raises exc.configuration: when *spec* declares no query named *query_key*, or *params*
        is omitted and the query's params model has required fields.
    """

    if query_key not in spec.queries:
        raise exc.configuration(
            f"query_invariant: analytics spec {str(spec.name)!r} declares no query {query_key!r}.",
            code="unknown_analytics_query",
        )

    if params is None:
        try:
            params = spec.queries[query_key].params()

        except ValidationError as error:
            raise exc.configuration(
                f"query_invariant: query {str(spec.name)!r}/{query_key!r} has required params; "
                "pass params=.",
                code="query_invariant_params_required",
            ) from error

    bound = params
    law = name or f"{spec.name}.{query_key}"

    async def observe(ctx: ExecutionContext) -> None:
        page = await ctx.analytics.query(spec).run(query_key, bound)
        record_event(
            QUERY_INVARIANT_KIND,
            name=law,
            held=bool(holds(page.hits)),
            rows=[hit.model_dump(mode="json") for hit in page.hits],
        )

    def _check(history: History) -> list[Violation]:
        return [
            Violation(
                invariant=law,
                message=(
                    f"query invariant {law!r} failed: {query_key!r} returned "
                    f"{len(event.fields['rows'])} row(s), first "
                    f"{event.fields['rows'][:_SHOWN_ROWS]}"
                ),
                events=(event,),
            )
            for event in history.of_kind(QUERY_INVARIANT_KIND)
            if event.fields.get("name") == law and not event.fields["held"]
        ]

    return CompiledOracle(observe=observe, invariants=(named(law, _check),))

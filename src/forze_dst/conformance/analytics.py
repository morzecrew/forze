"""The analytics differential — a mock resolver checked against the SQL it stands in for.

A resolver registered on the mock (``MockAnalyticsRegistry``) is a query's second implementation;
the first is the warehouse SQL. Nothing keeps the two in agreement, so a resolver that drifts from
its SQL passes every simulation and attests a law production does not check.
:func:`compare_analytics_query` closes that: it loads one seed into two execution contexts — the
mock and a real engine — runs the same query on both, and diffs the rows.

Rows are compared after the read model, as JSON. The port's output is the contract, so a
``Decimal`` from one engine and an ``int`` from the other agree when the read model types the
field. Order is ignored unless the caller says the query orders its rows: SQL without an
``ORDER BY`` promises none, and a resolver written over a dict has no reason to match the planner.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Awaitable, Callable, Sequence
from itertools import zip_longest
from typing import Any

import attrs
from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsSpec
from forze.application.execution import ExecutionContext
from forze.base.primitives import JsonDict

# ----------------------- #


@attrs.frozen(kw_only=True)
class RowDifference:
    """One row the two engines disagree on."""

    position: int | None
    """The row's index in an ordered comparison; ``None`` when order is ignored."""

    mock: JsonDict | None
    """The mock's row, or ``None`` when only the real engine returned one."""

    real: JsonDict | None
    """The real engine's row, or ``None`` when only the mock returned one."""

    def render(self) -> str:
        at = "" if self.position is None else f"row {self.position}: "

        if self.real is None:
            return f"{at}only on mock: {_text(self.mock)}"

        if self.mock is None:
            return f"{at}only on real: {_text(self.real)}"

        return f"{at}mock {_text(self.mock)} != real {_text(self.real)}"


# ....................... #


@attrs.frozen(kw_only=True)
class AnalyticsComparison:
    """The rows one query returned on the mock and on the real engine, and where they differ."""

    spec: str
    query_key: str
    ordered: bool
    mock_rows: tuple[JsonDict, ...]
    real_rows: tuple[JsonDict, ...]
    differences: tuple[RowDifference, ...]

    @property
    def agrees(self) -> bool:
        return not self.differences

    def render(self) -> str:
        """A readable report: one header line, then one line per differing row."""

        mode = "ordered" if self.ordered else "unordered"
        head = f"analytics {self.spec!r} query {self.query_key!r} ({mode}): "

        if self.agrees:
            return f"{head}mock and real agree on {len(self.real_rows)} row(s)"

        summary = (
            f"{head}{len(self.differences)} differing row(s); mock returned "
            f"{len(self.mock_rows)}, real returned {len(self.real_rows)}"
        )

        return "\n".join([summary, *(f"  {diff.render()}" for diff in self.differences)])


# ....................... #


async def compare_analytics_query(
    spec: AnalyticsSpec[Any, Any],
    query_key: str,
    params: BaseModel,
    *,
    mock: ExecutionContext,
    real: ExecutionContext,
    seed: Callable[[ExecutionContext], Awaitable[object]] | None = None,
    ordered: bool = False,
) -> AnalyticsComparison:
    """Run *query_key* on *mock* and on *real*, after loading the same seed into both.

    :param spec: The analytics spec both contexts route.
    :param query_key: The query to run.
    :param params: Params passed unchanged to both runs.
    :param mock: A context whose analytics port is the mock, with the resolver under test.
    :param real: A context whose analytics port is the real engine.
    :param seed: Loads the shared data into a context, e.g.
        ``lambda ctx: apply_seed(ctx, plan)`` with ``forze_mock.seeding``. Called on *mock*
        first, then on *real*. ``None`` when both are seeded already.
    :param ordered: Compare row by row in order. Pass it only when the query declares an
        order (an ``ORDER BY``); by default rows are compared as a multiset.
    :returns: The comparison; :attr:`AnalyticsComparison.agrees` is the verdict and
        :meth:`AnalyticsComparison.render` the report.
    """

    if seed is not None:
        await seed(mock)
        await seed(real)

    mock_rows = await _rows(mock, spec, query_key, params)
    real_rows = await _rows(real, spec, query_key, params)
    differences = (
        _ordered_differences(mock_rows, real_rows)
        if ordered
        else _unordered_differences(mock_rows, real_rows)
    )

    return AnalyticsComparison(
        spec=str(spec.name),
        query_key=query_key,
        ordered=ordered,
        mock_rows=mock_rows,
        real_rows=real_rows,
        differences=differences,
    )


# ....................... #


async def _rows(
    ctx: ExecutionContext,
    spec: AnalyticsSpec[Any, Any],
    query_key: str,
    params: BaseModel,
) -> tuple[JsonDict, ...]:
    page = await ctx.analytics.query(spec).run(query_key, params)

    return tuple(hit.model_dump(mode="json") for hit in page.hits)


def _text(row: JsonDict | None) -> str:
    return json.dumps(row, sort_keys=True)


def _ordered_differences(
    mock_rows: Sequence[JsonDict],
    real_rows: Sequence[JsonDict],
) -> tuple[RowDifference, ...]:
    return tuple(
        RowDifference(position=index, mock=mock_row, real=real_row)
        for index, (mock_row, real_row) in enumerate(zip_longest(mock_rows, real_rows))
        if mock_row != real_row
    )


def _unordered_differences(
    mock_rows: Sequence[JsonDict],
    real_rows: Sequence[JsonDict],
) -> tuple[RowDifference, ...]:
    only_mock = _surplus(mock_rows, real_rows)
    only_real = _surplus(real_rows, mock_rows)

    return (
        *(RowDifference(position=None, mock=row, real=None) for row in only_mock),
        *(RowDifference(position=None, mock=None, real=row) for row in only_real),
    )


def _surplus(rows: Sequence[JsonDict], other: Sequence[JsonDict]) -> list[JsonDict]:
    """The rows of *rows* left over once each is matched to an equal row of *other*."""

    unmatched = Counter(_text(row) for row in other)
    surplus: list[JsonDict] = []

    for row in rows:
        key = _text(row)

        if unmatched[key]:
            unmatched[key] -= 1
        else:
            surplus.append(row)

    return surplus

"""The effective-dated filters say what `Period` says, on every convention and every boundary.

The kit's two reads are `Period.contains` and `Period.overlaps` written in the filter language,
and nothing makes the two halves agree except this. A divergence is not a wrong row count — it
is a caller holding a period, asking the store which row is in force, and being told about a
different one than the value object would have named.

Driven through the in-memory store rather than by evaluating the expressions by hand, because
the question is what the *store* answers when given the filter.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.base.exceptions import CoreException
from forze.base.primitives import Bounds, Period
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_kits.aggregates.temporal.handlers import effective_on_filter, timeline_filter
from forze_kits.domain.temporal import DocWithTemporal
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

pytestmark = [pytest.mark.asyncio]

# ----------------------- #

BOUNDS: tuple[Bounds, ...] = ("[)", "[]", "(]", "()")

KEY = ("owner",)
OWNER = "o1"


class _Row(Document):
    owner: str
    valid_from: date
    valid_to: date | None = None


class _RowRead(ReadDocument):
    owner: str
    valid_from: date
    valid_to: date | None = None


class _RowCreate(CreateDocumentCmd):
    owner: str
    valid_from: date
    valid_to: date | None = None


class _RowUpdate(BaseDTO):
    valid_to: date | None = None


def _spec() -> DocumentSpec[_RowRead, _Row, _RowCreate, _RowUpdate]:
    return DocumentSpec[_RowRead, _Row, _RowCreate, _RowUpdate](
        name="spans",
        read=_RowRead,
        write=DocumentWriteTypes(domain=_Row, create_cmd=_RowCreate, update_cmd=_RowUpdate),
    )


D0 = date(2026, 3, 1)


def _d(offset: int) -> date:
    return D0 + timedelta(days=offset)


# Deliberately including a one-day period, a touching pair and an open end: the cases the four
# conventions disagree about are all boundary cases, so a grid of comfortably-separated periods
# would agree under every convention and prove nothing.
PERIODS: tuple[tuple[int, int | None], ...] = (
    (0, 0),
    (0, 2),
    (2, 4),
    (4, 4),
    (2, None),
    (5, 9),
)

DAYS = tuple(range(-1, 11))


def _storable(bounds: Bounds) -> tuple[tuple[int, int | None], ...]:
    """The grid's periods that this convention admits.

    A single-day span is a real period under ``"[]"`` and an empty one under the other three,
    and the kit refuses to write an empty period at all — so the row a convention cannot hold
    is not one this comparison should expect the store to answer with.
    """

    return tuple(p for p in PERIODS if not _period(*p, bounds).is_empty)


async def _seeded(bounds: Bounds) -> tuple[Any, DocumentSpec[Any, Any, Any, Any]]:
    ctx = context_from_modules(MockDepsModule())
    spec = _spec()
    command = ctx.doc.command(spec)

    for start, end in _storable(bounds):
        await command.create(
            _RowCreate(
                owner=OWNER,
                valid_from=_d(start),
                valid_to=None if end is None else _d(end),
            )
        )

    # A row under another key, which must never appear in either answer.
    await command.create(_RowCreate(owner="other", valid_from=_d(0), valid_to=_d(9)))

    return ctx, spec


def _period(start: int, end: int | None, bounds: Bounds) -> Period[date]:
    return Period(_d(start), None if end is None else _d(end), bounds)


def _expected_on(day: int, bounds: Bounds) -> set[tuple[int, int | None]]:
    return {p for p in _storable(bounds) if _period(*p, bounds).contains(_d(day))}


def _found(rows: list[Any]) -> set[tuple[int, int | None]]:
    return {
        ((row.valid_from - D0).days, None if row.valid_to is None else (row.valid_to - D0).days)
        for row in rows
    }


# ....................... #


class TestEffectiveOnIsPeriodContains:
    @pytest.mark.parametrize("bounds", BOUNDS)
    async def test_every_day_matches_the_value_object(self, bounds: Bounds) -> None:
        ctx, spec = await _seeded(bounds)
        query = ctx.doc.query(spec)

        for day in DAYS:
            page = await query.find_page(
                filters=effective_on_filter(KEY, {"owner": OWNER}, _d(day), bounds),
            )

            assert _found(list(page.hits)) == _expected_on(day, bounds), (
                f"bounds={bounds} day={day}"
            )

    @pytest.mark.parametrize("bounds", BOUNDS)
    async def test_the_grid_is_not_vacuous(self, bounds: Bounds) -> None:
        # A comparison that matched empty against empty on every day would pass while proving
        # nothing, and a grid of separated periods would agree under all four conventions.
        matched = [day for day in DAYS if _expected_on(day, bounds)]

        assert len(matched) > 5, f"bounds={bounds} covers almost no day"

    async def test_the_conventions_actually_disagree(self) -> None:
        # The premise of parametrising over four conventions: if they all answered alike, this
        # file would be one test repeated four times.
        answers = {bounds: frozenset(map(repr, _expected_on(4, bounds))) for bounds in BOUNDS}

        assert len(set(answers.values())) > 1, answers


# ....................... #


class TestTimelineIsPeriodOverlaps:
    @pytest.mark.parametrize("bounds", BOUNDS)
    @pytest.mark.parametrize("window", [(0, 0), (1, 3), (3, 3), (4, 6), (-1, 11), (6, None)])
    async def test_every_window_matches_the_value_object(
        self,
        bounds: Bounds,
        window: tuple[int, int | None],
    ) -> None:
        ctx, spec = await _seeded(bounds)
        query = ctx.doc.query(spec)
        start, end = window

        if _period(start, end, bounds).is_empty:
            pytest.skip(f"an empty window is refused under {bounds}, covered on its own leg")

        page = await query.find_page(
            filters=timeline_filter(
                KEY, {"owner": OWNER}, _d(start), None if end is None else _d(end), bounds
            ),
        )

        expected = {
            p
            for p in _storable(bounds)
            if _period(*p, bounds).overlaps(_period(start, end, bounds))
        }

        assert _found(list(page.hits)) == expected, f"bounds={bounds} window={window}"


# ....................... #


class TestTheKeyIsMatchedExactly:
    async def test_a_row_under_another_key_never_appears(self) -> None:
        ctx, spec = await _seeded("[]")
        query = ctx.doc.query(spec)

        page = await query.find_page(
            filters=timeline_filter(KEY, {"owner": OWNER}, _d(-5), _d(20), "[]"),
        )

        assert all(row.owner == OWNER for row in page.hits)
        assert len(list(page.hits)) == len(PERIODS)

    async def test_a_missing_key_field_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            effective_on_filter(("owner", "kind"), {"owner": OWNER}, _d(0), "[]")

        assert caught.value.kind.value == "validation"

    async def test_an_extra_key_field_is_refused(self) -> None:
        with pytest.raises(CoreException):
            effective_on_filter(KEY, {"owner": OWNER, "kind": "x"}, _d(0), "[]")


# ....................... #


class TestAPeriodInForceOnNoDayIsRefused:
    """The row the filter language cannot exclude, refused where it is written instead.

    An empty period overlaps nothing, and "overlaps nothing" is a fact about the row's two
    fields compared with each other — which a filter cannot say, since it compares a field to a
    value. So the store never holds one, and the filter never has to.
    """

    @pytest.mark.parametrize("bounds", ["[)", "(]", "()"])
    async def test_an_empty_row_cannot_be_written(self, bounds: Bounds) -> None:
        model = type("Empty", (DocWithTemporal,), {"temporal_bounds": bounds})

        with pytest.raises(CoreException) as caught:
            model(valid_from=_d(3), valid_to=_d(3))

        assert caught.value.kind.value == "domain"

    async def test_a_single_day_period_is_fine_when_the_end_is_in_force(self) -> None:
        # The contrast: under `[]` the same two dates are an ordinary one-day period.
        model = type("Closed", (DocWithTemporal,), {"temporal_bounds": "[]"})

        assert model(valid_from=_d(3), valid_to=_d(3)).valid_to == _d(3)

    @pytest.mark.parametrize("bounds", BOUNDS)
    async def test_the_message_names_a_span_that_works(self, bounds: Bounds) -> None:
        # The hint has to be true under the convention it is given for, which is the kind of
        # sentence that is wrong in three of four cases when written for one of them.
        shortest = (0 if bounds[0] == "[" else 1) + (0 if bounds[1] == "]" else 1)
        period = Period(_d(0), _d(shortest), bounds)

        assert not period.is_empty
        assert sum(1 for day in DAYS if period.contains(_d(day))) == 1


# ....................... #


class TestAnEmptyWindowIsRefused:
    @pytest.mark.parametrize("bounds", ["[)", "(]", "()"])
    async def test_a_zero_width_window_is_refused(self, bounds: Bounds) -> None:
        with pytest.raises(CoreException) as caught:
            timeline_filter(KEY, {"owner": OWNER}, _d(3), _d(3), bounds)

        assert caught.value.kind.value == "validation"

    async def test_the_same_window_is_fine_when_its_endpoints_are_in_force(self) -> None:
        assert timeline_filter(KEY, {"owner": OWNER}, _d(3), _d(3), "[]")


# ....................... #


class TestTheDdlNamesTheRightRangeType:
    """The printed migration has to construct a range over the column it names.

    A `daterange` over a `timestamptz` column is a statement that does not run, and a refusal
    carrying a statement that does not run is worse than one carrying none: it sends an operator
    to the database to find out.
    """

    def test_each_column_type_gets_its_constructor(self) -> None:
        from forze_postgres.kernel.catalog.introspect import PostgresType
        from forze_postgres.kernel.catalog.validation.validate_schema import _range_function

        def _t(base: str) -> PostgresType:
            return PostgresType(base=base, is_array=False, not_null=False)

        assert _range_function(_t("date")) == "daterange"
        assert _range_function(_t("timestamptz")) == "tstzrange"
        assert _range_function(_t("timestamp with time zone")) == "tstzrange"
        assert _range_function(_t("timestamp")) == "tsrange"

    def test_an_unknown_column_falls_back_to_the_grain_the_kit_writes(self) -> None:
        from forze_postgres.kernel.catalog.validation.validate_schema import _range_function

        assert _range_function(None) == "daterange"

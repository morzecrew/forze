"""Unit tests for the period primitive.

The value of this type is entirely at its edges, so the cases are enumerated rather than
sampled: every bounds convention against every pair shape, and the shared-endpoint cases in
both directions because that is the property a wrong implementation breaks first.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, tzinfo

import pytest

from forze.base.exceptions import CoreException
from forze.base.primitives import Bounds, Period

# ----------------------- #

JAN, FEB, MAR, APR = (date(2026, month, 1) for month in (1, 2, 3, 4))

_ALL_BOUNDS: tuple[Bounds, ...] = ("[)", "[]", "(]", "()")


def _p(start: date, end: date | None = None, bounds: Bounds = "[)") -> Period[date]:
    return Period(start=start, end=end, bounds=bounds)


# ----------------------- #


class TestConstruction:
    @pytest.mark.parametrize("bounds", _ALL_BOUNDS)
    def test_it_takes_every_convention(self, bounds: Bounds) -> None:
        assert _p(JAN, FEB, bounds).bounds == bounds

    def test_the_default_is_half_open(self) -> None:
        """The convention that tiles: adjacent periods cover a timeline with no gap and no
        overlap, which is why it is the default rather than the inclusive reading."""

        assert _p(JAN, FEB).bounds == "[)"
        assert not _p(JAN, FEB).overlaps(_p(FEB, MAR))

    def test_an_open_end_is_none_not_a_sentinel(self) -> None:
        """A sentinel date would lie in every export, comparison and report that met it."""

        assert _p(JAN).end is None

    def test_an_end_before_the_start_is_refused(self) -> None:
        with pytest.raises(CoreException) as ei:
            _p(FEB, JAN)

        assert "ends before it starts" in str(ei.value)

    def test_mixed_grains_are_refused_at_construction(self) -> None:
        """`datetime` subclasses `date`, so a type checker passes a mixed pair — verified
        against mypy --strict while writing this. Left alone it survives to the first
        comparison, where the stdlib raises `TypeError` from inside whichever predicate
        touched it; refused here instead, naming both endpoints.
        """

        with pytest.raises(CoreException) as ei:
            Period(start=date(2026, 1, 1), end=datetime(2026, 2, 1))  # type: ignore[type-var]

        assert "not comparable" in str(ei.value)

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            ({"start": 1, "end": 2}, "must be a date or a datetime"),
            ({"start": "2026-01-01"}, "must be a date or a datetime"),
            ({"start": date(2026, 1, 1), "end": 7}, "must be a date or a datetime"),
            ({"start": date(2026, 1, 1), "bounds": "]["}, "is not one of"),
        ],
    )
    def test_endpoints_and_bounds_are_checked_at_runtime(
        self,
        kwargs: dict[str, object],
        expected: str,
    ) -> None:
        """The annotations guarantee nothing here: a period is built from recorded markers and
        other dynamic input as often as from typed code, and every one of these would otherwise
        surface as a stdlib `TypeError` from inside whichever predicate touched the value — past
        the boundary where a caller can report it. `Period(start=1, end=2)` is the worst case: it
        sorts and compares like a period, so it is swept as *valid*.
        """

        with pytest.raises(CoreException) as ei:
            Period(**kwargs)  # type: ignore[arg-type]

        assert expected in str(ei.value)

    def test_mixed_datetime_awareness_is_refused(self) -> None:
        """A naive and an aware `datetime` are one type, so neither the annotation nor the
        grain check sees the difference — and `<` between them raises `TypeError`."""

        with pytest.raises(CoreException) as ei:
            Period(start=datetime(2026, 1, 1), end=datetime(2026, 2, 1, tzinfo=UTC))

        assert "not comparable" in str(ei.value)
        assert "naive datetime" in str(ei.value)
        assert "aware datetime" in str(ei.value)

    def test_a_tzinfo_that_will_not_answer_is_refused(self) -> None:
        """`utcoffset()` is application code, and whether an endpoint is aware is what a sweep
        groups by — so a `tzinfo` that raises has to be refused here rather than escaping from
        whatever asks the value's grain first. Settled even for an open-ended period, which has
        no second endpoint to compare against."""

        class Unhelpful(tzinfo):
            def utcoffset(self, dt: datetime | None) -> timedelta | None:
                raise RuntimeError("no offset for you")

            def dst(self, dt: datetime | None) -> timedelta | None:
                return None

        with pytest.raises(CoreException) as ei:
            Period(start=datetime(2026, 1, 1, tzinfo=Unhelpful()))

        assert "utcoffset() failed" in str(ei.value)

    def test_an_aware_period_is_ordinary(self) -> None:
        period = Period(
            start=datetime(2026, 1, 1, tzinfo=UTC), end=datetime(2026, 2, 1, tzinfo=UTC)
        )

        assert period.contains(datetime(2026, 1, 15, tzinfo=UTC))

        with pytest.raises(CoreException):
            period.contains(datetime(2026, 1, 15))

    def test_a_datetime_period_is_ordinary(self) -> None:
        period = Period(start=datetime(2026, 1, 1, 9), end=datetime(2026, 1, 1, 17))

        assert period.contains(datetime(2026, 1, 1, 12))
        assert not period.contains(datetime(2026, 1, 1, 18))


class TestZeroLength:
    @pytest.mark.parametrize("bounds", ["[)", "(]", "()"])
    def test_it_is_empty_wherever_an_endpoint_is_excluded(self, bounds: Bounds) -> None:
        period = _p(JAN, JAN, bounds)

        assert period.is_empty
        assert not period.contains(JAN)

    def test_it_is_a_single_point_when_closed(self) -> None:
        period = _p(JAN, JAN, "[]")

        assert not period.is_empty
        assert period.contains(JAN)

    def test_it_is_accepted_rather_than_refused(self) -> None:
        """A booking cancelled in the instant it was made is a real row; refusing it would push
        the case back into every caller."""

        assert _p(JAN, JAN).end == JAN

    @pytest.mark.parametrize("bounds", ["[)", "(]", "()"])
    def test_an_empty_period_overlaps_nothing(self, bounds: Bounds) -> None:
        empty = _p(FEB, FEB, bounds)

        assert not empty.overlaps(_p(JAN, MAR, "[]"))
        assert not _p(JAN, MAR, "[]").overlaps(empty)


class TestContains:
    @pytest.mark.parametrize(
        ("bounds", "at_start", "at_end"),
        [
            ("[)", True, False),
            ("[]", True, True),
            ("(]", False, True),
            ("()", False, False),
        ],
    )
    def test_each_endpoint_counts_only_when_it_is_in_force(
        self,
        bounds: Bounds,
        at_start: bool,
        at_end: bool,
    ) -> None:
        period = _p(JAN, MAR, bounds)

        assert period.contains(JAN) is at_start
        assert period.contains(MAR) is at_end
        assert period.contains(FEB) is True

    @pytest.mark.parametrize("bounds", _ALL_BOUNDS)
    def test_a_point_outside_is_outside_under_every_convention(self, bounds: Bounds) -> None:
        period = _p(FEB, MAR, bounds)

        assert not period.contains(JAN)
        assert not period.contains(APR)

    @pytest.mark.parametrize("bounds", ["[)", "[]"])
    def test_an_open_end_contains_everything_after_the_start(self, bounds: Bounds) -> None:
        period = _p(FEB, None, bounds)

        assert period.contains(FEB)
        assert period.contains(APR)
        assert not period.contains(JAN)

    def test_an_open_end_with_an_excluded_start_excludes_only_that_point(self) -> None:
        period = _p(FEB, None, "()")

        assert not period.contains(FEB)
        assert period.contains(MAR)

    def test_a_mixed_grain_point_is_refused(self) -> None:
        with pytest.raises(CoreException) as ei:
            _p(JAN, MAR).contains(datetime(2026, 2, 1))  # type: ignore[arg-type]

        assert "mixes grains" in str(ei.value)


class TestOverlaps:
    @pytest.mark.parametrize(
        ("left", "right", "expected"),
        [
            # Disjoint, and the gap is real under every reading.
            ((JAN, FEB), (MAR, APR), False),
            # Nested, identical, and staggered.
            ((JAN, APR), (FEB, MAR), True),
            ((JAN, FEB), (JAN, FEB), True),
            ((JAN, MAR), (FEB, APR), True),
            # One open end, then both.
            ((JAN, None), (MAR, APR), True),
            ((JAN, None), (MAR, None), True),
            # An open end still starts where it starts.
            ((MAR, None), (JAN, FEB), False),
        ],
    )
    def test_pair_shapes_under_the_default_convention(
        self,
        left: tuple[date, date | None],
        right: tuple[date, date | None],
        expected: bool,
    ) -> None:
        a, b = _p(*left), _p(*right)

        assert a.overlaps(b) is expected
        assert b.overlaps(a) is expected

    @pytest.mark.parametrize(
        ("earlier_bounds", "later_bounds", "expected"),
        [
            # The shared point needs the earlier period's end *and* the later one's start.
            ("[]", "[]", True),
            ("[]", "()", False),
            ("[)", "[]", False),
            ("[)", "[)", False),
            ("(]", "[)", True),
            ("()", "(]", False),
        ],
    )
    def test_a_shared_endpoint_needs_both_sides_in_force(
        self,
        earlier_bounds: Bounds,
        later_bounds: Bounds,
        expected: bool,
    ) -> None:
        """Adjacent periods touching at one point: the mixed-convention cases are the reason
        the predicate tests each start against the other's end rather than picking a winner."""

        earlier, later = _p(JAN, FEB, earlier_bounds), _p(FEB, MAR, later_bounds)

        assert earlier.overlaps(later) is expected
        assert later.overlaps(earlier) is expected

    @pytest.mark.parametrize("bounds", _ALL_BOUNDS)
    @pytest.mark.parametrize("other_bounds", _ALL_BOUNDS)
    def test_the_predicate_is_symmetric(self, bounds: Bounds, other_bounds: Bounds) -> None:
        """Every convention against every convention, on periods that touch at a point and on
        periods that genuinely overlap. Asymmetry is the first thing a wrong implementation
        produces, and it would make an invariant's verdict depend on row order."""

        for left, right in (
            (_p(JAN, FEB, bounds), _p(FEB, MAR, other_bounds)),
            (_p(JAN, MAR, bounds), _p(FEB, APR, other_bounds)),
            (_p(JAN, None, bounds), _p(FEB, MAR, other_bounds)),
        ):
            assert left.overlaps(right) is right.overlaps(left)

    def test_an_excluded_start_does_not_meet_a_point_at_it(self) -> None:
        """The case a start-ordered implementation gets wrong: an open-ended period that
        excludes its start, against the single point at that start."""

        assert not _p(JAN, None, "()").overlaps(_p(JAN, JAN, "[]"))
        assert _p(JAN, None, "[)").overlaps(_p(JAN, JAN, "[]"))

    def test_a_mixed_grain_period_is_refused(self) -> None:
        other: Period[datetime] = Period(start=datetime(2026, 1, 1))

        with pytest.raises(CoreException) as ei:
            _p(JAN, MAR).overlaps(other)  # type: ignore[arg-type]

        assert "mixes grains" in str(ei.value)


class TestIntersects:
    def test_a_window_is_read_half_open(self) -> None:
        """A caller asking "what falls in February" has two values and no convention of its
        own, so the window is the reading that tiles."""

        assert _p(JAN, FEB, "[]").intersects(FEB, MAR) is True
        assert _p(JAN, FEB, "[)").intersects(FEB, MAR) is False

    def test_an_open_window_end_reaches_forward(self) -> None:
        assert _p(MAR, APR).intersects(JAN, None)

    def test_a_window_outside_the_period_does_not_intersect(self) -> None:
        assert not _p(JAN, FEB).intersects(MAR, APR)

    def test_a_backwards_window_is_refused(self) -> None:
        with pytest.raises(CoreException):
            _p(JAN, APR).intersects(MAR, FEB)


class TestTiling:
    def test_consecutive_half_open_periods_tile(self) -> None:
        """The property the default exists for: no gap, no overlap, and every point in exactly
        one period."""

        months = [_p(start, end) for start, end in ((JAN, FEB), (FEB, MAR), (MAR, APR))]

        for earlier, later in zip(months, months[1:], strict=False):
            assert not earlier.overlaps(later)

        for at in (JAN, FEB, MAR, date(2026, 1, 15)):
            assert sum(1 for month in months if month.contains(at)) == 1

    def test_consecutive_closed_periods_do_not(self) -> None:
        """Which is why an aggregate taking human-written dates declares `"[]"` and accepts
        that its own periods must not share an endpoint."""

        assert _p(JAN, FEB, "[]").overlaps(_p(FEB, MAR, "[]"))

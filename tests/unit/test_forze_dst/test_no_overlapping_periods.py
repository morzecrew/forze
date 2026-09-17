"""The data-level overlap oracle: no two recorded periods for one owner may overlap.

Driven from hand-built histories rather than a simulation, because what is under test is the
assertion itself — which pairs it catches, which it must not, and what it does with a marker the
workload recorded badly. The property it asserts is the one behind effective-dated master data,
bookings, shifts and leases.
"""

from __future__ import annotations

from datetime import date, datetime

from forze_dst.invariants import no_overlapping_periods
from forze_dst.oracle import Event, History

# ----------------------- #

JAN, FEB, MAR, APR, MAY = (date(2026, month, 1) for month in (1, 2, 3, 4, 5))


def _history(*fields: dict[str, object], kind: str = "shift") -> History:
    return History(
        seed=1,
        events=tuple(
            Event(seq=seq, kind=kind, at=float(seq), fields=entry)
            for seq, entry in enumerate(fields)
        ),
    )


def _shift(owner: str, start: date | datetime | None, end: date | datetime | None = None):
    entry: dict[str, object] = {"employee_id": owner}

    if start is not None:
        entry["valid_from"] = start

    if end is not None:
        entry["valid_to"] = end

    return entry


_CHECK = no_overlapping_periods(
    "shift",
    key="employee_id",
    start="valid_from",
    end="valid_to",
)

# ----------------------- #


class TestWhatItCatches:
    def test_one_overlapping_pair_is_reported_once_with_both_events(self) -> None:
        violations = _CHECK(_history(_shift("ann", JAN, MAR), _shift("ann", FEB, APR)))

        assert len(violations) == 1
        assert violations[0].invariant == "no_overlapping_periods"
        assert "employee_id='ann'" in violations[0].message
        assert len(violations[0].events) == 2

    def test_an_open_ended_period_swallows_everything_after_it(self) -> None:
        """The case a max-end sweep gets wrong if it treats a missing end as a small value."""

        violations = _CHECK(_history(_shift("ann", JAN), _shift("ann", MAR, APR)))

        assert len(violations) == 1

    def test_a_long_period_enclosing_two_short_ones_reports_both(self) -> None:
        """Why the sweep carries the period that reaches furthest rather than the latest one:
        keeping the most recently started period would forget the enclosing one."""

        violations = _CHECK(
            _history(
                _shift("ann", JAN, MAY),
                _shift("ann", FEB, MAR),
                _shift("ann", MAR, APR),
            )
        )

        assert len(violations) == 2

    def test_datetime_endpoints_are_ordinary(self) -> None:
        violations = _CHECK(
            _history(
                _shift("ann", datetime(2026, 1, 1, 9), datetime(2026, 1, 1, 17)),
                _shift("ann", datetime(2026, 1, 1, 16), datetime(2026, 1, 1, 18)),
            )
        )

        assert len(violations) == 1


class TestWhatItMustNotCatch:
    def test_a_tiling_history_is_clean(self) -> None:
        violations = _CHECK(
            _history(
                _shift("ann", JAN, FEB),
                _shift("ann", FEB, MAR),
                _shift("ann", MAR, APR),
            )
        )

        assert violations == []

    def test_two_owners_with_mirrored_periods_are_clean(self) -> None:
        """Grouping is per key: the law holds *per owner*, and a global sweep would report every
        pair of employees working the same month."""

        violations = _CHECK(
            _history(
                _shift("ann", JAN, MAR),
                _shift("bob", JAN, MAR),
                _shift("cat", JAN, MAR),
            )
        )

        assert violations == []

    def test_events_of_another_kind_are_not_read(self) -> None:
        history = History(
            seed=1,
            events=(
                Event(seq=0, kind="other", at=0.0, fields=_shift("ann", JAN, MAR)),
                Event(seq=1, kind="other", at=1.0, fields=_shift("ann", FEB, APR)),
            ),
        )

        assert _CHECK(history) == []

    def test_an_empty_history_is_clean(self) -> None:
        assert _CHECK(History(seed=1, events=())) == []


class TestTheDeclaredConvention:
    def test_touching_periods_overlap_under_the_inclusive_reading(self) -> None:
        inclusive = no_overlapping_periods(
            "shift",
            key="employee_id",
            start="valid_from",
            end="valid_to",
            bounds="[]",
        )

        touching = _history(_shift("ann", JAN, FEB), _shift("ann", FEB, MAR))

        assert len(inclusive(touching)) == 1
        assert _CHECK(touching) == []


class TestAMarkerTheWorkloadRecordedBadly:
    def test_a_missing_key_or_start_is_reported_not_raised(self) -> None:
        """An oracle that raised on a malformed marker would take the whole run's checking with
        it, and the run that recorded it is exactly the one worth reporting."""

        violations = _CHECK(
            _history({"valid_from": JAN}, _shift("ann", None, FEB), _shift("ann", JAN, FEB))
        )

        assert len(violations) == 2
        assert all("recorded without" in violation.message for violation in violations)

    def test_endpoints_a_period_cannot_be_built_from_are_reported(self) -> None:
        violations = _CHECK(
            _history(
                _shift("ann", MAR, JAN),
                _shift("bob", JAN, datetime(2026, 2, 1)),
            )
        )

        assert len(violations) == 2
        assert all("unusable period" in violation.message for violation in violations)

    def test_an_unhashable_owner_is_reported_not_raised(self) -> None:
        """`defaultdict` raises on an unhashable key, which would end the run's checking."""

        violations = _CHECK(_history({"employee_id": {"team": "a"}, "valid_from": JAN}))

        assert len(violations) == 1
        assert "cannot be grouped" in violations[0].message

    def test_markers_mixing_grains_for_one_owner_are_reported_and_still_swept(self) -> None:
        """Each marker builds a valid period, so the mismatch only surfaces when the two are
        compared — where a `date` against a `datetime` raises `TypeError` out of the sort. It is
        reported once per owner, and each grain is still swept so a real overlap survives."""

        violations = _CHECK(
            _history(
                _shift("ann", JAN, MAR),
                _shift("ann", datetime(2026, 2, 1), datetime(2026, 4, 1)),
                _shift("ann", FEB, APR),
            )
        )

        assert len(violations) == 2
        assert any("mix grains" in violation.message for violation in violations)
        assert any("overlapping periods" in violation.message for violation in violations)

    def test_an_explicit_none_end_is_open_ended(self) -> None:
        violations = _CHECK(
            _history(
                {"employee_id": "ann", "valid_from": JAN, "valid_to": None},
                _shift("ann", MAR, APR),
            )
        )

        assert len(violations) == 1
        assert "overlapping periods" in violations[0].message

    def test_a_bad_marker_does_not_hide_a_real_overlap(self) -> None:
        violations = _CHECK(
            _history(
                _shift("ann", MAR, JAN),
                _shift("ann", JAN, MAR),
                _shift("ann", FEB, APR),
            )
        )

        assert len(violations) == 2
        assert any("unusable period" in violation.message for violation in violations)
        assert any("overlapping periods" in violation.message for violation in violations)

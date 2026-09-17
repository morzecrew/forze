"""The data-level overlap oracle: no two recorded periods for one owner may overlap.

Driven from hand-built histories rather than a simulation, because what is under test is the
assertion itself — which pairs it catches, which it must not, and what it does with a marker the
workload recorded badly. The property it asserts is the one behind effective-dated master data,
bookings, shifts and leases.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from datetime import UTC, date, datetime, timedelta, tzinfo

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

    def test_an_open_end_reached_later_keeps_covering(self) -> None:
        """The open-ended period is not the earliest one, so it only keeps covering if the sweep
        treats "no end" as reaching furthest. Sabotaging that branch leaves every other leg in
        this file passing — the first period is adopted before the branch is ever consulted, and
        the open-ended one is adopted by the `covering is None` path there.
        """

        violations = _CHECK(
            _history(
                _shift("ann", JAN, FEB),
                _shift("ann", FEB),
                _shift("ann", MAR, APR),
            )
        )

        assert len(violations) == 1
        assert "overlapping periods" in violations[0].message

    def test_a_closed_period_does_not_displace_an_open_one(self) -> None:
        """The mirror: once an open-ended period is covering, a shorter closed one that overlaps
        it must not take its place, or everything after it is served as clean."""

        violations = _CHECK(
            _history(
                _shift("ann", JAN),
                _shift("ann", FEB, MAR),
                _shift("ann", APR, MAY),
            )
        )

        assert len(violations) == 2

    def test_every_overlapping_pair_is_reported(self) -> None:
        """Three nested periods are three conflicts. A sweep that carries only the
        furthest-reaching period reports the outer one against each inner one and never compares
        the two inner ones with each other, so a consumer correcting what it was handed would
        leave an overlap behind.
        """

        violations = _CHECK(
            _history(
                _shift("ann", JAN, MAY),
                _shift("ann", FEB, APR),
                _shift("ann", MAR, APR),
            )
        )

        assert len(violations) == 3

    def test_markers_mixing_datetime_awareness_are_reported_not_raised(self) -> None:
        """A naive and an aware `datetime` are one type, so they group together and `<` between
        their starts raises `TypeError` out of the sort — the third shape of this crash."""

        violations = _CHECK(
            _history(
                _shift("ann", datetime(2026, 1, 1), datetime(2026, 3, 1)),
                _shift("ann", datetime(2026, 2, 1, tzinfo=UTC), datetime(2026, 4, 1, tzinfo=UTC)),
            )
        )

        assert len(violations) == 1
        assert "not mutually comparable" in violations[0].message
        assert "naive datetime, aware datetime" in violations[0].message or (
            "aware datetime, naive datetime" in violations[0].message
        )

    def test_int_endpoints_are_reported_rather_than_swept_as_valid(self) -> None:
        """Ints sort and compare like a period, so before `Period` checked its endpoints a
        marker carrying them was swept as *valid* — the quiet half of this failure mode."""

        violations = _CHECK(
            _history(
                {"employee_id": "ann", "valid_from": 1, "valid_to": 10},
                {"employee_id": "ann", "valid_from": 2, "valid_to": 8},
            )
        )

        assert len(violations) == 2
        assert all("unusable period" in violation.message for violation in violations)

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

    def test_markers_that_are_not_mutually_comparable_are_reported_and_still_swept(self) -> None:
        """Each marker builds a valid period, so the mismatch only surfaces when the two are
        compared — where a `date` against a `datetime` raises `TypeError` out of the sort. It is
        reported once per owner, and each comparable group is still swept so a real overlap
        survives."""

        violations = _CHECK(
            _history(
                _shift("ann", JAN, MAR),
                _shift("ann", datetime(2026, 2, 1), datetime(2026, 4, 1)),
                _shift("ann", FEB, APR),
            )
        )

        assert len(violations) == 2
        assert any("not mutually comparable" in violation.message for violation in violations)
        assert any("overlapping periods" in violation.message for violation in violations)

    def test_an_overlap_in_the_second_grain_is_still_found(self) -> None:
        """ "Each grain is still swept" is only a claim while the overlap sits in the grain that
        happened to be recorded first: sweeping one grain per owner passes every other leg in
        this file. Here the `date` markers tile and the `datetime` ones overlap.
        """

        violations = _CHECK(
            _history(
                _shift("ann", JAN, FEB),
                _shift("ann", datetime(2026, 2, 1), datetime(2026, 4, 1)),
                _shift("ann", datetime(2026, 3, 1), datetime(2026, 5, 1)),
            )
        )

        assert len(violations) == 2
        assert any("not mutually comparable" in violation.message for violation in violations)
        assert any("overlapping periods" in violation.message for violation in violations)

    def test_the_mixture_message_names_the_groups_in_a_canonical_order(self) -> None:
        """Sorted rather than insertion-ordered, so two histories that mix the same two grains
        report the same sentence whichever marker arrived first."""

        datetime_first = _CHECK(
            _history(
                _shift("ann", datetime(2026, 1, 1), datetime(2026, 2, 1)),
                _shift("ann", JAN, FEB),
            )
        )

        assert len(datetime_first) == 1
        assert datetime_first[0].message.endswith(
            "are not mutually comparable: date, naive datetime"
        )

    def test_an_explicit_none_end_is_open_ended(self) -> None:
        violations = _CHECK(
            _history(
                {"employee_id": "ann", "valid_from": JAN, "valid_to": None},
                _shift("ann", MAR, APR),
            )
        )

        assert len(violations) == 1
        assert "overlapping periods" in violations[0].message

    def test_a_tzinfo_that_will_not_answer_is_reported(self) -> None:
        """An open-ended marker whose `tzinfo` raises from `utcoffset()`: the fourth shape of
        this crash, and the one that made the guard cover the whole per-marker step rather than
        one more exception type."""

        class Unhelpful(tzinfo):
            def utcoffset(self, dt: datetime | None) -> timedelta | None:
                raise RuntimeError("no offset for you")

            def dst(self, dt: datetime | None) -> timedelta | None:
                return None

        violations = _CHECK(
            _history({"employee_id": "ann", "valid_from": datetime(2026, 1, 1, tzinfo=Unhelpful())})
        )

        assert len(violations) == 1
        assert "unusable period" in violations[0].message

    def test_one_owners_unorderable_values_do_not_cost_another_owner_the_check(self) -> None:
        """A `date` subclass that refuses to be ordered raises out of the sort, past every
        per-marker guard. Reported for that owner, and the next owner's real overlap still
        surfaces — which is the whole point of reporting rather than raising."""

        class Rude(date):
            def __lt__(self, other: object) -> bool:
                raise RuntimeError("I refuse to be ordered")

        violations = _CHECK(
            _history(
                _shift("bob", Rude(2026, 1, 1)),
                _shift("bob", Rude(2026, 2, 1)),
                _shift("cat", JAN, MAR),
                _shift("cat", FEB, APR),
            )
        )

        assert len(violations) == 2
        assert any("could not be swept" in violation.message for violation in violations)
        assert any("overlapping periods for employee_id='cat'" in v.message for v in violations)

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


class TestTheReportFollowsTheMarkers:
    def test_violations_come_back_in_recorded_order_across_owners(self) -> None:
        """Grouping by owner is an implementation detail; the report is not. With owners
        interleaved, an overlap completed by an earlier marker must be reported first, or a
        reader correlating the report with the history reads them out of order.
        """

        violations = _CHECK(
            _history(
                _shift("ann", JAN, MAR),  # 0 — ann appears first…
                _shift("bob", JAN, MAR),  # 1
                _shift("bob", FEB, APR),  # 2 — …but bob's overlap completes first
                _shift("ann", FEB, APR),  # 3
            )
        )

        # Grouped by owner and left unsorted this reads [3, 2]: ann's conflict first, because
        # ann's first marker arrived first. The report follows the markers, not the grouping.
        assert [max(event.seq for event in violation.events) for violation in violations] == [2, 3]


class TestTheReportIsTheSameOnEveryRun:
    """A determinism tool that reports its findings in hash order is not one.

    The first fix for the mixed-grain crash grouped owners through a set comprehension, so the
    order of the violations it returned changed with ``PYTHONHASHSEED`` — which would make a
    minimized counterexample's report differ between two runs of the same seed. Single-process
    tests cannot see it: within one interpreter the order is stable, just arbitrary.
    """

    def test_violation_order_does_not_vary_by_hash_seed(self) -> None:
        script = textwrap.dedent(
            """
            from datetime import date, datetime

            from forze_dst.invariants import no_overlapping_periods
            from forze_dst.oracle import Event, History

            check = no_overlapping_periods("shift", key="employee_id", start="s", end="e")
            events = []

            for index, owner in enumerate(["ann", "bob", "cat", "dan", "eve"]):
                events.append(
                    Event(
                        seq=2 * index,
                        kind="shift",
                        at=0.0,
                        fields={"employee_id": owner, "s": date(2026, 1, 1)},
                    )
                )
                events.append(
                    Event(
                        seq=2 * index + 1,
                        kind="shift",
                        at=1.0,
                        fields={"employee_id": owner, "s": datetime(2026, 2, 1)},
                    )
                )

            print([violation.message for violation in check(History(seed=1, events=tuple(events)))])
            """
        )

        reports = {
            seed: subprocess.run(
                [sys.executable, "-c", script],
                env={**os.environ, "PYTHONHASHSEED": seed},
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            for seed in ("0", "1", "12345", "99999")
        }

        assert len(set(reports.values())) == 1, f"violation order varies by hash seed: {reports}"
        assert reports["0"].index("'ann'") < reports["0"].index("'eve'")

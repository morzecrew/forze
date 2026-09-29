"""Sorting and keyset seeks order aware datetimes by the instant, not by the text.

The comparator used to turn a datetime into its ISO string, and ISO strings with different UTC
offsets do not sort as instants: Berlin's noon in June (10:00 UTC) wrote as ``12:00+02:00`` and
sorted after 11:00 UTC. Through a repeated hour it went further wrong, since the second 02:15
reads earlier than the first 02:30. A cursor still carries the ISO string; a row's datetime meeting
it is compared as the instant the string names.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from typing import Final
from zoneinfo import ZoneInfo

import pytest

from forze.application.contracts.querying import (
    compare_keyset_sort_values,
    keyset_canonical_value,
    ordered_compare,
    row_passes_keyset_seek,
)
from forze.base.exceptions import CoreException

# ----------------------- #

BERLIN: Final = ZoneInfo("Europe/Berlin")
BERLIN_NOON: Final = datetime(2026, 6, 1, 12, 0, tzinfo=BERLIN)  # 10:00 UTC
ELEVEN_UTC: Final = datetime(2026, 6, 1, 11, 0, tzinfo=UTC)
SUMMER_0230: Final = datetime(2026, 10, 25, 2, 30, fold=0, tzinfo=BERLIN)  # 00:30 UTC
WINTER_0215: Final = datetime(2026, 10, 25, 2, 15, fold=1, tzinfo=BERLIN)  # 01:15 UTC


class TestTheComparator:
    @pytest.mark.parametrize(
        ("earlier", "later"),
        [(BERLIN_NOON, ELEVEN_UTC), (SUMMER_0230, WINTER_0215)],
        ids=["two-offsets", "repeated-hour"],
    )
    def test_the_earlier_instant_sorts_first(self, earlier: datetime, later: datetime) -> None:
        assert ordered_compare(earlier, later, direction="asc", nulls="first") == -1
        assert ordered_compare(later, earlier, direction="asc", nulls="first") == 1
        assert compare_keyset_sort_values(earlier, later) == -1

    def test_one_instant_written_twice_is_equal(self) -> None:
        assert (
            ordered_compare(
                BERLIN_NOON, BERLIN_NOON.astimezone(UTC), direction="asc", nulls="first"
            )
            == 0
        )

    def test_naive_datetimes_and_dates_still_compare(self) -> None:
        assert (
            ordered_compare(
                datetime(2026, 1, 1), datetime(2026, 1, 2), direction="asc", nulls="first"
            )
            == -1
        )
        assert (
            ordered_compare(date(2026, 1, 2), date(2026, 1, 1), direction="asc", nulls="first") == 1
        )


class TestTheCursor:
    def test_a_row_meets_the_instant_its_cursor_names(self) -> None:
        # The cursor carries Berlin's noon as text; a row at 11:00 UTC is after it in fact,
        # though "11:00+00:00" sorts before "12:00+02:00" as text.
        cursor = keyset_canonical_value(BERLIN_NOON)

        assert isinstance(cursor, str)
        assert row_passes_keyset_seek(
            {"at": ELEVEN_UTC},
            sort_keys=["at"],
            directions=["asc"],
            cursor_values=[cursor],
            after=True,
        )

    def test_a_cursor_through_the_repeated_hour_keeps_its_offset(self) -> None:
        cursor = keyset_canonical_value(SUMMER_0230)

        assert row_passes_keyset_seek(
            {"at": WINTER_0215},
            sort_keys=["at"],
            directions=["asc"],
            cursor_values=[cursor],
            after=True,
        )

    def test_a_date_row_meets_a_date_cursor(self) -> None:
        cursor = keyset_canonical_value(date(2026, 1, 1))

        assert row_passes_keyset_seek(
            {"d": date(2026, 1, 2)},
            sort_keys=["d"],
            directions=["asc"],
            cursor_values=[cursor],
            after=True,
        )

    def test_the_keyset_comparator_reads_a_cursors_text_too(self) -> None:
        assert compare_keyset_sort_values(ELEVEN_UTC, keyset_canonical_value(BERLIN_NOON)) == 1

    def test_a_date_is_not_a_cursor_for_a_datetime(self) -> None:
        # A datetime key's cursor always carries a time; a bare date would silently move the
        # boundary to midnight.
        with pytest.raises(CoreException) as caught:
            ordered_compare(
                datetime(2026, 6, 1, 9, 0), "2026-06-01", direction="asc", nulls="first"
            )

        assert caught.value.kind.value == "validation"

    def test_text_that_is_not_a_time_is_a_tampered_cursor(self) -> None:
        with pytest.raises(CoreException) as caught:
            ordered_compare(ELEVEN_UTC, "not a time", direction="asc", nulls="first")

        assert caught.value.kind.value == "validation"

    def test_offsets_that_share_nothing_but_an_instant_agree(self) -> None:
        tokyo = datetime(2026, 6, 1, 19, 0, tzinfo=timezone(timedelta(hours=9)))  # 10:00 UTC

        assert compare_keyset_sort_values(tokyo, BERLIN_NOON) == 0

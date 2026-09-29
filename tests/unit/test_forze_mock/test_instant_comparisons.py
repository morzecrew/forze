"""The in-memory store compares aware datetimes as instants, as a real store does.

Python compares two datetimes that share a ``tzinfo`` by their wall clocks and ignores ``fold``,
and compares two in different zones as instants. So through Berlin's repeated hour on 25 Oct
2026, the summer 02:30 and the winter 02:30 are an hour apart and compare equal, while Berlin's
12:00 in June and 10:00 UTC are one instant written two ways. A ``timestamptz`` column reads all
of them as instants; the mock has to agree, or a test passes against it and fails in production.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Final
from zoneinfo import ZoneInfo

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.guarantees import UniqueTogether
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #

BERLIN: Final = ZoneInfo("Europe/Berlin")
SUMMER_0230: Final = datetime(2026, 10, 25, 2, 30, fold=0, tzinfo=BERLIN)
WINTER_0230: Final = datetime(2026, 10, 25, 2, 30, fold=1, tzinfo=BERLIN)
WINTER_0215: Final = datetime(2026, 10, 25, 2, 15, fold=1, tzinfo=BERLIN)


class _Slot(Document):
    at: datetime


class _SlotRead(ReadDocument):
    at: datetime


class _SlotCreate(CreateDocumentCmd):
    at: datetime


class _SlotUpdate(BaseDTO):
    at: datetime | None = None


SLOTS = DocumentSpec(
    name="slots",
    read=_SlotRead,
    write=DocumentWriteTypes(domain=_Slot, create_cmd=_SlotCreate, update_cmd=_SlotUpdate),
    guarantees=(UniqueTogether(fields=("at",)),),
)


def _ctx():
    return context_from_modules(MockDepsModule())


# ....................... #


class TestTheUniqueGuarantee:
    async def test_two_passes_through_the_repeated_hour_are_two_instants(self) -> None:
        ctx = _ctx()
        await ctx.document.command(SLOTS).create(_SlotCreate(at=SUMMER_0230))
        await ctx.document.command(SLOTS).create(_SlotCreate(at=WINTER_0230))

        assert await ctx.document.query(SLOTS).count() == 2

    async def test_one_instant_written_in_two_zones_is_one_value(self) -> None:
        ctx = _ctx()
        await ctx.document.command(SLOTS).create(
            _SlotCreate(at=datetime(2026, 6, 1, 12, 0, tzinfo=BERLIN))
        )

        with pytest.raises(CoreException) as caught:
            await ctx.document.command(SLOTS).create(
                _SlotCreate(at=datetime(2026, 6, 1, 10, 0, tzinfo=UTC))
            )

        assert caught.value.kind.value == "conflict"


class TestTheFilters:
    async def _slots(self) -> object:
        ctx = _ctx()

        for at in (SUMMER_0230, WINTER_0215):
            await ctx.document.command(SLOTS).create(_SlotCreate(at=at))

        return ctx

    @pytest.mark.parametrize(
        ("op", "value", "expected"),
        [
            # The winter 02:15 is 45 minutes after the summer 02:30, though its clock reads less.
            ("$gt", SUMMER_0230, [WINTER_0215]),
            ("$lt", WINTER_0215, [SUMMER_0230]),
            # The winter 02:30 reads equal to the summer one on the clock; it is an hour later.
            ("$eq", WINTER_0230, []),
            ("$eq", SUMMER_0230.astimezone(UTC), [SUMMER_0230]),
        ],
        ids=["gt", "lt", "eq-fold", "eq-other-zone"],
    )
    async def test_a_comparison_reads_instants(
        self, op: str, value: datetime, expected: list[datetime]
    ) -> None:
        ctx = await self._slots()
        page = await ctx.document.query(SLOTS).find_many(  # type: ignore[attr-defined]
            filters={"$values": {"at": {op: value}}}
        )

        assert sorted(hit.at - SUMMER_0230 for hit in page.hits) == sorted(
            at - SUMMER_0230 for at in expected
        )


class TestOperandsThatAreNotInstants:
    @pytest.mark.parametrize(
        "filters",
        [
            {"$values": {"at": {"$in": [SUMMER_0230 - datetime(1970, 1, 1, tzinfo=UTC)]}}},
            {"$fields": {"d": {"$lt": "at"}}},
            {"$fields": {"d": {"$eq": "at"}}},
        ],
        ids=["in-list", "field-lt", "field-eq"],
    )
    def test_a_duration_never_compares_with_an_instant(self, filters: dict[str, object]) -> None:
        # A store refuses interval < timestamptz; read as a distance from the epoch, an instant
        # would compare cleanly against a timedelta and match.
        from forze.application.contracts.querying.internal.matching import evaluate_filter

        row = {"at": SUMMER_0230, "d": SUMMER_0230 - datetime(1970, 1, 1, tzinfo=UTC)}

        assert evaluate_filter(row, filters) is False  # type: ignore[arg-type]

    def test_a_zone_that_cannot_answer_is_no_match(self) -> None:
        from datetime import tzinfo

        from forze.application.contracts.querying.internal.matching import evaluate_filter

        class _Broken(tzinfo):
            def utcoffset(self, dt: datetime | None) -> None:
                raise ValueError("no answer")

            def dst(self, dt: datetime | None) -> None:
                return None

        row = {"at": datetime(2026, 6, 1, tzinfo=_Broken())}

        assert evaluate_filter(row, {"$values": {"at": {"$eq": "x"}}}) is False


class TestTheSetOperators:
    @pytest.mark.parametrize(
        ("op", "value", "expected"),
        [
            ("$overlaps", [SUMMER_0230.astimezone(UTC)], True),
            ("$superset", [WINTER_0230], False),
            ("$subset", [SUMMER_0230.astimezone(UTC), WINTER_0215], True),
            ("$disjoint", [WINTER_0230], True),
        ],
        ids=["overlaps-other-zone", "superset-fold", "subset-other-zone", "disjoint-fold"],
    )
    def test_members_are_compared_as_instants(
        self, op: str, value: list[datetime], expected: bool
    ) -> None:
        from forze.application.contracts.querying.internal.matching import evaluate_filter

        row = {"ats": [SUMMER_0230]}

        assert evaluate_filter(row, {"$values": {"ats": {op: value}}}) is expected  # type: ignore[arg-type]


def test_an_instant_key_never_equals_a_duration() -> None:
    # The unique guarantee keys a row by these; an instant must not collide with a timedelta
    # that happens to hold the same distance from the epoch.
    from forze.application.contracts.querying.internal.matching import instant_key

    distance = SUMMER_0230 - datetime(1970, 1, 1, tzinfo=UTC)

    assert instant_key(SUMMER_0230) != instant_key(distance)
    assert instant_key(SUMMER_0230) == instant_key(SUMMER_0230.astimezone(UTC))
    assert instant_key(SUMMER_0230) != instant_key(WINTER_0230)


class _Stamped(Document):
    grp: str
    at: datetime


class _StampedRead(ReadDocument):
    grp: str
    at: datetime


class _StampedCreate(CreateDocumentCmd):
    grp: str
    at: datetime


STAMPS = DocumentSpec(
    name="stamps",
    read=_StampedRead,
    write=DocumentWriteTypes(domain=_Stamped, create_cmd=_StampedCreate),
)


class TestTheAggregates:
    async def _stamps(self) -> object:
        ctx = _ctx()

        for at in (SUMMER_0230, WINTER_0230):
            await ctx.document.command(STAMPS).create(_StampedCreate(grp="g", at=at))

        return ctx

    async def test_distinct_min_and_max_read_instants(self) -> None:
        ctx = await self._stamps()
        page = await ctx.document.query(STAMPS).aggregate_page(  # type: ignore[attr-defined]
            aggregates={
                "$groups": {"g": "grp"},
                "$computed": {
                    "n": {"$count_distinct": "at"},
                    "lo": {"$min": "at"},
                    "hi": {"$max": "at"},
                },
            },
            pagination={"limit": 10},
        )
        [row] = page.hits

        # An hour apart in fact, equal on the clock.
        assert row["n"] == 2
        assert (row["lo"] - SUMMER_0230, row["hi"] - WINTER_0230) == (timedelta(0), timedelta(0))

    async def test_a_group_is_one_instant(self) -> None:
        ctx = await self._stamps()
        page = await ctx.document.query(STAMPS).aggregate_page(  # type: ignore[attr-defined]
            aggregates={"$groups": {"at": "at"}, "$computed": {"n": {"$count": None}}},
            pagination={"limit": 10},
        )

        assert sorted(row["n"] for row in page.hits) == [1, 1]

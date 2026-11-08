"""Derived inputs are a pure function of the seed, date and time fields included.

polyfactory and Faker draw dates and times relative to the wall clock, so before
:class:`~forze_dst._input_factory.InputFactory` the same seed built different inputs on
different runs and a recorded failing seed did not replay.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from types import SimpleNamespace
from typing import Any, cast

import faker.providers.date_time as faker_date_time
import polyfactory.value_generators.constrained_dates as constrained_dates
import pytest
from polyfactory.factories.pydantic_factory import ModelFactory
from pydantic import (
    UUID1,
    AwareDatetime,
    BaseModel,
    Field,
    FutureDate,
    FutureDatetime,
    NaiveDatetime,
    PastDate,
    PastDatetime,
)

from forze_dst._input_factory import InputFactory
from forze_dst.derive import _arg_builder
from forze_dst.engines.cases import OperationCase
from forze_dst.engines.context import input_for
from forze_dst.scenario import ModelState
from forze_dst.time_source import DEFAULT_EPOCH

# ----------------------- #


class _Leg(BaseModel):
    departs: datetime
    booked: date = Field(gt=date(2000, 1, 1))


class _Trip(BaseModel):
    name: str
    starts: datetime
    ends: datetime | None
    day: date
    at: time
    lasts: timedelta
    aware: AwareDatetime
    naive: NaiveDatetime
    was: PastDatetime
    will: FutureDatetime
    was_day: PastDate
    will_day: FutureDate
    ref: UUID1
    legs: list[_Leg]
    seats: int


@pytest.fixture
def wall_clock(monkeypatch: pytest.MonkeyPatch) -> Callable[[timedelta], None]:
    """Move the wall clock that Faker and polyfactory read by a shift."""

    def move(shift: timedelta) -> None:
        class _Real(type):
            def __instancecheck__(cls, value: object) -> bool:  # real dates pass Faker's checks
                return isinstance(value, cls.__mro__[1])

        class _Datetime(datetime, metaclass=_Real):
            @classmethod
            def now(cls, tz: Any = None) -> Any:
                return datetime.now(tz) + shift

        class _Date(date, metaclass=_Real):
            @classmethod
            def today(cls) -> Any:
                return date.today() + shift

        monkeypatch.setattr(faker_date_time, "datetime", _Datetime)
        monkeypatch.setattr(faker_date_time, "dtdate", _Date)
        monkeypatch.setattr(constrained_dates, "datetime", _Datetime)

    return move


def _derived(seed: int) -> Any:
    return _arg_builder(_Trip, {})(ModelState(), random.Random(seed))


def _op_case(seed: int) -> Any:
    descriptor = SimpleNamespace(input_type=_Trip)
    sim = cast(Any, SimpleNamespace(operations=SimpleNamespace(descriptors={"book": descriptor})))
    return input_for(sim, "book", random.Random(seed), OperationCase(op="book"))


# ....................... #


class TestSeedReplay:
    @pytest.mark.parametrize("build", [_derived, _op_case], ids=["derived", "op_case"])
    def test_a_seed_builds_the_same_input_under_any_wall_clock(
        self, wall_clock: Callable[[timedelta], None], build: Callable[[int], Any]
    ) -> None:
        runs = []

        for shift in (timedelta(0), timedelta(days=-1000, hours=5), timedelta(days=1000)):
            wall_clock(shift)
            runs.append([build(seed) for seed in range(20)])

        assert runs[0] == runs[1] == runs[2]
        assert len({trip.starts for trip in runs[0]}) > 1  # times still vary by seed

    def test_other_fields_draw_what_plain_polyfactory_drew(self) -> None:
        # Each time value takes one draw from the seeded Faker stream, as Faker's own did, so
        # stored seeds keep every other field. UUID1 is left out: uuid1() drew nothing.
        class Booking(BaseModel):
            guest: str
            checks_in: datetime
            nights: int
            maybe: date | None
            paid: bool
            stay: timedelta
            rate: float
            due: FutureDate
            tags: list[str]
            note: str

        times = {"checks_in", "maybe", "stay", "due"}
        plain, seeded = ModelFactory.create_factory(Booking), InputFactory.create_factory(Booking)

        for seed in range(200):
            plain.seed_random(seed)  # both factories share polyfactory's Faker: seed each build
            old = plain.build()
            seeded.seed_random(seed)
            new = seeded.build()

            assert old.model_dump(exclude=times) == new.model_dump(exclude=times)
            assert (old.maybe is None) == (new.maybe is None)


class TestTimeWindows:
    def test_times_sit_on_the_simulated_clock_and_future_ones_past_the_real_one(self) -> None:
        factory = InputFactory.create_factory(_Trip)
        naive_epoch = DEFAULT_EPOCH.replace(tzinfo=None)

        for seed in range(50):
            factory.seed_random(seed)
            trip = factory.build()

            assert trip.starts.tzinfo is None and trip.starts <= naive_epoch
            assert trip.aware.tzinfo is UTC and trip.aware <= DEFAULT_EPOCH
            assert trip.was < naive_epoch and trip.was_day < DEFAULT_EPOCH.date()
            assert trip.will.year >= 2100 and trip.will_day.year >= 2100
            assert timedelta(0) <= trip.lasts <= timedelta(days=30)
            assert trip.ref.version == 1

    def test_bounded_dates_stay_inside_their_bounds(self) -> None:
        class Window(BaseModel):
            after: date = Field(gt=date(2030, 1, 1))
            from_: date = Field(ge=date(2031, 6, 1))
            before: datetime = Field(lt=datetime(2000, 1, 1))
            upto: date = Field(le=date(1999, 6, 1))
            within: date = Field(ge=date(2024, 3, 1), le=date(2024, 3, 3))
            since: datetime = Field(gt=datetime(2010, 5, 5, 12, tzinfo=UTC))

        factory = InputFactory.create_factory(Window)
        span = timedelta(days=100)

        for seed in range(100):
            factory.seed_random(seed)
            window = factory.build()  # pydantic checks every bound on build

            assert date(2030, 1, 2) <= window.after <= date(2030, 1, 2) + span
            assert date(2031, 6, 1) <= window.from_ <= date(2031, 6, 1) + span
            assert date(1999, 12, 31) - span <= window.before.date() <= date(1999, 12, 31)
            assert date(1999, 6, 1) - span <= window.upto <= date(1999, 6, 1)
            assert date(2024, 3, 1) <= window.within <= date(2024, 3, 3)
            assert window.since.date() <= DEFAULT_EPOCH.date() + span

    def test_plain_factories_keep_polyfactory_providers(
        self, wall_clock: Callable[[timedelta], None]
    ) -> None:
        # InputFactory registers as a polyfactory base factory; nested models of other
        # factories must not start building through it.
        class Outer(BaseModel):
            leg: _Leg

        wall_clock(timedelta(days=200 * 365))
        factory = ModelFactory.create_factory(Outer)
        factory.seed_random(0)

        assert factory.build().leg.departs.year > 2150  # Faker's clock, not the epoch

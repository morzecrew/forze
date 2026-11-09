"""The polyfactory factory DST builds operation inputs with: dates and times come from the seed.

polyfactory's own providers draw dates and times relative to the wall clock (Faker's ``"now"``
and ``date.today()``, ``datetime.now()`` for a bounded date, ``uuid1()``), so one seed built a
different input on every run and a recorded failing seed did not replay. :class:`InputFactory`
draws them from fixed windows instead, anchored at the simulated clock's default start
(:data:`~forze_dst.time_source.DEFAULT_EPOCH`).

Each date, time or ``timedelta`` replacement takes one ``random()`` from the seeded Faker
stream, as the Faker call it replaces did, so every other field of a seed keeps the value it
had before. A ``UUID1`` takes a draw ``uuid1()`` did not, so the fields built after one change.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from functools import partial
from typing import Any, TypeGuard
from uuid import UUID

from polyfactory.factories.base import BuildContext
from polyfactory.factories.pydantic_factory import ModelFactory
from polyfactory.field_meta import FieldMeta
from polyfactory.utils.predicates import is_safe_subclass
from pydantic import (
    AwareDatetime,
    BaseModel,
    FutureDate,
    FutureDatetime,
    NaiveDatetime,
    PastDate,
    PastDatetime,
)

from forze_dst.time_source import DEFAULT_EPOCH

# ----------------------- #

_HISTORY = timedelta(days=30 * 365)
"""How far before the epoch an unconstrained date or time can fall (Faker's ``-30y``)."""

_WINDOW = timedelta(days=30)
"""Width of the ``Past*`` / ``Future*`` windows and the ``timedelta`` range (Faker's ``30d``)."""

_BOUND_SPAN = timedelta(days=100)
"""How far a one-sided date bound reaches (polyfactory's ``±100`` days)."""

_SECOND = timedelta(seconds=1)
_DAY = timedelta(days=1)
_MICROSECOND = timedelta(microseconds=1)

# shortcut: future values hold until 2100; move the anchor before then.
_FUTURE = datetime(2100, 1, 1, tzinfo=UTC)
"""Where future values start: pydantic checks them against the real clock, not the simulated one."""

# ....................... #


def _day(value: date) -> date:
    return value.date() if isinstance(value, datetime) else value


def _same(value: date) -> date:
    return value


def _in_zone(bounds: tuple[Any, ...], aware: bool | None) -> tzinfo | None:
    """The zone a bounded instant is drawn in, or :obj:`None` for a naive one: an aware
    bound's zone, or UTC for a field that must be aware."""

    if aware is False:
        return None

    zones = (b.tzinfo for b in bounds if isinstance(b, datetime) and b.tzinfo is not None)

    return next(zones, UTC if aware else None)


def _framed(bound: Any, zone: tzinfo | None) -> Any:
    # pydantic compares a naive bound or value with an aware one by wall clock, and two
    # aware ones as instants: a naive bound reads in the value's zone, an aware one keeps its
    if not isinstance(bound, datetime):
        return bound

    if zone is None:
        return bound.replace(tzinfo=None)

    return bound if bound.tzinfo is not None else bound.replace(tzinfo=zone)


# ....................... #


class InputFactory(ModelFactory[BaseModel]):
    """A :class:`ModelFactory` whose date, time and ``UUID1`` values are a pure function of the seed."""

    __is_base_factory__ = True

    # ....................... #

    @classmethod
    def is_supported_type(cls, value: Any) -> TypeGuard[type[BaseModel]]:
        # A base factory joins polyfactory's global registry: never pick this one for another
        # factory's nested models. Ours reach it through ``__base_factory_overrides__``.
        return cls is not InputFactory and super().is_supported_type(value)

    # ....................... #

    @classmethod
    def _between(cls, start: datetime, end: datetime) -> datetime:
        """A naive UTC instant in ``[start, end]``, one draw from the seeded Faker stream."""

        return (start + (end - start) * cls.__faker__.random.random()).replace(tzinfo=None)

    # ....................... #

    @classmethod
    def get_provider_map(cls) -> dict[Any, Callable[[], Any]]:
        history = partial(cls._between, DEFAULT_EPOCH - _HISTORY, DEFAULT_EPOCH)
        past = partial(cls._between, DEFAULT_EPOCH - _WINDOW, DEFAULT_EPOCH - _SECOND)
        future = partial(cls._between, _FUTURE + _SECOND, _FUTURE + _WINDOW)

        return {
            **super().get_provider_map(),
            datetime: history,
            NaiveDatetime: history,
            AwareDatetime: lambda: history().replace(tzinfo=UTC),
            date: lambda: history().date(),
            time: lambda: history().time(),
            timedelta: lambda: _WINDOW * cls.__faker__.random.random(),
            PastDatetime: past,
            PastDate: lambda: past().date(),
            FutureDatetime: future,
            FutureDate: lambda: future().date(),
        }

    # ....................... #

    @classmethod
    def get_constrained_field_value(
        cls,
        annotation: Any,
        field_meta: FieldMeta,
        field_build_parameters: Any | None = None,
        build_context: BuildContext | None = None,
    ) -> Any:
        constraints: Any = field_meta.constraints or {}

        if constraints.get("uuid_version") == 1:
            return UUID(int=cls.__faker__.random.getrandbits(128), version=1)

        # polyfactory refuses a bound on pydantic's aware and naive datetime types outright
        named = annotation is AwareDatetime or annotation is NaiveDatetime

        if (named or is_safe_subclass(annotation, date)) and "json" not in constraints:
            return cls._bounded(
                constraints,
                instant=named or is_safe_subclass(annotation, datetime),
                aware=annotation is AwareDatetime if named else None,
            )

        return super().get_constrained_field_value(
            annotation,
            field_meta,
            field_build_parameters=field_build_parameters,
            build_context=build_context,
        )

    # ....................... #

    @classmethod
    def _bounded(cls, constraints: Any, *, instant: bool, aware: bool | None = None) -> date:
        """A day, or an *instant*, within ``ge``/``gt``/``le``/``lt``, with "now" at the epoch.

        The tighter bound of each side holds, and an instant keeps its bounds' time of day and
        zone: aware when a bound is or *aware* is true, naive when *aware* is false. A missing
        bound sits :data:`_BOUND_SPAN` past the given one or the epoch, whichever is farther
        out, so a one-sided bound never leaves an empty window.
        """

        ge, gt, le, lt = (constraints.get(key) for key in ("ge", "gt", "le", "lt"))
        zone = _in_zone((ge, gt, le, lt), aware) if instant else None

        if instant:
            ge, gt, le, lt = (_framed(b, zone) for b in (ge, gt, le, lt))

        # An exclusive bound moves in by the smallest step the value can take.
        step, edge = (_MICROSECOND, _same) if instant else (_DAY, _day)
        lows = (edge(ge) if ge is not None else None, edge(gt) + step if gt is not None else None)
        highs = (edge(le) if le is not None else None, edge(lt) - step if lt is not None else None)
        low = max((b for b in lows if b is not None), default=None)
        high = min((b for b in highs if b is not None), default=None)

        if not instant:
            epoch: date = DEFAULT_EPOCH.date()

        elif zone is not None:
            epoch = DEFAULT_EPOCH.astimezone(zone)

        else:
            epoch = DEFAULT_EPOCH.replace(tzinfo=None)

        if low is None:
            low = min(epoch, high or epoch) - _BOUND_SPAN

        if high is None:
            high = max(epoch, low) + _BOUND_SPAN

        value = low + (high - low) * cls.__faker__.random.random()

        # bounds in two zones: the wall clock a naive bound is read by is the one in ``zone``
        return value.astimezone(zone) if isinstance(value, datetime) and zone else value


# Nested models build through this factory too, so their dates come from the seed as well.
InputFactory.__base_factory_overrides__ = {BaseModel: InputFactory}

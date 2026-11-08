"""The polyfactory factory DST builds operation inputs with: dates and times come from the seed.

polyfactory's own providers draw dates and times relative to the wall clock (Faker's ``"now"``
and ``date.today()``, ``datetime.now()`` for a bounded date, ``uuid1()``), so one seed built a
different input on every run and a recorded failing seed did not replay. :class:`InputFactory`
draws them from fixed windows instead, anchored at the simulated clock's default start
(:data:`~forze_dst.time_source.DEFAULT_EPOCH`).

Each replacement takes one ``random()`` from the seeded Faker stream, as the Faker call it
replaces did, so every other field of a seed keeps the value it had before.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
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

# shortcut: future values hold until 2100; move the anchor before then.
_FUTURE = datetime(2100, 1, 1, tzinfo=UTC)
"""Where future values start: pydantic checks them against the real clock, not the simulated one."""

# ....................... #


def _day(value: date) -> date:
    return value.date() if isinstance(value, datetime) else value


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

        if is_safe_subclass(annotation, date) and "json" not in constraints:
            return cls._bounded_day(constraints)

        return super().get_constrained_field_value(
            annotation,
            field_meta,
            field_build_parameters=field_build_parameters,
            build_context=build_context,
        )

    # ....................... #

    @classmethod
    def _bounded_day(cls, constraints: Any) -> date:
        """A day within ``ge``/``gt``/``le``/``lt``, as polyfactory picks one, with "now" at the epoch.

        A missing bound sits :data:`_BOUND_SPAN` past the given one or the epoch, whichever is
        farther out, so a one-sided bound never leaves an empty window.
        """

        ge, gt, le, lt = (constraints.get(key) for key in ("ge", "gt", "le", "lt"))
        low = _day(ge) if ge is not None else (_day(gt) + _DAY if gt is not None else None)
        high = _day(le) if le is not None else (_day(lt) - _DAY if lt is not None else None)
        epoch = DEFAULT_EPOCH.date()

        if low is None:
            low = min(epoch, high or epoch) - _BOUND_SPAN

        if high is None:
            high = max(epoch, low) + _BOUND_SPAN

        return low + (high - low) * cls.__faker__.random.random()


# Nested models build through this factory too, so their dates come from the seed as well.
InputFactory.__base_factory_overrides__ = {BaseModel: InputFactory}

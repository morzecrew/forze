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

from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from functools import partial
from types import NoneType
from typing import Annotated, Any, TypeGuard, get_args, get_origin
from uuid import UUID

from polyfactory.factories.base import BuildContext
from polyfactory.factories.pydantic_factory import ModelFactory
from polyfactory.field_meta import FieldMeta
from polyfactory.utils.normalize_type import normalize_type
from polyfactory.utils.predicates import is_safe_subclass, is_union
from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    FutureDate,
    FutureDatetime,
    NaiveDatetime,
    PastDate,
    PastDatetime,
    PlainValidator,
    TypeAdapter,
    WrapValidator,
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


class _AwarePastDatetime(datetime):
    """A past instant that must be aware: pydantic has no type for one."""


class _AwareFutureDatetime(datetime):
    """A future instant that must be aware: pydantic has no type for one."""


_AWARE_AS: dict[type, Any] = {
    datetime: AwareDatetime,
    PastDatetime: _AwarePastDatetime,
    FutureDatetime: _AwareFutureDatetime,
}
"""The type a datetime leaf builds as when its metadata asks for an aware value."""


_VALIDATORS = (AfterValidator, BeforeValidator, PlainValidator, WrapValidator)
"""Metadata that can refuse a value; with markers (:data:`_HOOK`), what the awareness probe keeps."""

_HOOK = "__get_pydantic_core_schema__"
"""What a marker such as ``AwareDatetime()`` defines to shape the schema."""


_AS_METADATA: dict[str, Any] = {
    "after": AfterValidator,
    "before": BeforeValidator,
    "plain": PlainValidator,
    "wrap": WrapValidator,
}
"""The annotation form of each ``@field_validator`` mode."""


def _asks_aware(source: type, metadata: Iterable[Any]) -> bool | None:
    """Whether a datetime's metadata asks for an aware (true) or a naive (false) value.

    The field answers for itself: each of its validators and markers, bounds left out, is
    asked on its own (:func:`_answer`), so one that refuses every sample cannot hide another
    that names the awareness. Two that name opposite ones leave the field as it is.
    """

    checks = (m for m in metadata if isinstance(m, _VALIDATORS) or hasattr(m, _HOOK))
    answers = {_answer(source, check) for check in checks} - {None}

    return answers.pop() if len(answers) == 1 else None


def _answer(source: type, check: Any) -> bool | None:
    """Whether *check* on *source* accepts only an aware (true) or only a naive (false) instant.

    Validates one instant naive and aware, so a validator or marker that refuses exactly one
    of the two names the awareness: forze's ``AwareDatetime`` and a user's own alike. Each runs
    on both samples once, when the factory first reads its fields.
    """

    annotation: Any = Annotated[source, check]

    try:
        adapter: TypeAdapter[Any] = TypeAdapter(annotation)

    except Exception:  # a marker that only builds within its model: it names nothing here
        return None

    sample = _FUTURE + _WINDOW if source is FutureDatetime else DEFAULT_EPOCH
    naive, aware = _accepts(adapter, sample.replace(tzinfo=None)), _accepts(adapter, sample)

    return aware if naive != aware else None


def _accepts(adapter: TypeAdapter[Any], value: datetime) -> bool:
    """Whether *adapter* validates *value*."""

    try:
        adapter.validate_python(value)

    except Exception:  # a user's validator can refuse a value with any exception
        return False

    return True


def _name_awareness(meta: FieldMeta, source: Any) -> None:
    """Type each datetime leaf of *meta* as pydantic's aware or naive datetime where its
    *source* annotation's metadata asks for one: polyfactory keeps only the constraints."""

    metadata: list[Any] = []

    while (named := normalize_type(source)) != source:  # a ``type`` alias, as polyfactory reads it
        source = named

    if get_origin(source) is Annotated:
        source, *metadata = get_args(source)

    if not meta.children:
        if not (isinstance(source, type) and source in _AWARE_AS):
            return

        aware = _asks_aware(source, metadata)

        if aware:
            meta.annotation = _AWARE_AS[source]

        elif aware is False and source is datetime:
            meta.annotation = NaiveDatetime

        return

    # A union's children skip ``None`` and get the union's validators, which check the arm a
    # value takes; a generic's follow its arguments, when they line up.
    args = [arg for arg in get_args(source) if arg is not NoneType]

    if metadata and is_union(source):
        args = [Annotated[arg, *metadata] for arg in args]

    if len(args) == len(meta.children):
        for child, arg in zip(meta.children, args, strict=True):
            _name_awareness(child, arg)


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

    # in UTC, arithmetic runs on instants: within one zone it is wall-clock and drops ``fold``
    return (bound if bound.tzinfo is not None else bound.replace(tzinfo=zone)).astimezone(UTC)


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
    def get_model_fields(cls) -> list[FieldMeta]:
        """polyfactory's fields, each datetime typed aware or naive where its metadata asks."""

        # polyfactory reads the fields once per factory; name the awareness on that pass
        first = "_fields_metadata" not in cls.__dict__
        fields = super().get_model_fields()

        if first:
            model = cls.__model__
            decorators = model.__pydantic_decorators__.field_validators.values()

            for meta, (name, info) in zip(fields, model.model_fields.items(), strict=True):
                # a ``@field_validator`` checks the whole field, as its annotation's validators do
                own = [
                    _AS_METADATA[d.info.mode](d.func)
                    for d in decorators
                    if name in d.info.fields or "*" in d.info.fields
                ]
                metadata = [*info.metadata, *own]
                source = Annotated[info.annotation, *metadata] if metadata else info.annotation
                _name_awareness(meta, source)

        return fields

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
            _AwarePastDatetime: lambda: past().replace(tzinfo=UTC),
            FutureDatetime: future,
            FutureDate: lambda: future().date(),
            _AwareFutureDatetime: lambda: future().replace(tzinfo=UTC),
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
            epoch = DEFAULT_EPOCH

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

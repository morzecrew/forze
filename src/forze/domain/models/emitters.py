"""Helpers for declaring and collecting domain-event emitters.

An emitter mirrors :func:`~forze.domain.validation.update_validator`: a method of the
form ``(before, after, diff) -> DomainEvent | None`` collected across the class
hierarchy and run after :meth:`~forze.domain.models.document.Document.update`.
Validators enforce invariants (raise); emitters declare which domain event a state
transition raises. Emitters must be declared on an
:class:`~forze.domain.models.aggregate.AggregateRoot` subclass.
"""

from collections import OrderedDict
from collections.abc import Callable, Iterable
from typing import Final, TypeVar, cast, overload

import attrs
from pydantic import BaseModel

from forze.base.primitives import JsonDict

from .._callables import normalize_before_after_diff
from .._logger import logger
from .events import DomainEvent

# ----------------------- #

EVENT_EMITTER_METADATA_FIELD: Final = "__event_emitter__"
"""Attribute that stores the emitter metadata on a decorated method."""

EVENT_EMITTER_STORE_FIELD: Final = "_event_emitters_"
"""ClassVar that stores collected emitters on an aggregate class."""

# ....................... #

M = TypeVar("M", bound=BaseModel)

type EventEmitter[X: BaseModel] = Callable[[X, X, JsonDict], DomainEvent | None]
"""Normalized emitter signature."""

type EventEmitterLike[X: BaseModel] = (
    Callable[[X], DomainEvent | None]
    | Callable[[X, X], DomainEvent | None]
    | Callable[[X, X, JsonDict], DomainEvent | None]
)
"""Allowed emitter signatures."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class EventEmitterMetadata:
    """Metadata attached to an emitter by :func:`event_emitter`."""

    fields: frozenset[str] | None = attrs.field(default=None)
    """Fields that trigger the emitter. If ``None``, it runs on any update."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _EmitterEntry:
    owner: type
    meta: EventEmitterMetadata


# ....................... #


@overload
def event_emitter(_func: EventEmitterLike[M]) -> EventEmitter[M]:
    """Register a method as an emitter when used as a bare decorator."""


@overload
def event_emitter(
    _func: None = None,
    *,
    fields: Iterable[str] | None = None,
) -> Callable[[EventEmitterLike[M]], EventEmitter[M]]:
    """Return a decorator that registers an emitter with an optional field filter."""


def event_emitter(
    _func: EventEmitterLike[M] | None = None,
    *,
    fields: Iterable[str] | None = None,
) -> EventEmitter[M] | Callable[[EventEmitterLike[M]], EventEmitter[M]]:
    """Turn a method into a normalized domain-event emitter.

    The wrapped function may accept ``before``, and optional ``after`` and ``diff``,
    and returns a :class:`~forze.domain.models.events.DomainEvent` to record on the
    aggregate (or ``None``). Must be declared on an ``AggregateRoot`` subclass.
    """

    def decorator(f: EventEmitterLike[M]) -> EventEmitter[M]:
        logger.trace(
            "Registering event emitter %s",
            getattr(f, "__qualname__", getattr(f, "__name__", repr(f))),
        )

        wrapper = normalize_before_after_diff(f, kind="Event emitter")
        meta = EventEmitterMetadata(fields=frozenset(fields) if fields else None)
        setattr(wrapper, EVENT_EMITTER_METADATA_FIELD, meta)

        return cast("EventEmitter[M]", wrapper)

    if _func is not None:
        return decorator(_func)

    return decorator


# ....................... #


def has_event_emitters(cls: type) -> bool:
    """Return ``True`` if *cls* declares any ``@event_emitter`` methods directly."""

    return any(
        isinstance(getattr(attr, EVENT_EMITTER_METADATA_FIELD, None), EventEmitterMetadata)
        for attr in cls.__dict__.values()
    )


# ....................... #


def collect_event_emitters(cls: type) -> list[tuple[str, EventEmitterMetadata]]:
    """Collect ``@event_emitter`` methods declared on *cls* and its bases (child wins)."""

    by_name: OrderedDict[str, _EmitterEntry] = OrderedDict()

    for b in reversed(cls.mro()[:-1]):
        for name, attr in b.__dict__.items():
            meta = getattr(attr, EVENT_EMITTER_METADATA_FIELD, None)

            if not isinstance(meta, EventEmitterMetadata):
                continue

            by_name[name] = _EmitterEntry(owner=b, meta=meta)
            by_name.move_to_end(name)

    return [(name, entry.meta) for name, entry in by_name.items()]

"""Aggregate root base with in-process domain-event collection."""

from collections.abc import Mapping
from typing import Any, ClassVar, Self

from pydantic import PrivateAttr

from forze.base.primitives import JsonDict

from .base import CoreModel
from .emitters import EventEmitterMetadata, collect_event_emitters
from .events import DomainEvent

# ----------------------- #


class AggregateRoot(CoreModel):
    """Base for aggregate roots that record domain events.

    Events are collected on the instance in a transient, non-persisted buffer via
    :meth:`record_event` and drained by the application layer via
    :meth:`collect_events` for in-process dispatch. Compose with
    :class:`~forze.domain.models.document.Document` for a persisted aggregate::

        class Order(Document, AggregateRoot): ...

    Behavior methods record events on the instance they return, or declare
    :func:`~forze.domain.models.emitters.event_emitter` methods that raise an event
    from an ``(before, after, diff)`` transition on :meth:`Document.update`.
    """

    _pending_events: list[DomainEvent] = PrivateAttr(default_factory=list)

    _event_emitters_: ClassVar[list[tuple[str, EventEmitterMetadata]]] = []
    """Domain-event emitters collected from this class and its bases."""

    # ....................... #

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._event_emitters_ = collect_event_emitters(cls)

    # ....................... #

    def record_event(self, event: DomainEvent) -> Self:
        """Record a domain event on this aggregate; returns ``self`` for chaining."""

        self._pending_events.append(event)
        return self

    # ....................... #

    def collect_events(self) -> tuple[DomainEvent, ...]:
        """Drain and return the recorded domain events, clearing the buffer."""

        events = tuple(self._pending_events)
        self._pending_events.clear()
        return events

    # ....................... #

    @property
    def has_pending_events(self) -> bool:
        """Whether any domain events are pending dispatch."""

        return bool(self._pending_events)

    # ....................... #

    def _emit_domain_events(self, before: Self, diff: JsonDict) -> None:
        """Run the declared emitters and record their events on this instance.

        Called by :meth:`Document.update` on the **result** instance (``self`` is
        ``after``); each emitter is a pure ``(before, after, diff) -> event | None``.
        """

        keys = diff.keys()
        cls = type(self)

        for name, meta in cls._event_emitters_:
            if meta.fields is not None and keys.isdisjoint(meta.fields):
                continue

            event = getattr(cls, name)(before, self, diff)

            if event is not None:
                self.record_event(event)

    # ....................... #

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> Self:
        """Copy the aggregate with an **independent** pending-events buffer.

        Pydantic's ``model_copy`` shares private-attribute lists by reference; without
        this override an aggregate and its copies (e.g. from ``Document.update``)
        would alias events and risk double-dispatch.
        """

        new = super().model_copy(update=update, deep=deep)
        new._pending_events = list(self._pending_events)

        return new

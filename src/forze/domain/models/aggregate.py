"""Aggregate root base with in-process domain-event collection."""

from typing import Any, Mapping, Self

from pydantic import PrivateAttr

from .base import CoreModel
from .events import DomainEvent

# ----------------------- #


class AggregateRoot(CoreModel):
    """Base for aggregate roots that record domain events.

    Events are collected on the instance in a transient, non-persisted buffer via
    :meth:`record_event` and drained by the application layer via
    :meth:`collect_events` for in-process dispatch. Compose with
    :class:`~forze.domain.models.document.Document` for a persisted aggregate::

        class Order(Document, AggregateRoot): ...

    Behavior methods record events on the instance they return.
    """

    _pending_events: list[DomainEvent] = PrivateAttr(default_factory=list)

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

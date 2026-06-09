"""Map integration event types to notification commands."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from forze.application.contracts.outbox import IntegrationEvent
from forze.base.exceptions import exc

from .payloads import NotificationCommand

# ----------------------- #

EventMapper = Callable[[IntegrationEvent[Any]], list[NotificationCommand]]


class NotificationRouter:
    """Resolve :class:`~forze.application.contracts.outbox.IntegrationEvent` to notification commands."""

    def __init__(self) -> None:
        self._mappers: dict[str, EventMapper] = {}

    def register(self, event_type: str, mapper: EventMapper) -> None:
        """Register *mapper* for an integration ``event_type`` string."""

        self._mappers[event_type] = mapper

    def resolve(self, event: IntegrationEvent[Any]) -> list[NotificationCommand]:
        """Return notification commands for *event*, or an empty list when unmapped."""

        mapper = self._mappers.get(event.event_type)
        if mapper is None:
            return []
        return mapper(event)

    def resolve_or_raise(self, event: IntegrationEvent[Any]) -> list[NotificationCommand]:
        """Like :meth:`resolve`, but raise when *event_type* has no mapper."""

        if event.event_type not in self._mappers:
            raise exc.precondition(f"no notification mapper for event_type={event.event_type!r}")
        return self.resolve(event)

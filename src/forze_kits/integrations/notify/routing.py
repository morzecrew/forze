"""Map integration event types to notification commands.

Registration and resolution are separate, mirroring the framework's other registries
(``OperationRegistry`` → ``freeze()`` → ``FrozenOperationRegistry``): a mutable
:class:`NotificationRouter` accumulates event-type mappers at wiring time, then
``freeze()`` yields an immutable :class:`FrozenNotificationRouter` that only resolves.
The consumer holds the frozen resolver, so the routing table cannot change under it.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Self, final

import attrs

from forze.application.contracts.outbox import IntegrationEvent
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from .payloads import NotificationCommand

# ----------------------- #

EventMapper = Callable[[IntegrationEvent[Any]], list[NotificationCommand]]

# ....................... #


@final
@attrs.define(slots=True)
class NotificationRouter:
    """Mutable builder: register event-type mappers, then :meth:`freeze` to resolve.

    Holds no resolution logic itself — registration only. Call :meth:`freeze` once at
    wiring time and hand the resulting :class:`FrozenNotificationRouter` to the consumer.
    """

    _mappers: dict[StrKey, EventMapper] = attrs.field(factory=dict, alias="mappers")

    # ....................... #

    def register(self, event_type: StrKey, mapper: EventMapper) -> Self:
        """Register *mapper* for an integration ``event_type`` string; returns ``self``."""

        self._mappers[event_type] = mapper
        return self

    # ....................... #

    def freeze(self) -> FrozenNotificationRouter:
        """Snapshot the registered mappers into an immutable resolver."""

        return FrozenNotificationRouter(mappers=dict(self._mappers))


# ....................... #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class FrozenNotificationRouter:
    """Immutable resolver: map an :class:`IntegrationEvent` to notification commands."""

    mappers: Mapping[StrKey, EventMapper]
    """The frozen event-type → mapper table (built via :meth:`NotificationRouter.freeze`)."""

    # ....................... #

    def resolve(self, event: IntegrationEvent[Any]) -> list[NotificationCommand]:
        """Return notification commands for *event*, or an empty list when unmapped."""

        mapper = self.mappers.get(event.event_type)

        return [] if mapper is None else mapper(event)

    # ....................... #

    def resolve_or_raise(
        self,
        event: IntegrationEvent[Any],
    ) -> list[NotificationCommand]:
        """Like :meth:`resolve`, but raise when *event_type* has no mapper."""

        if event.event_type not in self.mappers:
            raise exc.precondition(
                f"no notification mapper for event_type={event.event_type!r}"
            )

        return self.resolve(event)

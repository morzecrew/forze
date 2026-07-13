"""The realtime signal — the narrow waist of the egress plane.

A :class:`RealtimeSignal` is the message an application publishes (as data) onto
a messaging substrate and that a gateway consumes to bridge to live connections.
It is a plain pydantic model so it serialises through the standard message codec;
the audience is flattened onto it (no tenant — that travels in the relay/message
headers), and the payload rides as already-serialised JSON.

Build typed signals with :meth:`RealtimeSignal.for_event`, which validates the
payload against its declared :class:`RealtimeEvent` and enforces the event's
audience-kind constraint.
"""

from collections.abc import Mapping
from typing import Any, Self

from pydantic import BaseModel, ConfigDict

from forze.base.exceptions import exc

from .audience import Audience, AudienceKind
from .events import RealtimeEvent

# ----------------------- #


class RealtimeSignal(BaseModel):
    """A server→client signal: a logical audience, an event name, and a payload."""

    model_config = ConfigDict(frozen=True)

    audience_kind: AudienceKind
    """Kind of the target audience."""

    audience_name: str
    """Selector key of the target audience."""

    event: str
    """The declared event name (resolved against the catalog by the edges)."""

    payload: dict[str, Any]
    """The event payload in JSON-compatible form."""

    # ....................... #

    @property
    def audience(self) -> Audience:
        """Reconstruct the logical :class:`Audience` this signal targets."""

        return Audience(kind=self.audience_kind, name=self.audience_name)

    # ....................... #

    @classmethod
    def of(cls, audience: Audience, event: str, payload: Mapping[str, Any]) -> Self:
        """Build a signal from raw parts (no catalog validation)."""

        return cls(
            audience_kind=audience.kind,
            audience_name=audience.name,
            event=event,
            payload=dict(payload),
        )

    # ....................... #

    @classmethod
    def for_event[P: BaseModel](
        cls,
        audience: Audience,
        event: RealtimeEvent[P],
        payload: P,
    ) -> Self:
        """Build a typed signal, validating *payload* against *event*.

        :raises exc.precondition: If *event* may not target *audience*'s kind.
        """

        if not event.accepts(audience):
            raise exc.precondition(
                f"Realtime event `{event.name}` may not target a `{audience.kind.value}` audience"
            )

        validated = event.parse(payload)

        return cls.of(audience, event.name, validated.model_dump(mode="json"))

"""Declared realtime events — a capability the edges recognize.

A realtime event is **declared and typed**, not a free-form string: a
:class:`RealtimeEvent` binds an event name to its payload model (and optionally
constrains which audience kinds it may target). The set of them is a frozen
:class:`RealtimeEventCatalog` — the egress twin of the inbound command catalog /
operation registry.

Because the catalog is declarative data in core, every edge can consume it: the
publish surface validates payloads against it at the call site, the gateway
validates/deserialises against it, and tooling can enumerate the realtime egress
surface (client typings, docs) from it — just as the operation catalog drives
route generation + OpenAPI on the inbound side.
"""

from collections.abc import Iterator, Mapping
from typing import Any, Self, final

import attrs
from pydantic import BaseModel

from forze.base.exceptions import exc

from .audience import Audience, AudienceKind

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RealtimeEvent[Payload: BaseModel]:
    """A declared, typed realtime event: a name bound to its payload model."""

    name: str
    """The event name as it appears on the wire and at the client."""

    payload_type: type[Payload]
    """The payload model; payloads are validated against it."""

    audience_kinds: frozenset[AudienceKind] | None = None
    """Optional constraint: the audience kinds this event may target.

    ``None`` means any kind. A non-``None`` set lets the edges reject, e.g.,
    a ``message.new`` aimed at a ``principal`` when it is topic-only.
    """

    offline_delivery: bool = True
    """Declared delivery tier: whether this event is worth delivering to a recipient
    who is offline when it is sent (vs. live-only, best-effort).

    A declared property the edges recognise (like :attr:`audience_kinds`): ``True``
    (default) means a durable, principal-addressed signal of this event should still
    reach the recipient when they reconnect; ``False`` opts it out as emit-only — e.g.
    a high-frequency signal promoted to durable for ordering but not worth retaining.
    *How* an edge fulfils offline delivery (a store-and-forward gateway, etc.) is the
    edge's concern, not the contract's.
    """

    # ....................... #

    def accepts(self, audience: Audience) -> bool:
        """Return whether *audience* satisfies this event's :attr:`audience_kinds`."""

        return self.audience_kinds is None or audience.kind in self.audience_kinds

    # ....................... #

    def parse(self, payload: Any) -> Payload:
        """Validate *payload* (model or mapping) into this event's payload model."""

        return self.payload_type.model_validate(payload)


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class RealtimeEventCatalog:
    """A frozen registry of declared :class:`RealtimeEvent` s, keyed by name."""

    _by_name: Mapping[str, RealtimeEvent[Any]] = attrs.field(repr=False)

    # ....................... #

    @classmethod
    def of(cls, *events: RealtimeEvent[Any]) -> Self:
        """Build a catalog from *events*, rejecting duplicate names.

        :raises exc.configuration: If two events share a name.
        """

        by_name: dict[str, RealtimeEvent[Any]] = {}

        for event in events:
            if event.name in by_name:
                raise exc.configuration(f"Realtime event `{event.name}` is declared more than once")

            by_name[event.name] = event

        return cls(by_name)

    # ....................... #

    def get(self, name: str) -> RealtimeEvent[Any] | None:
        """Return the declared event named *name*, or :obj:`None`."""

        return self._by_name.get(name)

    # ....................... #

    def require(self, name: str) -> RealtimeEvent[Any]:
        """Return the declared event named *name*.

        :raises exc.configuration: If no such event is declared.
        """

        event = self._by_name.get(name)

        if event is None:
            raise exc.configuration(f"Realtime event `{name}` is not declared")

        return event

    # ....................... #

    def __iter__(self) -> Iterator[RealtimeEvent[Any]]:
        return iter(self._by_name.values())

    # ....................... #

    def __len__(self) -> int:
        return len(self._by_name)

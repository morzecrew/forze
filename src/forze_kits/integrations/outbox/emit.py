"""Bind an aggregate's outbox emit-and-relay wiring from one declaration.

Standing up the transactional outbox for an aggregate is four hand-wired pieces, assembled
identically every time: the domain-event -> integration-event **bridge** (staging), the in-tx
**flush** hook (atomicity — forget it and you get a dual-write), the background **relay** step
(delivery), and the backend ``outboxes=`` config. This module folds the three *app-layer* pieces
into one :class:`OutboxEmit` declaration; :func:`bind_outbox` returns an :class:`OutboxWiring`
carrying the bridge registrations (merge into a
:class:`~forze.application.execution.domain.DomainEventRegistry`), the on-success flush hook
(attach to the write operation's plan), and the relay lifecycle step (register on the runtime).

The backend ``outboxes={name: cfg}`` config stays the author's — it belongs to the deps module,
not the app-layer wiring, and keeps the hexagonal layer split intact.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import Any, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.base import BaseSpec
from forze.application.contracts.execution import (
    LifecycleStep,
    OnSuccessFactory,
    OnSuccessStep,
)
from forze.application.contracts.outbox import OutboxDestinationKind, OutboxSpec
from forze.application.contracts.pubsub import PubSubSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.contracts.stream import StreamSpec
from forze.application.execution.domain import (
    DomainEventHandlerFactory,
    DomainEventRegistry,
    outbox_event_handler,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKey
from forze.domain.models import DomainEvent

from .flush import outbox_flush_tx_on_success_factory
from .lifecycle import outbox_relay_background_lifecycle_step

# ----------------------- #


@final
@attrs.frozen(kw_only=True)
class EmitMapping[E: DomainEvent, M: BaseModel]:
    """One domain-event -> integration-event staging rule for an outbox route.

    When an aggregate emits *event*, the bridge stages an integration event of type
    *event_type* whose payload is ``to_payload(event)`` (a codec-typed model). The
    mapping is subclass-aware: a rule for a base event type matches its subclasses.
    """

    event: type[E]
    """Domain-event type this rule stages for (isinstance-matched)."""

    event_type: str
    """Integration-event type string recorded on the staged outbox row."""

    to_payload: Callable[[E], M]
    """Maps the domain event to the outbox route's codec payload model."""


# ....................... #


@final
@attrs.frozen(kw_only=True)
class RelayBinding:
    """Background-relay configuration for an outbox route.

    A faithful, aggregate-scoped config over
    :func:`~forze_kits.integrations.outbox.outbox_relay_background_lifecycle_step`; the
    knobs and their defaults mirror it. Opt-in — omit :attr:`OutboxEmit.relay` when the
    relay runs as a separate worker/cron (the common production shape) and drive it there.
    """

    transport: OutboxDestinationKind = "queue"
    """Which transport each tick relays to (default ``queue``)."""

    queue_spec: QueueSpec[Any] | None = None
    """Target queue spec (required when :attr:`transport` is ``queue``)."""

    stream_spec: StreamSpec[Any] | None = None
    """Target stream spec (required when :attr:`transport` is ``stream``)."""

    pubsub_spec: PubSubSpec[Any] | None = None
    """Target pubsub spec (required when :attr:`transport` is ``pubsub``)."""

    interval: timedelta = timedelta(seconds=30)
    """Sleep between relay ticks."""

    jitter: float = 0.2
    """Multiplicative tick jitter in ``[0, 1)`` (desynchronizes N replicas)."""

    reclaim_stale_after: timedelta | None = timedelta(minutes=5)
    """Reset rows stuck ``processing`` longer than this before claim (``None`` skips)."""

    limit: int | None = None
    """Per-batch claim size (``None`` = backend default)."""

    max_attempts: int = 5
    """Publish attempts before a transiently-failing row is parked ``failed``."""

    retry_base_delay: timedelta = timedelta(seconds=1)
    """Base of the per-row exponential retry backoff."""

    retry_max_backoff: timedelta = timedelta(minutes=5)
    """Cap on the per-row retry backoff."""

    max_batches_per_tick: int = 100
    """Batches drained per tick before yielding (starvation cap)."""

    tenants: Callable[[], Sequence[UUID]] | None = None
    """When set, the outbox is tenant-partitioned; the shard is frozen at startup."""

    drain_on_shutdown: bool = False
    """Publish what is still claimable at shutdown instead of leaving it pending.

    Rejected for a ``pubsub`` transport — see
    :func:`~forze_kits.integrations.outbox.outbox_relay_background_lifecycle_step`. The drain
    touches the database during teardown, but needs no ordering of its own to do so: the
    runtime stops every background loop *before* lifecycle teardown begins, so the client is
    still open."""

    shutdown_drain_timeout: timedelta = timedelta(seconds=5)
    """Budget for the shutdown drain; keep it under the runtime's ``shutdown_step_timeout``."""

    requires: tuple[StrKey, ...] = ()
    """Capabilities this step is ordered after (e.g. the one its database client provides)."""

    depends_on: tuple[StrKey, ...] = ()
    """Step ids this step is ordered after."""

    # ....................... #

    @property
    def transport_spec(self) -> BaseSpec | None:
        """The one transport spec this relay actually binds, by its :attr:`transport`.

        A queue *and* a stream spec may both be set; only the one :attr:`transport` names is ever
        resolved, so this is the single answer. Handing the others to the inventory would have it
        demand a dependency route that nothing wires.

        ``None`` means the spec has not been supplied — legal only where something else supplies
        it. An aggregate's search-sync relay is that case: its queue is *derived* by the kit and
        evolved in, so the author writes tick knobs alone. On :class:`OutboxEmit` there is nothing
        to derive from, and a relay with no destination is refused at construction.
        """

        match self.transport:
            case "queue":
                return self.queue_spec

            case "stream":
                return self.stream_spec

            case "pubsub":
                return self.pubsub_spec

            case _:  # pragma: no cover - the destination kinds are exhaustive  # pyright: ignore[reportUnnecessaryComparison]
                return None

    # ....................... #

    def as_lifecycle_step(
        self, outbox_spec: OutboxSpec[Any], *, step_id: StrKey = "outbox_relay"
    ) -> LifecycleStep:
        """Build the background relay lifecycle step for *outbox_spec* under this config."""

        return outbox_relay_background_lifecycle_step(
            outbox_spec=outbox_spec,
            transport=self.transport,
            queue_spec=self.queue_spec,
            stream_spec=self.stream_spec,
            pubsub_spec=self.pubsub_spec,
            interval=self.interval,
            jitter=self.jitter,
            reclaim_stale_after=self.reclaim_stale_after,
            limit=self.limit,
            max_attempts=self.max_attempts,
            retry_base_delay=self.retry_base_delay,
            retry_max_backoff=self.retry_max_backoff,
            max_batches_per_tick=self.max_batches_per_tick,
            tenants=self.tenants,
            drain_on_shutdown=self.drain_on_shutdown,
            shutdown_drain_timeout=self.shutdown_drain_timeout,
            requires=self.requires,
            depends_on=self.depends_on,
            step_id=step_id,
        )


# ....................... #


@final
@attrs.frozen(kw_only=True)
class OutboxEmit:
    """One declaration binding an aggregate's domain events to an outbox route.

    Bundles the outbox route, the domain-event staging rules (:attr:`emits`), and an
    optional background :attr:`relay`. Pass it to :func:`bind_outbox` to get the bridge
    registrations + flush hook + relay step ready to compose.
    """

    spec: OutboxSpec[Any]
    """The outbox route staged events land on."""

    emits: tuple[EmitMapping[Any, Any], ...] = attrs.field(converter=tuple)
    """The domain-event -> integration-event staging rules (at least one)."""

    relay: RelayBinding | None = None
    """Optional background relay; omit when relay runs out-of-process. Its transport spec is the
    author's and is **required** — unlike a search-sync relay, whose queue the kit derives."""

    # ....................... #

    @property
    def relay_transport_spec(self) -> BaseSpec | None:
        """Where the relay publishes, or ``None`` when there is no relay.

        ``None`` means *no relay* and never "a relay whose destination is missing" — that is
        refused at construction. The distinction is the whole point: the inventory reads this
        (``AggregateKit.spec_contributions``), and a missing destination there would not be an
        error but an *absence*, quietly dropping the route from the catalogue.
        """

        return self.relay.transport_spec if self.relay is not None else None

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.emits:
            raise exc.configuration(
                f"OutboxEmit for route {self.spec.name!r} must declare at least one emit mapping"
            )

        if self.relay is not None and self.relay.transport_spec is None:
            raise exc.precondition(
                f"OutboxEmit for route {self.spec.name!r} declares a relay with transport "
                f"{self.relay.transport!r} but no {self.relay.transport}_spec; the relay has "
                f"nowhere to publish."
            )


# ....................... #


@final
@attrs.frozen(kw_only=True)
class OutboxWiring:
    """The composed outbox wiring for one aggregate, emitted as separate artifacts.

    Never a coupled god-object: :meth:`register_events` merges the bridge into a domain-event
    registry (app layer), :meth:`flush_step` yields the in-tx flush step for a write operation's
    plan (app layer), and :attr:`lifecycle_steps` carries the relay step for the runtime. The
    backend ``outboxes=`` config stays the author's.
    """

    spec: OutboxSpec[Any]
    """The outbox route this wiring drives."""

    event_handlers: tuple[tuple[type[DomainEvent], DomainEventHandlerFactory], ...]
    """The ``(event type, bridge factory)`` pairs to register on a domain-event registry."""

    flush_factory: OnSuccessFactory
    """The in-tx flush hook factory (attach via :meth:`flush_step` or directly)."""

    lifecycle_steps: tuple[LifecycleStep, ...]
    """The background relay step(s); empty when :attr:`OutboxEmit.relay` was omitted."""

    # ....................... #

    def register_events(self, registry: DomainEventRegistry) -> None:
        """Register every staging bridge on *registry* (in declaration order)."""

        for event_type, factory in self.event_handlers:
            registry.register(event_type, factory)

    # ....................... #

    def domain_event_registry(self) -> DomainEventRegistry:
        """Return a fresh :class:`DomainEventRegistry` carrying only these bridges."""

        registry = DomainEventRegistry()
        self.register_events(registry)
        return registry

    # ....................... #

    def flush_step(self, *, step_id: StrKey = "outbox_flush") -> OnSuccessStep:
        """The in-tx flush as an ``on_success`` step for a write operation's plan."""

        return OnSuccessStep(id=step_id, factory=self.flush_factory)


# ....................... #


def bind_outbox(
    emit: OutboxEmit,
    *,
    relay_step_id: StrKey = "outbox_relay",
) -> OutboxWiring:
    """Compose an :class:`OutboxEmit` into its bridge + flush + relay wiring.

    Collapses the four-piece outbox dance to one call: the returned :class:`OutboxWiring`
    carries the staging bridges (merge into a
    :class:`~forze.application.execution.domain.DomainEventRegistry`), the in-tx flush hook
    (attach to the write operation's plan), and the background relay lifecycle step (register
    on the runtime) when :attr:`OutboxEmit.relay` is set. The backend ``outboxes={name: cfg}``
    config is *not* emitted — it stays in the deps module, preserving the app/backend split.
    """

    handlers: tuple[tuple[type[DomainEvent], DomainEventHandlerFactory], ...] = tuple(
        (mapping.event, outbox_event_handler(emit.spec, mapping.event_type, mapping.to_payload))
        for mapping in emit.emits
    )
    relay_steps: tuple[LifecycleStep, ...] = (
        (emit.relay.as_lifecycle_step(emit.spec, step_id=relay_step_id),)
        if emit.relay is not None
        else ()
    )

    return OutboxWiring(
        spec=emit.spec,
        event_handlers=handlers,
        flush_factory=outbox_flush_tx_on_success_factory(emit.spec),
        lifecycle_steps=relay_steps,
    )

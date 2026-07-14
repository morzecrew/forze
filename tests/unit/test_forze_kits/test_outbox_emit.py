"""`bind_outbox` folds the four-piece outbox dance into one declaration (mock).

Proves the three app-layer artifacts an :class:`OutboxEmit` emits actually compose end to end:
the **bridge** stages an integration event when the domain event dispatches, the **flush** hook
persists the staged buffer, and a **relay** drains it to the queue — the same three steps an
author wires by hand, now bound from one declaration. Plus the shape/guard checks.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.execution import LifecycleStep, OnSuccessStep
from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze.application.contracts.queue import QueueQueryDepKey, QueueSpec
from forze.application.contracts.stream import StreamSpec
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.application.execution.domain import InProcessDomainEventDispatcher
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.serialization import PydanticModelCodec
from forze.domain.models import DomainEvent
from forze_kits.integrations.outbox import (
    EmitMapping,
    OutboxEmit,
    OutboxRelay,
    RelayBinding,
    bind_outbox,
)
from forze_mock import MockDepsModule

# ----------------------- #


class OrderConfirmed(DomainEvent):
    aggregate_id: UUID


class OrderShipped(DomainEvent):
    aggregate_id: UUID


class OrderConfirmedPayload(BaseModel):
    order_id: str


# The outbox route name, its destination route, and the queue name must agree for relay.
QUEUE = QueueSpec(name="orders", codec=PydanticModelCodec(OrderConfirmedPayload))
OUTBOX = OutboxSpec(
    name="orders",
    codec=PydanticModelCodec(OrderConfirmedPayload),
    destination=OutboxDestination.queue(route="orders", channel="orders"),
)


def _emit(*, relay: RelayBinding | None = None) -> OutboxEmit:
    return OutboxEmit(
        spec=OUTBOX,
        emits=(
            EmitMapping(
                event=OrderConfirmed,
                event_type="order.confirmed",
                to_payload=lambda e: OrderConfirmedPayload(order_id=str(e.aggregate_id)),
            ),
        ),
        relay=relay,
    )


def _ctx() -> ExecutionContext:
    return ExecutionContext(
        deps=DepsRegistry.from_modules(MockDepsModule()).freeze().resolve()
    )


# ....................... #


class TestBindOutboxShape:
    def test_emits_one_bridge_per_mapping(self) -> None:
        emit = OutboxEmit(
            spec=OUTBOX,
            emits=(
                EmitMapping(
                    event=OrderConfirmed,
                    event_type="order.confirmed",
                    to_payload=lambda e: OrderConfirmedPayload(order_id=str(e.aggregate_id)),
                ),
                EmitMapping(
                    event=OrderShipped,
                    event_type="order.shipped",
                    to_payload=lambda e: OrderConfirmedPayload(order_id=str(e.aggregate_id)),
                ),
            ),
        )

        wiring = bind_outbox(emit)

        # one bridge per mapping, in declaration order
        assert [event for event, _ in wiring.event_handlers] == [OrderConfirmed, OrderShipped]

    def test_flush_step_carries_the_given_id(self) -> None:
        step = bind_outbox(_emit()).flush_step(step_id="confirm_flush")

        assert isinstance(step, OnSuccessStep)
        assert step.id == "confirm_flush"

    def test_relay_step_emitted_only_when_relay_is_configured(self) -> None:
        assert bind_outbox(_emit()).lifecycle_steps == ()

        steps = bind_outbox(_emit(relay=RelayBinding(queue_spec=QUEUE))).lifecycle_steps
        assert len(steps) == 1
        assert isinstance(steps[0], LifecycleStep)

    def test_empty_emits_is_rejected(self) -> None:
        with pytest.raises(CoreException) as ei:
            OutboxEmit(spec=OUTBOX, emits=())

        assert ei.value.kind is ExceptionKind.CONFIGURATION


# ....................... #


class TestBindOutboxComposesEndToEnd:
    async def test_dispatch_stages_flush_persists_relay_delivers(self) -> None:
        ctx = _ctx()
        wiring = bind_outbox(_emit(relay=RelayBinding(queue_spec=QUEUE)))
        order_id = uuid4()

        # Bridge: dispatching the domain event stages an integration event (buffered).
        dispatcher = InProcessDomainEventDispatcher(
            registry=wiring.domain_event_registry(), ctx=ctx
        )
        await dispatcher.dispatch([OrderConfirmed(aggregate_id=order_id)])

        # Flush: the on-success hook persists the staged buffer so the relay can claim it.
        await wiring.flush_factory(ctx)(None, None)

        # Relay: drains the staged row to the queue.
        result = await OutboxRelay(outbox_spec=OUTBOX).to_queue(ctx, QUEUE)
        assert result.published == 1

        queue = ctx.deps.resolve_configurable(
            ctx, QueueQueryDepKey, QUEUE, route=QUEUE.name
        )
        messages = await queue.receive("orders")
        assert len(messages) == 1
        assert messages[0].type == "order.confirmed"

    async def test_unflushed_stage_is_not_relayed(self) -> None:
        # Without the flush hook the staged event stays buffered — the dual-write guard the
        # flush piece exists for. The relay claims nothing.
        ctx = _ctx()
        wiring = bind_outbox(_emit())

        dispatcher = InProcessDomainEventDispatcher(
            registry=wiring.domain_event_registry(), ctx=ctx
        )
        await dispatcher.dispatch([OrderConfirmed(aggregate_id=uuid4())])

        result = await OutboxRelay(outbox_spec=OUTBOX).to_queue(ctx, QUEUE)
        assert result.published == 0


# ....................... #


class TestRelayDestinationIsRequired:
    """A relay with no destination must not construct — and must not merely go missing.

    ``AggregateKit.spec_contributions`` reads the relay's transport spec into the inventory. A
    transport whose spec was never supplied does not surface there as an error; it surfaces as an
    *absence* — the destination route drops silently out of the catalogue, which is the one thing
    an inventory may never do. So it is refused where the author declares it.
    """

    @pytest.mark.parametrize("transport", ["queue", "stream", "pubsub"])
    def test_a_transport_without_its_spec_is_refused(self, transport: str) -> None:
        with pytest.raises(CoreException) as ei:
            _emit(relay=RelayBinding(transport=transport))  # type: ignore[arg-type]

        assert ei.value.kind is ExceptionKind.PRECONDITION
        assert "nowhere to publish" in str(ei.value)

    def test_the_declared_transport_is_the_one_that_counts(self) -> None:
        # A stream spec set on a queue-transport relay is not the destination and cannot stand in
        # for the missing queue: only the spec ``transport`` names is ever resolved.
        stray = StreamSpec(name="elsewhere", codec=PydanticModelCodec(OrderConfirmedPayload))

        with pytest.raises(CoreException):
            _emit(relay=RelayBinding(transport="queue", stream_spec=stray))

        emit = _emit(relay=RelayBinding(transport="queue", queue_spec=QUEUE, stream_spec=stray))

        assert emit.relay_transport_spec is QUEUE

    def test_no_relay_means_no_transport_spec(self) -> None:
        # The one remaining ``None`` — and now the only thing it can mean.
        assert _emit().relay_transport_spec is None

    def test_a_search_sync_relay_may_omit_its_queue(self) -> None:
        """The kit derives that queue and evolves it in, so the author writes tick knobs alone.

        This is why the guard lives on ``OutboxEmit`` and not on ``RelayBinding``: the same class
        serves a role where the destination is genuinely not the author's to supply.
        """

        partial = RelayBinding(interval=timedelta(milliseconds=20), jitter=0.0)

        assert partial.transport_spec is None

        completed = attrs.evolve(partial, transport="queue", queue_spec=QUEUE)

        assert completed.transport_spec is QUEUE

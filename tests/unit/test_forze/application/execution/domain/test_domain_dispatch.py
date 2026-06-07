"""Tests for the in-process domain-event dispatcher and outbox bridge."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxSpec
from forze.application.execution import (
    DomainEventRegistry,
    ExecutionContext,
    InProcessDomainEventDispatcher,
    outbox_event_handler,
)
from forze.base.serialization import PydanticModelCodec
from forze.domain.models import AggregateRoot, Document, DomainEvent
from tests.support.execution_context import context_from_modules

from forze_mock import MockDepsModule

# ----------------------- #


class OrderCreated(DomainEvent):
    aggregate_id: UUID


class OtherEvent(DomainEvent):
    pass


class OrderPayload(BaseModel):
    order_id: str


class Order(Document, AggregateRoot):
    name: str = "init"


def _spec() -> OutboxSpec[OrderPayload]:
    return OutboxSpec(name="orders", codec=PydanticModelCodec(OrderPayload))


def _ctx() -> ExecutionContext:
    return context_from_modules(MockDepsModule())


# ....................... #


def _noop_factory(
    _ctx: ExecutionContext,
) -> Callable[[DomainEvent], Awaitable[None]]:
    async def _handler(_event: DomainEvent) -> None: ...

    return _handler


class TestRegistry:
    def test_isinstance_matching_catches_subclass(self) -> None:
        registry = DomainEventRegistry()

        registry.register(DomainEvent, _noop_factory)  # base type catches all
        assert registry.factories_for(OrderCreated(aggregate_id=UUID(int=1))) == [
            _noop_factory,
        ]

    def test_no_match_for_unrelated_type(self) -> None:
        registry = DomainEventRegistry()

        registry.register(OrderCreated, _noop_factory)
        assert registry.factories_for(OtherEvent()) == []


class TestDispatcher:
    async def test_runs_handlers_in_order_factory_gets_ctx(self) -> None:
        ctx = _ctx()
        registry = DomainEventRegistry()
        calls: list[tuple[str, bool]] = []

        def make(name: str) -> Callable[
            [ExecutionContext], Callable[[OrderCreated], Awaitable[None]]
        ]:
            def factory(c: ExecutionContext) -> Callable[[OrderCreated], Awaitable[None]]:
                # The factory receives ctx; the running handler is ctx-free.
                async def handler(_event: OrderCreated) -> None:
                    calls.append((name, c is ctx))

                return handler

            return factory

        registry.register(OrderCreated, make("h1"))
        registry.register(OrderCreated, make("h2"))
        dispatcher = InProcessDomainEventDispatcher(registry=registry, ctx=ctx)

        await dispatcher.dispatch([OrderCreated(aggregate_id=UUID(int=1))])

        assert calls == [("h1", True), ("h2", True)]


class TestOutboxBridge:
    async def test_domain_event_stages_integration_event(self) -> None:
        spec = _spec()
        registry = DomainEventRegistry()
        registry.register(
            OrderCreated,
            outbox_event_handler(
                spec,
                "order.created",
                lambda e: OrderPayload(order_id=str(e.aggregate_id)),
            ),
        )
        ctx = context_from_modules(MockDepsModule(domain_events=registry))

        order = Order(name="x")
        order.record_event(OrderCreated(aggregate_id=order.id))

        await ctx.domain().dispatch(order.collect_events())

        staged = ctx.outbox_staging.buffer.peek()
        assert len(staged) == 1
        assert staged[0].event.event_type == "order.created"
        assert staged[0].event.payload.order_id == str(order.id)
        assert order.has_pending_events is False  # drained by collect_events


class TestCtxDomain:
    async def test_empty_registry_dispatch_is_noop(self) -> None:
        ctx = _ctx()
        order = Order(name="x")

        await ctx.domain().dispatch([OrderCreated(aggregate_id=order.id)])

        assert ctx.outbox_staging.buffer.peek() == []

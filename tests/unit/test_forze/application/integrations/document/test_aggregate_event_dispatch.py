"""Phase 1: an aggregate's domain events dispatch through the document command flow."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import DomainEventRegistry
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import (
    AggregateRoot,
    BaseDTO,
    CreateDocumentCmd,
    Document,
    DomainEvent,
    ReadDocument,
    event_emitter,
)
from tests.support.execution_context import context_from_deps

from forze_mock import MockDepsModule
from forze_mock.adapters import MockDocumentAdapter
from forze_mock.state import MockState

# ----------------------- #


class OrderConfirmed(DomainEvent):
    aggregate_id: UUID


class Order(Document, AggregateRoot):
    status: str = "pending"

    @event_emitter(fields={"status"})
    def _on_confirm(before, after, diff) -> DomainEvent | None:  # type: ignore[no-untyped-def]
        if after.status == "confirmed" and before.status != "confirmed":
            return OrderConfirmed(aggregate_id=after.id)
        return None


class OrderCreate(CreateDocumentCmd):
    status: str = "pending"


class OrderUpdate(BaseDTO):
    status: str | None = None


class OrderRead(ReadDocument):
    status: str


_ORDER_SPEC: DocumentSpec[OrderRead, Order, OrderCreate, OrderUpdate] = DocumentSpec(
    name="orders",
    read=OrderRead,
    write=DocumentWriteTypes(
        domain=Order, create_cmd=OrderCreate, update_cmd=OrderUpdate
    ),
)


class Plain(Document):
    name: str = "x"


class PlainCreate(CreateDocumentCmd):
    name: str = "x"


class PlainUpdate(BaseDTO):
    name: str | None = None


class PlainRead(ReadDocument):
    name: str


_PLAIN_SPEC: DocumentSpec[PlainRead, Plain, PlainCreate, PlainUpdate] = DocumentSpec(
    name="plain",
    read=PlainRead,
    write=DocumentWriteTypes(
        domain=Plain, create_cmd=PlainCreate, update_cmd=PlainUpdate
    ),
)


def _capturing_registry(seen: list[DomainEvent]) -> DomainEventRegistry:
    registry = DomainEventRegistry()

    def factory(_ctx: Any):  # type: ignore[no-untyped-def]
        async def handler(event: OrderConfirmed) -> None:
            seen.append(event)

        return handler

    registry.register(OrderConfirmed, factory)
    return registry


class TestAggregateEventDispatch:
    async def test_update_dispatches_emitter_event_in_command_flow(self) -> None:
        seen: list[DomainEvent] = []
        ctx = context_from_deps(
            MockDepsModule(domain_events=_capturing_registry(seen))()
        )
        cmd = ctx.document.command(_ORDER_SPEC)

        created = await cmd.create(OrderCreate())
        assert seen == []  # create transition emits nothing

        await cmd.update(created.id, created.rev, OrderUpdate(status="confirmed"))

        assert len(seen) == 1
        assert isinstance(seen[0], OrderConfirmed)
        assert seen[0].aggregate_id == created.id

    async def test_non_aggregate_document_dispatches_nothing(self) -> None:
        seen: list[DomainEvent] = []
        ctx = context_from_deps(
            MockDepsModule(domain_events=_capturing_registry(seen))()
        )
        cmd = ctx.document.command(_PLAIN_SPEC)

        created = await cmd.create(PlainCreate())
        await cmd.update(created.id, created.rev, PlainUpdate(name="y"))

        assert seen == []

    async def test_emitting_without_dispatcher_raises(self) -> None:
        # An aggregate that emits events but no dispatcher is wired must raise, never
        # silently drop the event. Build the adapter with a no-dispatcher provider.
        adapter = MockDocumentAdapter[OrderRead, Order, OrderCreate, OrderUpdate](
            spec=_ORDER_SPEC,
            state=MockState(),
            namespace="orders",
            read_model=OrderRead,
            domain_model=Order,
            dispatcher_provider=lambda: None,
            tenant_aware=False,
            tenant_provider=lambda: None,
        )

        created = await adapter.create(OrderCreate())

        with pytest.raises(CoreException) as ei:
            await adapter.update(
                created.id, created.rev, OrderUpdate(status="confirmed")
            )

        assert ei.value.kind is ExceptionKind.CONFIGURATION

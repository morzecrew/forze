"""Phase 2: AggregateRepository load -> decide -> apply, with in-tx event dispatch."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import DomainEventRegistry
from forze.base.exceptions import CoreException, exc
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

from forze_kits.aggregates import aggregate_repository
from forze_mock import MockDepsModule

# ----------------------- #


class OrderConfirmed(DomainEvent):
    aggregate_id: UUID


class OrderUpdate(BaseDTO):
    status: str | None = None


class Order(Document, AggregateRoot):
    status: str = "pending"

    @event_emitter(fields={"status"})
    def _on_confirm(before, after, diff) -> DomainEvent | None:  # type: ignore[no-untyped-def]
        if after.status == "confirmed" and before.status != "confirmed":
            return OrderConfirmed(aggregate_id=after.id)
        return None

    def confirm(self) -> OrderUpdate:
        """Decider: pure decision returning a merge-patch (raises on invalid)."""
        if self.status != "pending":
            raise exc.domain("only pending orders can be confirmed")
        return OrderUpdate(status="confirmed")


class OrderCreate(CreateDocumentCmd):
    status: str = "pending"


class OrderRead(ReadDocument):
    status: str


_SPEC: DocumentSpec[OrderRead, Order, OrderCreate, OrderUpdate] = DocumentSpec(
    name="kit_orders",
    read=OrderRead,
    write=DocumentWriteTypes(
        domain=Order, create_cmd=OrderCreate, update_cmd=OrderUpdate
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


class TestAggregateRepository:
    async def test_load_reconstructs_the_domain_aggregate(self) -> None:
        ctx = context_from_deps(MockDepsModule()())
        repo = aggregate_repository(ctx, _SPEC)

        created = await repo.add(OrderCreate())
        order = await repo.load(created.id)

        assert isinstance(order, Order)
        assert order.id == created.id
        assert order.rev == created.rev
        assert order.status == "pending"

    async def test_load_decide_apply_dispatches_event(self) -> None:
        seen: list[DomainEvent] = []
        ctx = context_from_deps(
            MockDepsModule(domain_events=_capturing_registry(seen))()
        )
        repo = aggregate_repository(ctx, _SPEC)

        created = await repo.add(OrderCreate())

        order = await repo.load(created.id)
        patch = order.confirm()  # decision on the aggregate
        result = await repo.apply(order, patch)

        assert result.status == "confirmed"
        assert len(seen) == 1
        assert isinstance(seen[0], OrderConfirmed)
        assert seen[0].aggregate_id == created.id

    async def test_decider_rejects_invalid_transition(self) -> None:
        ctx = context_from_deps(MockDepsModule()())
        repo = aggregate_repository(ctx, _SPEC)

        created = await repo.add(OrderCreate(status="confirmed"))
        order = await repo.load(created.id)

        with pytest.raises(CoreException):
            order.confirm()

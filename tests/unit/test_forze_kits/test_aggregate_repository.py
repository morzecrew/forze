"""Phase 2: AggregateRepository load -> decide -> apply, with in-tx event dispatch."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest
from pydantic import Field, computed_field

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import DomainEventRegistry
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import utcnow, uuid7
from forze.domain.models import (
    AggregateRoot,
    BaseDTO,
    CreateDocumentCmd,
    Document,
    DomainEvent,
    ReadDocument,
    event_emitter,
    invariant,
)
from forze_kits.aggregates import aggregate_repository
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_deps

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


# ----------------------- #
# read-model -> domain conversion fidelity
#
# ``AggregateRepository.load`` reconstructs the domain aggregate via
# ``model_validate(read, from_attributes=True)`` instead of the prior
# ``model_validate(read.model_dump())`` roundtrip. These pin the two as equivalent on
# the cases that distinguish them — computed fields and serialization aliases — so the
# cheaper path can never silently diverge from the dump-roundtrip semantics.
# ----------------------- #


class _LoadRead(ReadDocument):
    amount: float
    label: str = Field(serialization_alias="label_out")

    @computed_field
    @property
    def doubled(self) -> float:
        return self.amount * 2


class _LoadAgg(Document):
    amount: float
    label: str
    doubled: float  # regular field on the domain, fed from the read's computed field

    @invariant
    def _amount_non_negative(self) -> None:
        if self.amount < 0:
            raise exc.domain("amount must be >= 0")


def _load_read(amount: float = 50.0) -> _LoadRead:
    return _LoadRead(
        id=uuid7(),
        rev=1,
        created_at=utcnow(),
        last_update_at=utcnow(),
        amount=amount,
        label="x",
    )


def test_from_attributes_load_matches_model_dump_roundtrip() -> None:
    """``from_attributes`` reconstructs the same aggregate as the dump roundtrip.

    Covers a computed field (``doubled``) and a serialization alias (``label``) —
    exactly where ``read.__dict__`` would crash or drop data.
    """

    read = _load_read()

    via_attributes = _LoadAgg.model_validate(read, from_attributes=True)
    via_roundtrip = _LoadAgg.model_validate(read.model_dump())

    assert via_attributes == via_roundtrip
    assert via_attributes.doubled == 100.0  # computed field carried through
    assert via_attributes.label == "x"


def test_from_attributes_load_still_enforces_invariants() -> None:
    """The cheaper conversion path keeps running the domain type's invariants."""

    bad = _load_read(amount=-1.0)

    with pytest.raises(CoreException):
        _LoadAgg.model_validate(bad, from_attributes=True)

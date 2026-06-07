"""Tests for DomainEvent + AggregateRoot domain primitives."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from forze.domain.models import AggregateRoot, Document, DomainEvent

# ----------------------- #


class _Created(DomainEvent):
    aggregate_id: UUID


class _Renamed(DomainEvent):
    aggregate_id: UUID
    name: str


class _Order(Document, AggregateRoot):
    name: str = "init"

    def rename(self, name: str) -> _Order:
        new, _ = self.update({"name": name})
        return new.record_event(_Renamed(aggregate_id=new.id, name=name))


# ....................... #


class TestDomainEvent:
    def test_defaults(self) -> None:
        event = _Created(aggregate_id=UUID(int=1))
        assert isinstance(event.event_id, UUID)
        assert isinstance(event.occurred_at, datetime)

    def test_is_frozen(self) -> None:
        event = _Created(aggregate_id=UUID(int=1))
        with pytest.raises(ValidationError):
            event.aggregate_id = UUID(int=2)


class TestAggregateRoot:
    def test_record_collect_clear(self) -> None:
        order = _Order(name="a")
        assert order.has_pending_events is False

        order.record_event(_Created(aggregate_id=order.id))
        assert order.has_pending_events is True

        events = order.collect_events()
        assert len(events) == 1
        assert isinstance(events[0], _Created)
        assert order.has_pending_events is False
        assert order.collect_events() == ()

    def test_model_copy_has_independent_events(self) -> None:
        order = _Order(name="a")
        order.record_event(_Created(aggregate_id=order.id))

        copy = order.model_copy()
        assert copy._pending_events is not order._pending_events

        copy.record_event(_Renamed(aggregate_id=order.id, name="b"))
        # Recording on the copy must not leak into the original.
        assert len(order.collect_events()) == 1
        assert len(copy.collect_events()) == 2

    def test_events_carry_through_document_update(self) -> None:
        order = _Order(name="a")
        order.record_event(_Created(aggregate_id=order.id))

        updated = order.rename("b")  # update() (model_copy) + record on new instance

        assert [type(e).__name__ for e in updated.collect_events()] == [
            "_Created",
            "_Renamed",
        ]
        # The original instance is independent and still holds its event.
        assert order.has_pending_events is True

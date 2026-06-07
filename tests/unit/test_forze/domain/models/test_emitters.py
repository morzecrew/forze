"""Tests for declarative @event_emitter domain-event emission."""

from __future__ import annotations

from uuid import UUID

import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import AggregateRoot, Document, DomainEvent, event_emitter

# ----------------------- #


class StatusChanged(DomainEvent):
    aggregate_id: UUID
    status: str


class _Order(Document, AggregateRoot):
    status: str = "pending"
    name: str = "x"

    @event_emitter(fields={"status"})
    def _on_status(before, after, diff) -> DomainEvent | None:  # type: ignore[no-untyped-def]
        if after.status != before.status:
            return StatusChanged(aggregate_id=after.id, status=after.status)
        return None


class TestEventEmitter:
    def test_emits_on_matching_transition(self) -> None:
        order, _ = _Order(status="pending").update({"status": "confirmed"})

        events = order.collect_events()
        assert len(events) == 1
        assert isinstance(events[0], StatusChanged)
        assert events[0].status == "confirmed"

    def test_fields_filter_skips_unrelated_update(self) -> None:
        order, _ = _Order(status="pending").update({"name": "renamed"})
        assert order.collect_events() == ()

    def test_no_emit_when_value_unchanged(self) -> None:
        # status set to the same value -> empty diff -> emitter not run
        order, _ = _Order(status="pending").update({"status": "pending"})
        assert order.collect_events() == ()

    def test_events_independent_across_update_chain(self) -> None:
        o1, _ = _Order(status="pending").update({"status": "confirmed"})
        o2, _ = o1.update({"status": "shipped"})

        # o2 carries both transitions; o1 keeps only its own (no aliasing).
        assert [e.status for e in o2.collect_events()] == ["confirmed", "shipped"]  # type: ignore[attr-defined]
        assert [e.status for e in o1.collect_events()] == ["confirmed"]  # type: ignore[attr-defined]

    def test_emitter_on_plain_document_raises(self) -> None:
        with pytest.raises(CoreException) as ei:

            class _Bad(Document):
                @event_emitter
                def _e(before) -> DomainEvent | None:  # type: ignore[no-untyped-def]
                    return None

        assert ei.value.kind is ExceptionKind.CONFIGURATION

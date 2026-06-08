"""Tests for the ambient time-source seam behind utcnow()/uuid7()."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from forze.base.primitives import (
    FrozenTimeSource,
    SystemTimeSource,
    bind_time_source,
    current_time_source,
    utcnow,
    uuid7,
)

# ----------------------- #

_T0 = datetime(2020, 1, 1, 12, 0, tzinfo=UTC)


class TestDefaultBehavior:
    def test_default_source_is_system(self) -> None:
        assert isinstance(current_time_source(), SystemTimeSource)

    def test_utcnow_is_recent_and_aware(self) -> None:
        now = utcnow()
        assert now.tzinfo is not None
        assert abs((utcnow() - now).total_seconds()) < 1.0

    def test_uuid7_is_version_7(self) -> None:
        assert uuid7().version == 7

    def test_explicit_timestamp_still_deterministic(self) -> None:
        # The explicit-timestamp path is unchanged by the ambient seam.
        a = uuid7(timestamp_ms=1_700_000_000_000)
        b = uuid7(timestamp_ms=1_700_000_000_000)
        assert a.version == 7 and b.version == 7
        assert a != b  # same time, different random bits


class TestBoundSource:
    def test_frozen_controls_utcnow(self) -> None:
        with bind_time_source(FrozenTimeSource(instant=_T0)):
            assert utcnow() == _T0

    def test_frozen_uuids_are_deterministic_ordered_distinct(self) -> None:
        with bind_time_source(FrozenTimeSource(instant=_T0)):
            a, b = uuid7(), uuid7()

        assert a.version == 7
        assert a < b  # time-ordered (incrementing counter)
        assert a != b

    def test_bind_restores_previous_source_on_exit(self) -> None:
        before = utcnow()
        with bind_time_source(FrozenTimeSource(instant=_T0)):
            assert utcnow() == _T0
        assert abs((utcnow() - before).total_seconds()) < 1.0

    def test_nested_binds(self) -> None:
        t1 = datetime(2021, 6, 6, tzinfo=UTC)
        with bind_time_source(FrozenTimeSource(instant=_T0)):
            assert utcnow() == _T0
            with bind_time_source(FrozenTimeSource(instant=t1)):
                assert utcnow() == t1
            assert utcnow() == _T0


class TestDomainStampingIsControlled:
    def test_domain_event_and_document_stamp_from_bound_source(self) -> None:
        from forze.domain.models import DomainEvent, Document

        class _Event(DomainEvent):
            aggregate_id: UUID

        class _Doc(Document):
            name: str = "x"

        with bind_time_source(FrozenTimeSource(instant=_T0)):
            event = _Event(aggregate_id=UUID(int=1))
            doc = _Doc()

        # No domain code changes — stamping is transparently controlled by the seam.
        assert event.occurred_at == _T0
        assert event.event_id.version == 7
        assert doc.created_at == _T0
        assert doc.id.version == 7

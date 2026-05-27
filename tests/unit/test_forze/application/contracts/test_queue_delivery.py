"""Tests for queue delivery delay resolution."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from forze.application.contracts.queue import resolve_delivery_delay
from forze.base.exceptions import CoreException

UTC = timezone.utc


def test_resolve_none_for_immediate() -> None:
    assert resolve_delivery_delay(delay=None, not_before=None) is None


def test_resolve_relative_delay() -> None:
    assert resolve_delivery_delay(delay=timedelta(seconds=30), not_before=None) == timedelta(
        seconds=30
    )


def test_resolve_zero_delay_is_immediate() -> None:
    assert resolve_delivery_delay(delay=timedelta(0), not_before=None) is None


def test_resolve_negative_delay_raises() -> None:
    with pytest.raises(CoreException):
        resolve_delivery_delay(delay=timedelta(seconds=-1), not_before=None)


def test_resolve_mutually_exclusive_raises() -> None:
    with pytest.raises(CoreException):
        resolve_delivery_delay(
            delay=timedelta(seconds=1),
            not_before=datetime(2030, 1, 1, tzinfo=UTC),
        )


def test_resolve_naive_not_before_raises() -> None:
    with pytest.raises(CoreException):
        resolve_delivery_delay(delay=None, not_before=datetime(2030, 1, 1))


def test_resolve_not_before_in_future() -> None:
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    target = datetime(2026, 1, 1, 12, 0, 45, tzinfo=UTC)

    resolved = resolve_delivery_delay(
        delay=None,
        not_before=target,
        now=lambda: now,
    )

    assert resolved == timedelta(seconds=45)


def test_resolve_not_before_in_past_is_immediate() -> None:
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    target = datetime(2026, 1, 1, 11, 0, 0, tzinfo=UTC)

    assert (
        resolve_delivery_delay(delay=None, not_before=target, now=lambda: now) is None
    )

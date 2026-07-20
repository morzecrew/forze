"""Tests for the inbox contract + mock adapter via ctx.inbox."""

from __future__ import annotations

from datetime import timedelta

import pytest

from forze.application.contracts.inbox import InboxPort, InboxSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #


class TestInboxSpec:
    def test_rejects_non_positive_ttl(self) -> None:
        with pytest.raises(CoreException) as ei:
            InboxSpec(name="events", ttl=timedelta(0))

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_default_ttl(self) -> None:
        spec = InboxSpec(name="events")
        assert spec.ttl.total_seconds() > 0


class TestMockInboxViaCtx:
    async def test_mark_if_unseen_dedups(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        port = ctx.inbox(InboxSpec(name="events"))

        assert await port.mark_if_unseen("events", "m1") is True
        assert await port.mark_if_unseen("events", "m1") is False  # duplicate
        assert await port.mark_if_unseen("events", "m2") is True  # distinct id

    async def test_distinct_routes_are_independent(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        orders = ctx.inbox(InboxSpec(name="orders"))
        payments = ctx.inbox(InboxSpec(name="payments"))

        assert await orders.mark_if_unseen("orders", "m1") is True
        assert await payments.mark_if_unseen("payments", "m1") is True  # other route

    def test_port_resolves(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        port = ctx.inbox(InboxSpec(name="events"))
        assert isinstance(port, InboxPort)

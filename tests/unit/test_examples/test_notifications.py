"""Transactional-notifications recipe — staged event routes to a sender (mock, no Docker)."""

from __future__ import annotations

from forze.application.execution import DepsRegistry, ExecutionContext
from forze_mock import MockDepsModule

from examples.recipes.notifications.app import (
    RecordingSenders,
    deliver_notifications,
    stage_welcome,
)


async def test_notification_delivered() -> None:
    ctx = ExecutionContext(deps=DepsRegistry.from_modules(MockDepsModule()).freeze().resolve())
    senders = RecordingSenders()

    await stage_welcome(ctx, "ada@example.com")
    sent = await deliver_notifications(ctx, senders)

    assert sent == 1
    assert len(senders.emails) == 1
    assert senders.emails[0].to == "ada@example.com"

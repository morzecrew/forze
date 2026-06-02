"""Unit tests for forze_kits.integrations.notify."""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import IntegrationEvent
from forze.application.contracts.queue import QueueMessage
from forze_kits.integrations.notify import (
    EmailNotification,
    NotificationRouter,
    dispatch_notification,
    process_notification_message,
)
from forze_kits.integrations.notify.payloads import WebhookNotification
class _ProjectCreated(BaseModel):
    project_id: str


class _RecordingSenders:
    def __init__(self) -> None:
        self.emails: list[EmailNotification] = []
        self.webhooks: list[WebhookNotification] = []

    async def send_email(self, notification: EmailNotification) -> None:
        self.emails.append(notification)

    async def send_push(self, notification) -> None:  # noqa: ANN001
        raise NotImplementedError

    async def send_webhook(self, notification: WebhookNotification) -> None:
        self.webhooks.append(notification)


@pytest.mark.asyncio
async def test_router_maps_event_to_commands() -> None:
    router = NotificationRouter()
    router.register(
        "project.created",
        lambda event: [
            EmailNotification(
                to="ops@example.com",
                subject="New project",
                body=str(event.payload.project_id),
            )
        ],
    )
    event = IntegrationEvent(
        event_type="project.created",
        payload=_ProjectCreated(project_id="p-1"),
        event_id=uuid4(),
    )
    commands = router.resolve(event)
    assert len(commands) == 1
    assert commands[0].kind == "email"


@pytest.mark.asyncio
async def test_dispatch_notification_calls_sender() -> None:
    senders = _RecordingSenders()
    await dispatch_notification(
        EmailNotification(to="a@b.c", subject="hi", body="there"),
        senders,
    )
    assert len(senders.emails) == 1


@pytest.mark.asyncio
async def test_process_notification_message_uses_queue_type_and_key() -> None:
    event_id = uuid4()
    router = NotificationRouter()
    router.register(
        "project.created",
        lambda event: [
            EmailNotification(
                to="user@example.com",
                subject="Created",
                body=event.payload.project_id,
            )
        ],
    )
    senders = _RecordingSenders()
    message = QueueMessage(
        queue="jobs",
        id="1",
        payload=_ProjectCreated(project_id="abc"),
        type="project.created",
        key=str(event_id),
        enqueued_at=datetime.now(),
    )

    count = await process_notification_message(
        message,
        router=router,
        senders=senders,
    )

    assert count == 1
    assert senders.emails[0].body == "abc"


@pytest.mark.asyncio
async def test_process_notification_message_skips_unmapped() -> None:
    senders = _RecordingSenders()
    message = QueueMessage(
        queue="jobs",
        id="1",
        payload=_ProjectCreated(project_id="x"),
        type="unknown.event",
    )

    count = await process_notification_message(
        message,
        router=NotificationRouter(),
        senders=senders,
    )

    assert count == 0
    assert senders.emails == []

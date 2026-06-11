"""Unit tests for forze_kits.integrations.notify."""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import IntegrationEvent
from forze.application.contracts.queue import QueueMessage
from forze.base.exceptions import CoreException
from forze_kits.integrations.notify import (
    EmailNotification,
    NotificationRouter,
    dispatch_notification,
    integration_event_from_queue_message,
    process_notification_message,
)
from forze_kits.integrations.notify.payloads import (
    PushNotification,
    WebhookNotification,
)
class _ProjectCreated(BaseModel):
    project_id: str


class _RecordingSenders:
    def __init__(self) -> None:
        self.emails: list[EmailNotification] = []
        self.pushes: list[PushNotification] = []
        self.webhooks: list[WebhookNotification] = []

    async def send_email(self, notification: EmailNotification) -> None:
        self.emails.append(notification)

    async def send_push(self, notification: PushNotification) -> None:
        self.pushes.append(notification)

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
async def test_dispatch_notification_routes_push() -> None:
    senders = _RecordingSenders()
    await dispatch_notification(
        PushNotification(device_token="tok", title="t", body="b"),
        senders,
    )
    assert len(senders.pushes) == 1
    assert senders.emails == []


@pytest.mark.asyncio
async def test_dispatch_notification_routes_webhook() -> None:
    senders = _RecordingSenders()
    await dispatch_notification(
        WebhookNotification(url="https://example.com/hook"),
        senders,
    )
    assert len(senders.webhooks) == 1


@pytest.mark.asyncio
async def test_dispatch_notification_rejects_unsupported_command() -> None:
    senders = _RecordingSenders()

    class _Unknown(BaseModel):
        kind: str = "mystery"

    with pytest.raises(CoreException, match="unsupported notification command"):
        await dispatch_notification(_Unknown(), senders)  # type: ignore[arg-type]


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


def test_integration_event_id_deterministic_without_key() -> None:
    """Redelivery of the same keyless message must yield the SAME event_id."""

    def build() -> QueueMessage[_ProjectCreated]:
        return QueueMessage(
            queue="jobs",
            id="broker-msg-42",
            payload=_ProjectCreated(project_id="abc"),
            type="project.created",
        )

    first = integration_event_from_queue_message(build())
    second = integration_event_from_queue_message(build())

    assert first.event_id == second.event_id

    other = integration_event_from_queue_message(
        QueueMessage(
            queue="jobs",
            id="broker-msg-43",
            payload=_ProjectCreated(project_id="abc"),
            type="project.created",
        )
    )
    assert other.event_id != first.event_id


def test_integration_event_id_uses_valid_key() -> None:
    event_id = uuid4()
    message = QueueMessage(
        queue="jobs",
        id="broker-msg-1",
        payload=_ProjectCreated(project_id="abc"),
        type="project.created",
        key=str(event_id),
    )

    event = integration_event_from_queue_message(message)

    assert event.event_id == event_id


def test_integration_event_id_invalid_key_falls_back_to_message_id() -> None:
    def build() -> QueueMessage[_ProjectCreated]:
        return QueueMessage(
            queue="jobs",
            id="broker-msg-7",
            payload=_ProjectCreated(project_id="abc"),
            type="project.created",
            key="not-a-uuid",
        )

    first = integration_event_from_queue_message(build())
    second = integration_event_from_queue_message(build())

    assert first.event_id == second.event_id


def test_integration_event_id_no_key_no_id_raises() -> None:
    message = QueueMessage(
        queue="jobs",
        id="",
        payload=_ProjectCreated(project_id="abc"),
        type="project.created",
    )

    with pytest.raises(CoreException, match="deterministic event_id"):
        integration_event_from_queue_message(message)


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

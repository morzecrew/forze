"""Unit tests for forze_kits.integrations.notify."""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.envelope import HEADER_EVENT_ID
from forze.application.contracts.outbox import IntegrationEvent
from forze.application.contracts.queue import QueueMessage
from forze.base.exceptions import CoreException
from forze_kits.integrations.notify import (
    EmailNotification,
    NotificationRouter,
    RecordingNotificationSenders,
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
async def test_recording_senders_records_kinds_and_clears() -> None:
    from forze_kits.integrations.notify import NotificationSenders

    senders = RecordingNotificationSenders()
    assert isinstance(senders, NotificationSenders)

    email = EmailNotification(to="a@b.c", subject="s", body="b")
    push = PushNotification(device_token="tok", title="t", body="b")
    hook = WebhookNotification(url="https://example.com/hook")

    await senders.send_email(email)
    await senders.send_push(push)
    await senders.send_webhook(hook)

    assert senders.sent == [("email", email), ("push", push), ("webhook", hook)]
    assert senders.emails == [email]
    assert senders.pushes == [push]
    assert senders.webhooks == [hook]

    senders.clear()

    assert senders.sent == []
    assert senders.emails == []
    assert senders.pushes == []
    assert senders.webhooks == []


@pytest.mark.asyncio
async def test_dispatch_notification_calls_sender() -> None:
    senders = RecordingNotificationSenders()
    await dispatch_notification(
        EmailNotification(to="a@b.c", subject="hi", body="there"),
        senders,
    )
    assert len(senders.emails) == 1


@pytest.mark.asyncio
async def test_dispatch_notification_routes_push() -> None:
    senders = RecordingNotificationSenders()
    await dispatch_notification(
        PushNotification(device_token="tok", title="t", body="b"),
        senders,
    )
    assert len(senders.pushes) == 1
    assert senders.emails == []


@pytest.mark.asyncio
async def test_dispatch_notification_routes_webhook() -> None:
    senders = RecordingNotificationSenders()
    await dispatch_notification(
        WebhookNotification(url="https://example.com/hook"),
        senders,
    )
    assert len(senders.webhooks) == 1


@pytest.mark.asyncio
async def test_dispatch_notification_rejects_unsupported_command() -> None:
    senders = RecordingNotificationSenders()

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
    senders = RecordingNotificationSenders()
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


# ....................... #
# event_id priority: forze_event_id header > UUID key > broker identity.


def test_integration_event_id_header_beats_uuid_shaped_key() -> None:
    """A UUID-shaped ordering key in ``key`` must not masquerade as the event id."""

    event_id = uuid4()
    ordering_key = uuid4()  # aggregate id used as ordering key — UUID-shaped!
    message = QueueMessage(
        queue="jobs",
        id="broker-msg-8",
        payload=_ProjectCreated(project_id="abc"),
        type="project.created",
        key=str(ordering_key),
        headers={HEADER_EVENT_ID: str(event_id)},
    )

    event = integration_event_from_queue_message(message)

    assert event.event_id == event_id
    assert event.event_id != ordering_key


def test_integration_event_ids_distinct_for_same_ordering_key() -> None:
    """Two different events sharing an ordering key keep distinct event ids."""

    ordering_key = str(uuid4())

    def build(event_id) -> QueueMessage[_ProjectCreated]:
        return QueueMessage(
            queue="jobs",
            id=f"broker-{event_id}",
            payload=_ProjectCreated(project_id="abc"),
            type="project.created",
            key=ordering_key,
            headers={HEADER_EVENT_ID: str(event_id)},
        )

    first_id, second_id = uuid4(), uuid4()
    first = integration_event_from_queue_message(build(first_id))
    second = integration_event_from_queue_message(build(second_id))

    assert first.event_id == first_id
    assert second.event_id == second_id
    assert first.event_id != second.event_id


def test_integration_event_id_malformed_header_falls_back_to_key() -> None:
    event_id = uuid4()
    message = QueueMessage(
        queue="jobs",
        id="broker-msg-9",
        payload=_ProjectCreated(project_id="abc"),
        type="project.created",
        key=str(event_id),
        headers={HEADER_EVENT_ID: "not-a-uuid"},
    )

    event = integration_event_from_queue_message(message)

    assert event.event_id == event_id


def test_integration_event_id_header_with_non_uuid_key_uses_header() -> None:
    event_id = uuid4()
    message = QueueMessage(
        queue="jobs",
        id="broker-msg-10",
        payload=_ProjectCreated(project_id="abc"),
        type="project.created",
        key="order-1",  # non-UUID ordering key
        headers={HEADER_EVENT_ID: str(event_id)},
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
    senders = RecordingNotificationSenders()
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

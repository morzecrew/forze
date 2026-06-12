"""Application-provided notification backends (kits-local, not core ports)."""

from __future__ import annotations

from typing import Protocol, final, runtime_checkable

from .payloads import (
    EmailNotification,
    NotificationCommand,
    PushNotification,
    WebhookNotification,
)
from collections.abc import Awaitable

# ----------------------- #


@runtime_checkable
class NotificationSenders(Protocol):
    """Send notifications using vendor SDKs wired by the application."""

    def send_email(self, _notification: EmailNotification) -> Awaitable[None]:
        """Deliver an email notification."""
        ...  # pragma: no cover

    def send_push(self, _notification: PushNotification) -> Awaitable[None]:
        """Deliver a push notification."""
        ...  # pragma: no cover

    def send_webhook(self, _notification: WebhookNotification) -> Awaitable[None]:
        """Deliver a webhook notification."""
        ...  # pragma: no cover


# ....................... #


@final
class RecordingNotificationSenders:
    """Test double: a :class:`NotificationSenders` that records instead of sending.

    Every delivery is appended to :attr:`sent` as a ``(kind, payload)`` tuple in
    call order, and to the per-channel convenience lists (:attr:`emails`,
    :attr:`pushes`, :attr:`webhooks`). Use it in tests and examples instead of
    hand-rolling a recorder.
    """

    sent: list[tuple[str, NotificationCommand]]
    """All recorded deliveries as ``(kind, payload)`` tuples, in call order."""

    emails: list[EmailNotification]
    """Recorded email notifications."""

    pushes: list[PushNotification]
    """Recorded push notifications."""

    webhooks: list[WebhookNotification]
    """Recorded webhook notifications."""

    # ....................... #

    def __init__(self) -> None:
        self.sent = []
        self.emails = []
        self.pushes = []
        self.webhooks = []

    # ....................... #

    async def send_email(self, notification: EmailNotification) -> None:
        self.sent.append((notification.kind, notification))
        self.emails.append(notification)

    async def send_push(self, notification: PushNotification) -> None:
        self.sent.append((notification.kind, notification))
        self.pushes.append(notification)

    async def send_webhook(self, notification: WebhookNotification) -> None:
        self.sent.append((notification.kind, notification))
        self.webhooks.append(notification)

    # ....................... #

    def clear(self) -> None:
        """Forget every recorded delivery."""

        self.sent.clear()
        self.emails.clear()
        self.pushes.clear()
        self.webhooks.clear()

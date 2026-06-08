"""Application-provided notification backends (kits-local, not core ports)."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .payloads import EmailNotification, PushNotification, WebhookNotification
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

"""Application-provided notification backends (kits-local, not core ports)."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .payloads import EmailNotification, PushNotification, WebhookNotification

# ----------------------- #


@runtime_checkable
class NotificationSenders(Protocol):
    """Send notifications using vendor SDKs wired by the application."""

    async def send_email(self, _notification: EmailNotification) -> None:
        """Deliver an email notification."""
        ...  # pragma: no cover

    async def send_push(self, _notification: PushNotification) -> None:
        """Deliver a push notification."""
        ...  # pragma: no cover

    async def send_webhook(self, _notification: WebhookNotification) -> None:
        """Deliver a webhook notification."""
        ...  # pragma: no cover

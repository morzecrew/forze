"""Dispatch a single notification command to app senders."""

from __future__ import annotations

from typing import TYPE_CHECKING

from forze.base.exceptions import exc

from .payloads import (
    EmailNotification,
    NotificationCommand,
    PushNotification,
    WebhookNotification,
)
from .senders import NotificationSenders

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


async def dispatch_notification(
    ctx: ExecutionContext,  # noqa: ARG001 — reserved for tracing/tenant-aware senders
    command: NotificationCommand,
    senders: NotificationSenders,
) -> None:
    """Route *command* to the matching :class:`NotificationSenders` method."""

    if isinstance(command, EmailNotification):
        await senders.send_email(command)
        return

    if isinstance(command, PushNotification):
        await senders.send_push(command)
        return

    if isinstance(
        command, WebhookNotification
    ):  # pyright: ignore[reportUnnecessaryIsInstance]
        await senders.send_webhook(command)
        return

    raise exc.precondition(f"unsupported notification command: {command!r}")

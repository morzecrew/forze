"""Notification routing and dispatch on top of outbox + queue (no core NotificationPort)."""

from .consumer import (
    integration_event_from_queue_message,
    notification_consumer_lifecycle_step,
    notification_queue_consumer_handler,
    process_notification_message,
)
from .dispatch import dispatch_notification
from .payloads import (
    EmailNotification,
    NotificationCommand,
    PushNotification,
    WebhookNotification,
)
from .routing import NotificationRouter
from .senders import NotificationSenders, RecordingNotificationSenders

__all__ = [
    "EmailNotification",
    "NotificationCommand",
    "NotificationRouter",
    "NotificationSenders",
    "PushNotification",
    "RecordingNotificationSenders",
    "WebhookNotification",
    "dispatch_notification",
    "integration_event_from_queue_message",
    "notification_consumer_lifecycle_step",
    "notification_queue_consumer_handler",
    "process_notification_message",
]

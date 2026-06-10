"""Shared building blocks for message-queue adapters (SQS, RabbitMQ, ...)."""

from .codec import BaseQueueMessage, QueueMessageCodec, RawQueueMessage
from .naming import ScopedQueueNamingMixin

# ----------------------- #

__all__ = [
    "BaseQueueMessage",
    "QueueMessageCodec",
    "RawQueueMessage",
    "ScopedQueueNamingMixin",
]

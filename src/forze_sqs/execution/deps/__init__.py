"""SQS dependency keys and module."""

from .configs import SQSQueueConfig
from .keys import SQSClientDepKey
from .module import SQSDepsModule

# ----------------------- #

__all__ = ["SQSDepsModule", "SQSClientDepKey", "SQSQueueConfig"]

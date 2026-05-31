"""SQS dependency factories."""

from .read import ConfigurableSQSQueueRead
from .write import ConfigurableSQSQueueWrite

# ----------------------- #

__all__ = [
    "ConfigurableSQSQueueRead",
    "ConfigurableSQSQueueWrite",
]

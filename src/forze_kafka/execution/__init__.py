"""Kafka execution wiring for the application kernel."""

from .deps import (
    KafkaClientDepKey,
    KafkaCommitStreamGroupConfig,
    KafkaDepsModule,
    KafkaStreamConfig,
)
from .lifecycle import kafka_lifecycle_step, routed_kafka_lifecycle_step

# ----------------------- #

__all__ = [
    "KafkaDepsModule",
    "KafkaClientDepKey",
    "kafka_lifecycle_step",
    "routed_kafka_lifecycle_step",
    "KafkaStreamConfig",
    "KafkaCommitStreamGroupConfig",
]

"""Kafka offset-log integration for Forze."""

from ._compat import require_kafka

require_kafka()

# ....................... #

from .execution import (
    KafkaClientDepKey,
    KafkaCommitStreamGroupConfig,
    KafkaDepsModule,
    KafkaStreamConfig,
    kafka_lifecycle_step,
    routed_kafka_lifecycle_step,
)
from .kernel.client import (
    KafkaClient,
    KafkaClientPort,
    KafkaConfig,
    RoutedKafkaClient,
)
from .kernel.relation import (
    NamedResourceSpec,
    coerce_named_resource_spec,
    resolve_kafka_topic,
)

# ----------------------- #

__all__ = [
    "KafkaClient",
    "KafkaClientPort",
    "KafkaConfig",
    "RoutedKafkaClient",
    "KafkaClientDepKey",
    "KafkaDepsModule",
    "kafka_lifecycle_step",
    "routed_kafka_lifecycle_step",
    "KafkaStreamConfig",
    "KafkaCommitStreamGroupConfig",
    "NamedResourceSpec",
    "coerce_named_resource_spec",
    "resolve_kafka_topic",
]

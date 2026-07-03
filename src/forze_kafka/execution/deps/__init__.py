"""Kafka dependency keys, configs, factories, and module."""

from .configs import KafkaCommitStreamGroupConfig, KafkaStreamConfig
from .factories import (
    ConfigurableKafkaAdmin,
    ConfigurableKafkaConsume,
    ConfigurableKafkaProduce,
)
from .keys import KafkaClientDepKey
from .module import KafkaDepsModule

# ----------------------- #

__all__ = [
    "KafkaDepsModule",
    "KafkaClientDepKey",
    "KafkaStreamConfig",
    "KafkaCommitStreamGroupConfig",
    "ConfigurableKafkaProduce",
    "ConfigurableKafkaConsume",
    "ConfigurableKafkaAdmin",
]

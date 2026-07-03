"""Dependency keys for Kafka-related services."""

from forze.application.contracts.deps import DepKey

from ...kernel.client import KafkaClientPort

# ----------------------- #

KafkaClientDepKey: DepKey[KafkaClientPort] = DepKey("kafka_client")
"""Key used to register a Kafka client (single-bootstrap or routed) in the deps container."""

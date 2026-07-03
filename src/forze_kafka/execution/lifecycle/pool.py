"""Kafka client lifecycle hooks and step factories."""

from typing import Any, cast, final

import attrs

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.context import ExecutionContext
from forze.application.execution.lifecycle.builtin import (
    ClientShutdownHook,
    routed_client_lifecycle_step,
)

from ...kernel.client import KafkaClient, KafkaConfig, RoutedKafkaClient
from ..deps import KafkaClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class KafkaStartupHook(LifecycleHook):
    """Startup hook that initializes the Kafka client from the deps container."""

    bootstrap_servers: str = attrs.field()
    config: KafkaConfig = attrs.field(factory=KafkaConfig, repr=False)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        client = cast(KafkaClient, ctx.deps.provide(KafkaClientDepKey))
        await client.initialize(self.bootstrap_servers, config=self.config)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class KafkaShutdownHook(ClientShutdownHook):
    """Shutdown hook that closes the Kafka producer/admin/consumers."""

    dep_key: DepKey[Any] = attrs.field(default=KafkaClientDepKey, init=False)


# ....................... #


def kafka_lifecycle_step(
    name: str = "kafka_lifecycle",
    *,
    bootstrap_servers: str,
    config: KafkaConfig = KafkaConfig(),
) -> LifecycleStep:
    """Build a lifecycle step for Kafka client init and shutdown."""

    startup_hook = KafkaStartupHook(bootstrap_servers=bootstrap_servers, config=config)
    shutdown_hook = KafkaShutdownHook()

    return LifecycleStep(id=name, startup=startup_hook, shutdown=shutdown_hook)


# ....................... #


def routed_kafka_lifecycle_step(
    name: str = "routed_kafka_lifecycle",
    *,
    client: RoutedKafkaClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedKafkaClient` registered as :data:`KafkaClientDepKey`.

    Do not combine with :func:`kafka_lifecycle_step` on the same instance.
    """

    return routed_client_lifecycle_step(name, client=client)

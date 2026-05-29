"""RabbitMQ dependency module for the application kernel."""

from enum import StrEnum
from typing import Mapping, final

import attrs

from forze.application.contracts.queue import QueueCommandDepKey, QueueQueryDepKey
from forze.application.contracts.tenancy import warn_dynamic_relation_with_tenant_aware
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import RabbitMQClientPort
from .configs import RabbitMQQueueConfig
from .deps import ConfigurableRabbitMQQueueRead, ConfigurableRabbitMQQueueWrite
from .keys import RabbitMQClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RabbitMQDepsModule[K: str | StrEnum](DepsModule[K]):
    """Dependency module that registers RabbitMQ client and queue ports."""

    client: RabbitMQClientPort
    """Pre-constructed RabbitMQ client (single-DSN or routed, not connected until lifecycle)."""

    queue_readers: Mapping[K, RabbitMQQueueConfig] | None = attrs.field(default=None)
    """Mapping from queue names to their RabbitMQ-specific configurations."""

    queue_writers: Mapping[K, RabbitMQQueueConfig] | None = attrs.field(default=None)
    """Mapping from queue names to their RabbitMQ-specific configurations."""

    def __attrs_post_init__(self) -> None:
        for mapping, kind in (
            (self.queue_readers, "queue_reader"),
            (self.queue_writers, "queue_writer"),
        ):
            if not mapping:
                continue

            for name, cfg in mapping.items():
                warn_dynamic_relation_with_tenant_aware(
                    integration="RabbitMQ",
                    route_name=str(name),
                    kind=kind,
                    tenant_aware=cfg.tenant_aware,
                    named_fields=[("namespace", cfg.namespace)],
                )

    # ....................... #

    def __call__(self) -> Deps[K]:
        """Build a dependency container with RabbitMQ-backed ports."""

        plain_deps = Deps[K].plain({RabbitMQClientDepKey: self.client})
        queue_reader_deps = Deps[K]()
        queue_writer_deps = Deps[K]()

        if self.queue_readers:
            queue_reader_deps = queue_reader_deps.merge(
                Deps[K].routed(
                    {
                        QueueQueryDepKey: {
                            name: ConfigurableRabbitMQQueueRead(config=config)
                            for name, config in self.queue_readers.items()
                        }
                    }
                )
            )

        if self.queue_writers:
            queue_writer_deps = queue_writer_deps.merge(
                Deps[K].routed(
                    {
                        QueueCommandDepKey: {
                            name: ConfigurableRabbitMQQueueWrite(config=config)
                            for name, config in self.queue_writers.items()
                        }
                    }
                )
            )

        return plain_deps.merge(queue_reader_deps, queue_writer_deps)

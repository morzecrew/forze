"""SQS dependency module for the application kernel."""

from typing import Mapping, final

import attrs

from forze.application.contracts.queue import QueueCommandDepKey, QueueQueryDepKey
from forze.application.contracts.tenancy import warn_dynamic_relation_with_tenant_aware
from forze.application.execution import Deps, DepsModule
from forze.base.primitives import StrKey

from ...kernel.client import SQSClientPort
from .configs import SQSQueueConfig
from .factories import ConfigurableSQSQueueRead, ConfigurableSQSQueueWrite
from .keys import SQSClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SQSDepsModule(DepsModule):
    """Dependency module that registers SQS client and queue ports."""

    client: SQSClientPort
    """Pre-constructed SQS client (single endpoint or routed, session not initialized until lifecycle)."""

    queue_readers: Mapping[StrKey, SQSQueueConfig] | None = attrs.field(default=None)
    """Mapping from queue names to their SQS-specific configurations."""

    queue_writers: Mapping[StrKey, SQSQueueConfig] | None = attrs.field(default=None)
    """Mapping from queue names to their SQS-specific configurations."""

    def __attrs_post_init__(self) -> None:
        for mapping, kind in (
            (self.queue_readers, "queue_reader"),
            (self.queue_writers, "queue_writer"),
        ):
            if not mapping:
                continue

            for name, cfg in mapping.items():
                warn_dynamic_relation_with_tenant_aware(
                    integration="SQS",
                    route_name=str(name),
                    kind=kind,
                    tenant_aware=cfg.tenant_aware,
                    named_fields=[("namespace", cfg.namespace)],
                )

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with SQS-backed ports."""

        plain_deps = Deps.plain({SQSClientDepKey: self.client})
        queue_reader_deps = Deps()
        queue_writer_deps = Deps()

        if self.queue_readers:
            queue_reader_deps = queue_reader_deps.merge(
                Deps.routed(
                    {
                        QueueQueryDepKey: {
                            name: ConfigurableSQSQueueRead(config=config)
                            for name, config in self.queue_readers.items()
                        }
                    }
                )
            )

        if self.queue_writers:
            queue_writer_deps = queue_writer_deps.merge(
                Deps.routed(
                    {
                        QueueCommandDepKey: {
                            name: ConfigurableSQSQueueWrite(config=config)
                            for name, config in self.queue_writers.items()
                        }
                    }
                )
            )

        return plain_deps.merge(queue_reader_deps, queue_writer_deps)

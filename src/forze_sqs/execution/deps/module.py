"""SQS dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.queue import QueueCommandDepKey, QueueQueryDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import SQSClient
from .configs import SQSQueueConfig
from .deps import ConfigurableSQSQueueRead, ConfigurableSQSQueueWrite
from .keys import SQSClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SQSDepsModule(DepsModule):
    """Dependency module that registers SQS client and queue ports."""

    client: SQSClient
    """Pre-constructed SQS client (session not yet initialized)."""

    queue_readers: dict[str, SQSQueueConfig] = attrs.field(factory=dict)
    """Mapping from queue names to their SQS-specific configurations."""

    queue_writers: dict[str, SQSQueueConfig] = attrs.field(factory=dict)
    """Mapping from queue names to their SQS-specific configurations."""

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

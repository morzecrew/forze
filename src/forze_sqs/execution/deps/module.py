"""SQS dependency module for the application kernel."""

from enum import StrEnum
from typing import Mapping, final

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
class SQSDepsModule[K: str | StrEnum](DepsModule[K]):
    """Dependency module that registers SQS client and queue ports."""

    client: SQSClient
    """Pre-constructed SQS client (session not yet initialized)."""

    queue_readers: Mapping[K, SQSQueueConfig] | None = None
    """Mapping from queue names to their SQS-specific configurations."""

    queue_writers: Mapping[K, SQSQueueConfig] | None = None
    """Mapping from queue names to their SQS-specific configurations."""

    # ....................... #

    def __call__(self) -> Deps[K]:
        """Build a dependency container with SQS-backed ports."""

        plain_deps = Deps[K].plain({SQSClientDepKey: self.client})
        queue_reader_deps = Deps[K]()
        queue_writer_deps = Deps[K]()

        if self.queue_readers:
            queue_reader_deps = queue_reader_deps.merge(
                Deps[K].routed(
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
                Deps[K].routed(
                    {
                        QueueCommandDepKey: {
                            name: ConfigurableSQSQueueWrite(config=config)
                            for name, config in self.queue_writers.items()
                        }
                    }
                )
            )

        return plain_deps.merge(queue_reader_deps, queue_writer_deps)

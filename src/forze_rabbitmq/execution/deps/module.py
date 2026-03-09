"""RabbitMQ dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.queue import QueueReadDepKey, QueueWriteDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import RabbitMQClient
from .deps import rabbitmq_queue
from .keys import RabbitMQClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RabbitMQDepsModule(DepsModule):
    """Dependency module that registers RabbitMQ client and queue ports."""

    client: RabbitMQClient
    """Pre-constructed RabbitMQ client (connection not yet initialized)."""

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with RabbitMQ-backed ports."""
        return Deps(
            {
                RabbitMQClientDepKey: self.client,
                QueueReadDepKey: rabbitmq_queue,
                QueueWriteDepKey: rabbitmq_queue,
            }
        )

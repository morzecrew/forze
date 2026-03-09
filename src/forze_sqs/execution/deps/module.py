"""SQS dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.queue import QueueReadDepKey, QueueWriteDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import SQSClient
from .deps import sqs_queue
from .keys import SQSClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SQSDepsModule(DepsModule):
    """Dependency module that registers SQS client and queue ports."""

    client: SQSClient
    """Pre-constructed SQS client (session not yet initialized)."""

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with SQS-backed ports."""
        return Deps(
            {
                SQSClientDepKey: self.client,
                QueueReadDepKey: sqs_queue,
                QueueWriteDepKey: sqs_queue,
            }
        )

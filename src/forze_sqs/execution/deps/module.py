"""SQS dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.queue import QueueCommandDepKey, QueueQueryDepKey
from forze.application.contracts.tenancy import warn_integration_routes
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import merge_deps, routed_from_mapping
from forze.base.primitives import MappingConverter, StrKeyMapping

from ...kernel.client import SQSClientPort
from ._warnings import SQS_QUEUE_READER_WARNING, SQS_QUEUE_WRITER_WARNING
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

    queue_readers: StrKeyMapping[SQSQueueConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from queue names to their SQS-specific configurations."""

    queue_writers: StrKeyMapping[SQSQueueConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from queue names to their SQS-specific configurations."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        warn_integration_routes(
            integration="SQS",
            routes=self.queue_readers,
            warning=SQS_QUEUE_READER_WARNING,
        )
        warn_integration_routes(
            integration="SQS",
            routes=self.queue_writers,
            warning=SQS_QUEUE_WRITER_WARNING,
        )

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with SQS-backed ports."""

        return merge_deps(
            routed_from_mapping(
                self.queue_readers,
                bindings=[(QueueQueryDepKey, ConfigurableSQSQueueRead)],
            ),
            routed_from_mapping(
                self.queue_writers,
                bindings=[(QueueCommandDepKey, ConfigurableSQSQueueWrite)],
            ),
            plain={SQSClientDepKey: self.client},
        )

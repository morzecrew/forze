"""SQS queue execution configs."""

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc
from forze_sqs.kernel.client import SQS_DEFAULT_MAX_BATCH_PAYLOAD_BYTES

# ----------------------- #

_SQS_MAX_MESSAGE_SIZE_CEILING = 1024 * 1024
"""AWS upper bound for a queue's ``MaximumMessageSize`` (1 MiB, raised from 256 KiB in 2025)."""


@attrs.define(slots=True, kw_only=True, frozen=True)
class SQSQueueConfig(TenantAwareIntegrationConfig):
    """Configuration for an SQS queue."""

    namespace: NamedResourceSpec = attrs.field(
        default="",
        converter=coerce_named_resource_spec,
    )
    """Base namespace for queues."""

    max_batch_payload_bytes: int = attrs.field(
        default=SQS_DEFAULT_MAX_BATCH_PAYLOAD_BYTES,
    )
    """Total ``send_message_batch`` request-payload cap for this queue (default 256 KiB).

    Match it to the queue's ``MaximumMessageSize`` attribute: keep the 256 KiB default for
    SQS-compatible backends (YMQ, ElasticMQ, LocalStack) and AWS queues on the legacy limit,
    or raise it up to 1 MiB for AWS queues configured with the higher 2025 ceiling so large
    batches are not split more than necessary."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not (1024 <= self.max_batch_payload_bytes <= _SQS_MAX_MESSAGE_SIZE_CEILING):
            raise exc.configuration(
                "SQSQueueConfig.max_batch_payload_bytes must be between 1 KiB and 1 MiB "
                f"(got {self.max_batch_payload_bytes})."
            )

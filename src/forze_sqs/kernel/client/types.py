from typing import final

import attrs

from forze.application.integrations.queue import BaseQueueMessage

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SQSQueueMessage(BaseQueueMessage):
    """Raw SQS queue message envelope.

    ``id`` carries the broker-assigned ``MessageId`` — stable across
    redeliveries and correlatable with the identifier returned by enqueue.
    The per-delivery ``ReceiptHandle`` (required for ack/nack/visibility
    operations) is exposed separately as :attr:`receipt_handle`.
    """

    receipt_handle: str = ""
    """Per-delivery SQS receipt handle; changes on every redelivery."""

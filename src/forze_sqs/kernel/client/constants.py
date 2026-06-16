"""Shared SQS client constants (a leaf module to avoid client/port import cycles)."""

from typing import Final

# ----------------------- #

SQS_DEFAULT_MAX_BATCH_PAYLOAD_BYTES: Final[int] = 256 * 1024
"""Safe **default** for the total payload of a ``send_message_batch`` request (256 KiB) —
the sum of every entry's encoded body and message attributes.

This is the *floor*, not a universal hard limit: it is a per-queue ``MaximumMessageSize``
attribute (1 KiB–256 KiB historically), and AWS raised the ceiling to **1 MiB** in Aug 2025,
so a queue can be configured higher. SQS-compatible backends (YMQ, ElasticMQ, LocalStack)
generally still cap at 256 KiB. 256 KiB is therefore the safe default that works everywhere;
raise ``SQSQueueConfig.max_batch_payload_bytes`` per route to match a queue's actual limit."""

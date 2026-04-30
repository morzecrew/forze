"""Dependency keys for SQS-related services."""

from forze.application.contracts.base import DepKey

from ...kernel.platform import SQSClientPort

# ----------------------- #

SQSClientDepKey: DepKey[SQSClientPort] = DepKey("sqs_client")
"""Key used to register an SQS client (single endpoint or routed) in the deps container."""

"""Dependency keys for SQS-related services."""

from forze.application.contracts.deps import DepKey

from ...kernel.client import SQSClientPort

# ----------------------- #

SQSClientDepKey: DepKey[SQSClientPort] = DepKey("sqs_client")
"""Key used to register an SQS client (single endpoint or routed) in the deps container."""

"""Dependency keys for SQS-related services."""

from forze.application.contracts.base import DepKey

from ...kernel.platform import SQSClient

# ----------------------- #

SQSClientDepKey: DepKey[SQSClient] = DepKey("sqs_client")
"""Key used to register the :class:`SQSClient` in the deps container."""

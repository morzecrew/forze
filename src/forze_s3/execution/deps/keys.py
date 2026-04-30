"""Dependency keys for S3-related services."""

from forze.application.contracts.base import DepKey

from ...kernel.platform import S3ClientPort

# ----------------------- #

S3ClientDepKey: DepKey[S3ClientPort] = DepKey("s3_client")
"""Key used to register an S3 client (single endpoint or routed) in the deps container."""

"""Dependency keys for AWS KMS services."""

from forze.application.contracts.deps import DepKey

from ...kernel.client import AwsKmsClientPort

# ----------------------- #

AwsKmsClientDepKey: DepKey[AwsKmsClientPort] = DepKey("awskms_client")
"""Key used to register an AWS KMS client in the deps container."""

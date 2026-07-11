"""Dependency keys for GCP KMS services."""

from forze.application.contracts.deps import DepKey

from ...kernel.client import GcpKmsClientPort

# ----------------------- #

GcpKmsClientDepKey: DepKey[GcpKmsClientPort] = DepKey("gcpkms_client")
"""Key used to register a GCP KMS client in the deps container."""

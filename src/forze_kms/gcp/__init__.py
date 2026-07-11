"""GCP KMS integration for Forze envelope encryption (BYOK key management)."""

from ._compat import require_kms_gcp

require_kms_gcp()

# ....................... #

from .adapters import GcpKmsKeyManagement
from .execution import GcpKmsClientDepKey, GcpKmsDepsModule, gcpkms_lifecycle_step
from .kernel.client import GcpKmsClient, GcpKmsClientPort, GcpKmsConfig

# ----------------------- #

__all__ = [
    "GcpKmsClient",
    "GcpKmsClientPort",
    "GcpKmsConfig",
    "GcpKmsClientDepKey",
    "GcpKmsDepsModule",
    "GcpKmsKeyManagement",
    "gcpkms_lifecycle_step",
]

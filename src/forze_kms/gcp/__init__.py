"""GCP KMS integration for Forze envelope encryption (BYOK key management)."""

from ._compat import require_kms_gcp

require_kms_gcp()

# ....................... #

from .adapters import GcpKmsKeyManagement, GcpKmsTenantProvisioner
from .execution import GcpKmsClientDepKey, GcpKmsDepsModule, gcpkms_lifecycle_step
from .kernel.client import GcpKmsClient, GcpKmsClientPort, GcpKmsConfig
from .settings import GcpKmsSettings

# ----------------------- #

__all__ = [
    "GcpKmsClient",
    "GcpKmsClientDepKey",
    "GcpKmsClientPort",
    "GcpKmsConfig",
    "GcpKmsSettings",
    "GcpKmsDepsModule",
    "GcpKmsKeyManagement",
    "GcpKmsTenantProvisioner",
    "gcpkms_lifecycle_step",
]

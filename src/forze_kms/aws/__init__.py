"""AWS KMS integration for Forze envelope encryption (BYOK key management)."""

from forze_kms.aws._compat import require_kms_aws

require_kms_aws()

# ....................... #

from .adapters import AwsKmsKeyManagement, AwsKmsTenantProvisioner
from .execution import AwsKmsClientDepKey, AwsKmsDepsModule, awskms_lifecycle_step
from .kernel.client import AwsKmsClient, AwsKmsClientPort, AwsKmsConfig

# ----------------------- #

__all__ = [
    "AwsKmsClient",
    "AwsKmsClientPort",
    "AwsKmsConfig",
    "AwsKmsClientDepKey",
    "AwsKmsDepsModule",
    "AwsKmsKeyManagement",
    "AwsKmsTenantProvisioner",
    "awskms_lifecycle_step",
]

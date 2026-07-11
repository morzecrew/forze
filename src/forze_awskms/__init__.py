"""AWS KMS integration for Forze envelope encryption (BYOK key management)."""

from forze_awskms._compat import require_awskms

require_awskms()

# ....................... #

from .adapters import AwsKmsKeyManagement
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
    "awskms_lifecycle_step",
]

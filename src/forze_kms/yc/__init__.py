"""Yandex Cloud KMS integration for Forze envelope encryption (BYOK key management)."""

from ._compat import require_kms_yc

require_kms_yc()

# ....................... #

from .adapters import YcKmsKeyManagement
from .execution import YcKmsClientDepKey, YcKmsDepsModule, yckms_lifecycle_step
from .kernel.client import YcKmsClient, YcKmsClientPort, YcKmsConfig

# ----------------------- #

__all__ = [
    "YcKmsClient",
    "YcKmsClientPort",
    "YcKmsConfig",
    "YcKmsClientDepKey",
    "YcKmsDepsModule",
    "YcKmsKeyManagement",
    "yckms_lifecycle_step",
]

"""AWS KMS execution wiring for the application kernel."""

from .deps import AwsKmsClientDepKey, AwsKmsDepsModule
from .lifecycle import awskms_lifecycle_step

# ----------------------- #

__all__ = [
    "AwsKmsClientDepKey",
    "AwsKmsDepsModule",
    "awskms_lifecycle_step",
]

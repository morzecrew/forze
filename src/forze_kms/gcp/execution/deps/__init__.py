"""GCP KMS dependency keys and module."""

from .keys import GcpKmsClientDepKey
from .module import GcpKmsDepsModule

# ----------------------- #

__all__ = [
    "GcpKmsClientDepKey",
    "GcpKmsDepsModule",
]

"""S3 integration for Forze.

Supports any S3-compatible object storage service such as
Yandex Cloud Object Storage, MinIO, Amazon S3, etc.
"""

from ._compat import require_s3

require_s3()

# ....................... #

from .execution import S3ClientDepKey, S3DepsModule, s3_lifecycle_step
from .kernel.platform import S3Client, S3Config

# ----------------------- #

__all__ = [
    "S3DepsModule",
    "S3Client",
    "S3Config",
    "S3ClientDepKey",
    "s3_lifecycle_step",
]

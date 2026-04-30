"""S3 integration for Forze.

Supports any S3-compatible object storage service such as
Yandex Cloud Object Storage, MinIO, Amazon S3, etc.
"""

from ._compat import require_s3

require_s3()

# ....................... #

from .execution import (
    S3ClientDepKey,
    S3DepsModule,
    S3StorageConfig,
    routed_s3_lifecycle_step,
    s3_lifecycle_step,
)
from .kernel.platform import (
    RoutedS3Client,
    S3Client,
    S3ClientPort,
    S3Config,
    S3RoutingCredentials,
)

# ----------------------- #

__all__ = [
    "S3DepsModule",
    "S3Client",
    "S3ClientPort",
    "S3Config",
    "RoutedS3Client",
    "S3RoutingCredentials",
    "S3ClientDepKey",
    "s3_lifecycle_step",
    "routed_s3_lifecycle_step",
    "S3StorageConfig",
]

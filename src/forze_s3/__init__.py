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
    S3ServerSideEncryption,
    S3StorageConfig,
    s3_lifecycle_step,
)
from .kernel.client import (
    RoutedS3Client,
    S3Client,
    S3ClientPort,
    S3Config,
    S3RoutingCredentials,
)
from .kernel.relation import (
    NamedResourceSpec,
    coerce_named_resource_spec,
    is_static_named_resource,
    resolve_s3_bucket,
)
from .settings import S3Settings

# ----------------------- #

__all__ = [
    "S3DepsModule",
    "S3Client",
    "S3ClientPort",
    "S3Config",
    "S3Settings",
    "RoutedS3Client",
    "S3RoutingCredentials",
    "S3ClientDepKey",
    "s3_lifecycle_step",
    "S3StorageConfig",
    "S3ServerSideEncryption",
    "NamedResourceSpec",
    "coerce_named_resource_spec",
    "is_static_named_resource",
    "resolve_s3_bucket",
]

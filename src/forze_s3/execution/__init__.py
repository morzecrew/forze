"""S3 execution wiring for the application kernel.

Provides :class:`S3DepsModule` (dependency module registering client and
storage port), :data:`S3ClientDepKey`, and :func:`s3_lifecycle_step` for
startup/shutdown of the S3 client.
"""

from .deps import S3ClientDepKey, S3DepsModule
from .lifecycle import s3_lifecycle_step

# ----------------------- #

__all__ = ["S3DepsModule", "S3ClientDepKey", "s3_lifecycle_step"]

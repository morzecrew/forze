from forze.application.kernel.dependencies import DependencyKey

from ..kernel.platform import S3Client

# ----------------------- #

S3ClientDependencyKey: DependencyKey[S3Client] = DependencyKey("s3_client")
"""Key used to register the :class:`S3Client` implementation."""

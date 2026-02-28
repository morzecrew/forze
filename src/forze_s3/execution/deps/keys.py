from forze.application.contracts.deps import DepKey

from ...kernel.platform import S3Client

# ----------------------- #

S3ClientDepKey: DepKey[S3Client] = DepKey("s3_client")
"""Key used to register the :class:`S3Client` implementation."""

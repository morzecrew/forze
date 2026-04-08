"""S3 dependency keys, module, and configurations."""

from .configs import S3StorageConfig
from .keys import S3ClientDepKey
from .module import S3DepsModule

# ----------------------- #

__all__ = ["S3DepsModule", "S3ClientDepKey", "S3StorageConfig"]

"""S3 dependency keys, module, and factory functions.

Provides :data:`S3ClientDepKey`, :class:`S3DepsModule`, and the
:func:`s3_storage` factory for storage port adapters.
"""

from .keys import S3ClientDepKey
from .module import S3DepsModule

# ----------------------- #

__all__ = ["S3DepsModule", "S3ClientDepKey"]

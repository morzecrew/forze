"""Storage contracts for object storage (e.g. S3-compatible).

Provides :class:`StoragePort`, TypedDicts for stored/downloaded objects, and
dependency keys/routers for building storage ports by bucket.
"""

from .deps import StorageDepKey, StorageDepPort, StorageDepRouter
from .ports import DownloadedObject, ObjectMetadata, StoragePort, StoredObject

# ----------------------- #

__all__ = [
    "StoragePort",
    "StoredObject",
    "DownloadedObject",
    "ObjectMetadata",
    "StorageDepKey",
    "StorageDepPort",
    "StorageDepRouter",
]

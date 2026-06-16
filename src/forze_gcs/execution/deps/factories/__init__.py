"""GCS dependency factories."""

from .storage import (
    ConfigurableGCSStorageCommand,
    ConfigurableGCSStorageQuery,
    ConfigurableGCSStorageUploads,
)

# ----------------------- #

__all__ = [
    "ConfigurableGCSStorageQuery",
    "ConfigurableGCSStorageCommand",
    "ConfigurableGCSStorageUploads",
]
